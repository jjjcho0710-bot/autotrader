"""
tests/test_principle_stats_rid.py - [AT] fix/principle-stats-rid

배경: AI가 "근거원칙: R12" 형식(learning_rules 기반 신규 원칙)으로 인용해도
dashboard.main._extract_principles가 "K\\d+"만 매칭해 trade_journal.principles에
전혀 저장되지 않았고, principle_stats 성과 집계에서도 통째로 빠졌다
(learning_rules 경로와 성과 추적 경로 단절).

수정 내용 검증:
1) _extract_principles가 K·R 토큰을 접두사 보존하며 모두 추출하는지(순수 함수).
2) _score_journal의 원칙별 성과 누적이 K는 양수, R은 음수로 principle_stats.
   principle_id에 저장해 서로 충돌하지 않는지(K12 vs R12가 별개 행으로 기록됨).
3) learning/curator.jarvis_knowledge_curate가 읽는 "principle_stats ps JOIN
   jarvis_notes n ON n.id = ps.principle_id" 조회는 jarvis_notes.id가 SERIAL(항상
   1 이상 양수)이라 음수로 저장된 R 항목을 구조적으로 절대 매칭하지 않는다 — 즉
   코드를 건드리지 않아도 안정화 기간 중 R 항목이 지식 정리(curate)의 원칙 성과
   통계·분류 결과에 전혀 섞이지 않는다는 것을 실제 JOIN 필터링을 흉내낸 가짜
   커넥션으로 검증한다. 기존 K 기반 동작(예: 적중률 표시)은 그대로 유지됨도 함께
   확인한다(회귀 방지).
"""
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402
from learning import curator  # noqa: E402
from tests.test_scoring_improvement import (  # noqa: E402
    _FakeKisSession, _FakeRedisNoop, _FakeScoreConfig, _JournalConn, _JournalPool,
)
from tests.test_weekend_alert_cleanup import FakePool  # noqa: E402


def _row(id_, symbol, name, price, principles):
    """trade_journal 행 더블 — _score_journal이 쓰는 컬럼만 담는다([AT]
    fix/principle-stats-rid)."""
    return {
        "id": id_, "symbol": symbol, "name": name, "action": "buy",
        "jarvis_decision": "EXECUTE", "price": price, "principles": principles,
    }


# ── 1) _extract_principles: K·R 접두사 보존 추출 (순수 함수) ──────────────────


class TestExtractPrinciplesKAndR(unittest.TestCase):
    def test_extracts_k_only_like_before(self):
        text = "강한 신호. 근거원칙: K12,K7"
        self.assertEqual(dm._extract_principles(text), "K12,K7")

    def test_extracts_r_only_was_previously_dropped_entirely(self):
        """수정 전엔 R 인용이 아예 매칭 안 돼 빈 문자열로 사라졌다 — 이 테스트는
        수정을 되돌리면 실패한다."""
        text = "강한 신호. 근거원칙: R12,R7"
        self.assertEqual(dm._extract_principles(text), "R12,R7")

    def test_extracts_mixed_k_and_r_preserving_prefix(self):
        """K와 R이 섞여 인용돼도 접두사가 지워지지 않아야 서로 다른 테이블의 id로
        구분할 수 있다."""
        text = "근거원칙: K3,R3,K9"
        result = dm._extract_principles(text)
        self.assertEqual(set(result.split(",")), {"K3", "R3", "K9"})
        self.assertIn("K3", result)
        self.assertIn("R3", result)

    def test_no_citation_returns_empty(self):
        self.assertEqual(dm._extract_principles("근거 없이 그냥 EXECUTE"), "")


# ── 2) _score_journal: K/R 분리 저장(충돌 금지) ───────────────────────────────


class TestScoreJournalPrincipleStatsKRSeparation(unittest.IsolatedAsyncioTestCase):
    async def _run_score_journal(self, rows, price_map):
        with patch.object(dm, "db_pool", _JournalPool(rows), create=True), \
             patch.object(dm, "redis_client", _FakeRedisNoop(), create=True), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="TOKEN")), \
             patch.object(dm, "config", _FakeScoreConfig()), \
             patch.object(dm, "_send_telegram", new=AsyncMock()), \
             patch("aiohttp.ClientSession", lambda **kw: _FakeKisSession(price_map)), \
             patch("asyncio.sleep", new=AsyncMock()):
            pool = dm.db_pool
            await dm._score_journal()
        return pool._conn.executed

    async def test_r_principle_stored_as_negative_principle_id(self):
        """'근거원칙: R12'로 기록된 판단이 적중하면 principle_stats에 principle_id=-12로
        기록돼야 한다(양수 공간은 jarvis_notes.id용으로 남겨둠)."""
        rows = [_row(1, "005930", "삼성전자", 70000, "R12")]
        # 종가 75000 → +7.1% → 적중(good=True)
        executed = await self._run_score_journal(rows, {"005930": 75000})
        inserts = [args for q, args in executed if "INSERT INTO principle_stats" in q]
        self.assertEqual(len(inserts), 1)
        # VALUES($1, 1, $2, NOW())의 applied=1은 SQL 리터럴이라 바인딩 args에는
        # (principle_id, hits) 두 값만 들어온다.
        principle_id, hits = inserts[0]
        self.assertEqual(principle_id, -12)
        self.assertEqual(hits, 1)

    async def test_k_and_r_with_same_number_do_not_collide(self):
        """K12와 R12가 같은 판단에 함께 인용돼도 principle_stats에는 principle_id=12와
        -12로 서로 다른 행에 기록된다(충돌 금지 요구사항)."""
        rows = [_row(1, "005930", "삼성전자", 70000, "K12,R12")]
        executed = await self._run_score_journal(rows, {"005930": 75000})
        inserts = [args[0] for q, args in executed if "INSERT INTO principle_stats" in q]
        self.assertEqual(sorted(inserts), [-12, 12])

    async def test_malformed_token_is_skipped_without_crashing(self):
        """레거시 숫자만(접두사 없음) 저장돼 있던 오래된 행도 조용히 건너뛰고(과거
        데이터는 K/R 구분 불가라 집계하지 않음) 채점 자체는 계속 진행돼야 한다."""
        rows = [_row(1, "005930", "삼성전자", 70000, "12,K7")]
        executed = await self._run_score_journal(rows, {"005930": 75000})
        inserts = [args[0] for q, args in executed if "INSERT INTO principle_stats" in q]
        self.assertEqual(inserts, [7])


# ── 3) curate 격리: principle_stats JOIN이 음수(R) 행을 구조적으로 제외 ──────


class _RealJoinFakeCuratorConnection:
    """FakeCuratorConnection(test_weekend_alert_cleanup.py)은 principle_stats 쿼리
    결과를 그대로 돌려줘 실제 'JOIN jarvis_notes n ON n.id = ps.principle_id'의
    필터링 동작을 검증할 수 없었다. 여기서는 jarvis_notes에 실존하는 양의 id
    집합(jarvis_notes_ids)만 통과시켜, principle_stats에 음수(R 유래) 행이 있어도
    실제 Postgres INNER JOIN에서는 결코 매칭되지 않음을 흉내낸다([AT]
    fix/principle-stats-rid)."""

    def __init__(self, stats_rows, jarvis_notes_ids):
        self._stats_rows = stats_rows
        self._notes_ids = set(jarvis_notes_ids)

    async def fetch(self, query, *args):
        if "FROM learning_rules" in query:
            return []
        if "category='knowledge' AND is_active=TRUE" in query and "LIMIT 80" in query:
            return [{"id": i, "content": f"원칙 {i}"} for i in range(3)]
        if "FROM principle_stats ps JOIN jarvis_notes n" in query:
            return [r for r in self._stats_rows if r["principle_id"] in self._notes_ids]
        raise AssertionError(f"unexpected fetch: {query}")

    async def execute(self, query, *args):
        return None


class TestCuratePrincipleStatsIsolatesNegativeRIds(unittest.IsolatedAsyncioTestCase):
    async def test_negative_r_encoded_rows_never_surface_in_curate(self):
        """principle_stats에 K12(양수, jarvis_notes에 실존) 행과 R7의 음수 인코딩(-7,
        jarvis_notes에는 당연히 존재하지 않음) 행이 섞여 있어도, 실제 JOIN 흉내
        커넥션을 거치면 음수 행은 curate 결과(통계 텍스트)에 전혀 나타나지 않는다 —
        코드 수정 없이 구조적으로 격리된다."""
        stats_rows = [
            {"principle_id": 12, "applied": 10, "hits": 8, "content": "K원칙"},
            {"principle_id": -7, "applied": 99, "hits": 99, "content": "R원칙(유출되면 안 됨)"},
        ]
        conn = _RealJoinFakeCuratorConnection(stats_rows, jarvis_notes_ids={12})
        pool = FakePool(conn)
        send_telegram_fn = AsyncMock()
        ask_llm_fn = AsyncMock(return_value="[진입] 테스트 원칙 1\n[청산] 테스트 원칙 2")

        await curator.jarvis_knowledge_curate(
            pool=pool, ask_llm_fn=ask_llm_fn, send_telegram_fn=send_telegram_fn)

        msg = send_telegram_fn.call_args.args[0]
        self.assertIn("K12", msg)
        self.assertNotIn("-7", msg, "음수(R 유래) 원칙 성과가 curate 결과에 노출되면 안 됨")
        self.assertNotIn("유출되면 안 됨", msg)

    async def test_existing_positive_k_flow_unchanged(self):
        """R 토큰 지원 추가가 기존 K 기반 성과 표시(적중률 등)에 영향을 주지 않는다
        (회귀 방지)."""
        stats_rows = [{"principle_id": 50, "applied": 10, "hits": 5, "content": "원칙E"}]
        conn = _RealJoinFakeCuratorConnection(stats_rows, jarvis_notes_ids={50})
        pool = FakePool(conn)
        send_telegram_fn = AsyncMock()
        ask_llm_fn = AsyncMock(return_value="[진입] 테스트 원칙 1\n[청산] 테스트 원칙 2")

        await curator.jarvis_knowledge_curate(
            pool=pool, ask_llm_fn=ask_llm_fn, send_telegram_fn=send_telegram_fn)

        msg = send_telegram_fn.call_args.args[0]
        self.assertIn("K50", msg)
        self.assertIn("50%", msg)


if __name__ == "__main__":
    unittest.main()
