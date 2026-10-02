"""
ML 학습 결과 보고서 / 하루 한 번 학습 / 보고서 전송 위치 검증.

배경: _run_ml_training 은 "✅ 005930: 57.1%" 를 15개까지 잘라 매매 알림방에 보냈고(종목명·의미 설명·전체 수·실패 수 없음),
ml_trained_date 가 메모리라 재시작하면 같은 학습·메시지를 다시 보냈다. 여기서는
1. build_ml_report_text 의 내용(종목명, 성공/실패/사유별 개수, 읽는 법, 정렬, 경고 표시, 요약, 전 종목 나열),
2. common.telegram.split_text / send_report 의 분할·전송 위치·실패 격리,
3. Redis 키(ml:trained:{KST날짜})로 재시작 후에도 재학습하지 않는지
를 고정한다.
"""
import asyncio
import logging
import sys
import types
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

from tests.test_stock_trader_stop_loss import StockTrader  # noqa: E402
from ml_report import build_ml_report_text  # noqa: E402
from common import telegram  # noqa: E402

main_mod = sys.modules[StockTrader.__module__]
KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 9, 28, 15, 41, tzinfo=KST)


def rec(symbol, acc, samples=300, name=None):
    return {"symbol": symbol, "name": name, "accuracy": acc, "samples": samples}


def meta(total, insufficient=0, other=0):
    return {"total": total, "insufficient": insufficient, "other": other}


class TestBuildMlReportText(unittest.TestCase):
    def test_header_counts_and_title(self):
        text = build_ml_report_text([rec("005930", 60.0), rec("000660", 52.0)], meta(5, 2, 1), NOW)
        lines = text.split("\n")
        self.assertEqual(lines[0], "🎓 ML 학습 결과 (09/28 15:41)")
        self.assertEqual(lines[1], "감시 5종목 중 학습 성공 2, 실패 3 (데이터 부족 2, 기타 1)")

    def test_reading_guide_block(self):
        text = build_ml_report_text([rec("005930", 60.0)], meta(1), NOW)
        self.assertIn("📖 읽는 법", text)
        self.assertIn("5거래일 뒤 0.5% 넘게 오를지를 맞힌 비율", text)
        self.assertIn("동전 던지기", text)
        self.assertIn("55% 미만은 참고하지 말 것", text)
        self.assertIn("수익률이 아니라 예측 정확도", text)
        self.assertIn("±15%p", text)
        self.assertIn("📊 결과 (정확도 높은 순)", text)

    def test_stock_name_shown_and_falls_back_to_code(self):
        text = build_ml_report_text([rec("005930", 60.0, name="삼성전자"), rec("000660", 58.0, name=None)], meta(2), NOW)
        self.assertIn("· 삼성전자 60.0%", text)
        self.assertNotIn("005930", text)
        self.assertIn("· 000660 58.0%", text)

    def test_name_is_html_escaped(self):
        text = build_ml_report_text([rec("1", 60.0, name="A&B <x>")], meta(1), NOW)
        self.assertIn("· A&amp;B &lt;x&gt; 60.0%", text)

    def test_sorted_by_accuracy_desc(self):
        text = build_ml_report_text(
            [rec("A", 51.0, name="가"), rec("B", 63.5, name="나"), rec("C", 55.0, name="다")], meta(3), NOW)
        self.assertLess(text.index("· 나 "), text.index("· 다 "))
        self.assertLess(text.index("· 다 "), text.index("· 가 "))

    def test_below_50_and_low_sample_markers(self):
        text = build_ml_report_text(
            [rec("A", 49.9, samples=300, name="가"), rec("B", 50.0, samples=149, name="나"),
             rec("C", 60.0, samples=150, name="다")], meta(3), NOW)
        line = {l.split()[1]: l for l in text.split("\n") if l.startswith("· ") and "%" in l}
        self.assertIn("(동전보다 낮음)", line["가"])
        self.assertNotIn("표본 적음", line["가"])
        self.assertNotIn("(동전보다 낮음)", line["나"])  # 50% 는 미만이 아님
        self.assertIn("⚠️표본 적음", line["나"])
        self.assertNotIn("(동전보다 낮음)", line["다"])
        self.assertNotIn("표본 적음", line["다"])  # 150 은 적음이 아님

    def test_summary_line(self):
        text = build_ml_report_text([rec("A", 60.0), rec("B", 55.0), rec("C", 45.0), rec("D", 52.0)], meta(4), NOW)
        self.assertEqual(text.split("\n")[-1], "요약: 평균 53.0% · 55% 이상 2종목 · 50% 미만 1종목")

    def test_all_results_listed_beyond_15(self):
        rs = [rec(f"{i:06d}", 50 + i * 0.1, name=f"종목{i}") for i in range(40)]
        text = build_ml_report_text(rs, meta(40), NOW)
        self.assertEqual(sum(1 for l in text.split("\n") if l.startswith("· 종목")), 40)

    def test_no_success(self):
        text = build_ml_report_text([], meta(3, 3, 0), NOW)
        self.assertIn("감시 3종목 중 학습 성공 0, 실패 3 (데이터 부족 3, 기타 0)", text)
        self.assertIn("요약: 학습 성공 종목 없음", text)


class TestSplitText(unittest.TestCase):
    def test_short_text_single_chunk(self):
        self.assertEqual(telegram.split_text("abc"), ["abc"])

    def test_split_over_4000_on_line_boundaries(self):
        lines = [f"· 종목{i} 55.0%" + "x" * 50 for i in range(200)]
        text = "\n".join(lines)
        self.assertGreater(len(text), 4000)
        chunks = telegram.split_text(text)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(c) <= 4000 for c in chunks))
        self.assertEqual("\n".join(chunks), text)  # 손실·중복 없음

    def test_overlong_single_line_hard_split(self):
        chunks = telegram.split_text("y" * 9000)
        self.assertTrue(all(len(c) <= 4000 for c in chunks))
        self.assertEqual("".join(chunks), "y" * 9000)


class TestSendReport(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        p = mock.patch.object(telegram.config, "STARK_BOT_TOKEN", "STARK-SECRET-TOKEN")
        p2 = mock.patch.object(telegram.config, "TELEGRAM_TOKEN", "TG-SECRET-TOKEN")
        p.start(); p2.start()
        self.addCleanup(p.stop); self.addCleanup(p2.stop)

    async def test_channel_id_set_sends_to_channel_with_stark_token(self):
        with mock.patch.dict("os.environ", {"TELEGRAM_CHANNEL_ID": "-100123"}), \
                mock.patch.object(telegram, "_post_channel", mock.AsyncMock(return_value=True)) as post, \
                mock.patch.object(telegram, "send_stock", mock.AsyncMock()) as stock:
            await telegram.send_report("hello")
        post.assert_awaited_once_with("STARK-SECRET-TOKEN", "-100123", "hello")
        stock.assert_not_awaited()

    async def test_token_falls_back_to_telegram_token(self):
        with mock.patch.dict("os.environ", {"TELEGRAM_CHANNEL_ID": "-100123"}), \
                mock.patch.object(telegram.config, "STARK_BOT_TOKEN", ""), \
                mock.patch.object(telegram, "_post_channel", mock.AsyncMock(return_value=True)) as post:
            await telegram.send_report("hello")
        post.assert_awaited_once_with("TG-SECRET-TOKEN", "-100123", "hello")

    async def test_channel_id_empty_falls_back_to_send_stock(self):
        for value in ("", "   "):
            with mock.patch.dict("os.environ", {"TELEGRAM_CHANNEL_ID": value}), \
                    mock.patch.object(telegram, "_post_channel", mock.AsyncMock()) as post, \
                    mock.patch.object(telegram, "send_stock", mock.AsyncMock()) as stock:
                await telegram.send_report("hello")
            stock.assert_awaited_once_with("hello")
            post.assert_not_awaited()

    async def test_channel_id_unset_falls_back_to_send_stock(self):
        env = {k: v for k, v in __import__("os").environ.items() if k != "TELEGRAM_CHANNEL_ID"}
        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(telegram, "send_stock", mock.AsyncMock()) as stock:
            await telegram.send_report("hello")
        stock.assert_awaited_once_with("hello")

    async def test_long_text_split_into_multiple_messages(self):
        text = "\n".join(f"line{i}" + "z" * 60 for i in range(200))
        with mock.patch.dict("os.environ", {"TELEGRAM_CHANNEL_ID": "-100123"}), \
                mock.patch.object(telegram, "_post_channel", mock.AsyncMock(return_value=True)) as post:
            await telegram.send_report(text)
        self.assertGreater(post.await_count, 1)
        self.assertTrue(all(len(c.args[2]) <= 4000 for c in post.await_args_list))

    async def test_channel_failure_does_not_raise_and_logs_no_secrets(self):
        class Boom:
            def __init__(self, *a, **k):
                pass

            async def __aenter__(self):
                raise RuntimeError("connect failed to bot STARK-SECRET-TOKEN chat -100123")

            async def __aexit__(self, *a):
                return False

        aiohttp_stub = mock.Mock(ClientSession=Boom, ClientTimeout=lambda **k: None)
        with mock.patch.dict("os.environ", {"TELEGRAM_CHANNEL_ID": "-100123"}), \
                mock.patch.object(telegram, "aiohttp", aiohttp_stub), \
                self.assertLogs(telegram.logger, level=logging.WARNING) as logs:
            await telegram.send_report("hello")  # 예외 없이 끝나야 한다
        out = "\n".join(logs.output)
        self.assertIn("텔레그램 채널 전송 실패", out)
        for secret in ("STARK-SECRET-TOKEN", "-100123"):
            self.assertNotIn(secret, out)

    async def test_unexpected_error_in_send_stock_is_swallowed(self):
        with mock.patch.dict("os.environ", {"TELEGRAM_CHANNEL_ID": ""}), \
                mock.patch.object(telegram, "send_stock", mock.AsyncMock(side_effect=RuntimeError("x"))):
            await telegram.send_report("hello")


class FakeRedis:
    """재시작해도 유지되는 외부 저장소 역할 (StockTrader 인스턴스와 별개 수명)"""
    def __init__(self):
        self.store = {}
        self.ttls = {}

    async def exists(self, key):
        return 1 if key in self.store else 0

    async def set(self, key, value, ex=None):
        self.store[key] = value
        self.ttls[key] = ex


class BrokenRedis:
    async def exists(self, key):
        raise ConnectionError("redis down")

    async def set(self, key, value, ex=None):
        raise ConnectionError("redis down")


def _trader():
    return StockTrader.__new__(StockTrader)


def _fake_ml(train):
    class FakeManager:
        def __init__(self, db_pool=None):
            pass

        async def train(self, symbol, ohlcv):
            return await train(symbol, ohlcv)

    mod = types.ModuleType("ml.model")
    mod.MLModelManager = FakeManager
    return mock.patch.dict(sys.modules, {"ml.model": mod})


class TestRunMlTraining(unittest.IsolatedAsyncioTestCase):
    """_run_ml_training 이 보고서 재료를 올바르게 모으고 기존 메모리 저장 형식을 유지하는지"""

    async def _run(self, symbols, ohlcv_len, train):
        fake_db = mock.Mock(pool=None)
        fake_db.get_watchlist_symbols = mock.AsyncMock(return_value=symbols)
        fake_db.get_recent_ohlcv = mock.AsyncMock(side_effect=lambda s, **k: [{}] * ohlcv_len[s])
        names = {"005930": "삼성전자", "000660": "SK하이닉스"}
        sent, saved = [], []
        trader = _trader()
        trader._save_ml_memory = mock.AsyncMock(side_effect=lambda r, s: saved.append((list(r), list(s))))
        with _fake_ml(train), \
                mock.patch.object(main_mod, "db", fake_db), \
                mock.patch.object(main_mod, "_resolve_stock_name", mock.AsyncMock(side_effect=lambda c: names.get(c, c))), \
                mock.patch("common.telegram.send_stock", mock.AsyncMock(side_effect=sent.append)):
            # [AT] feat/channel-slim: CHANNEL_SLIM=True(기본)면 ML 학습 결과는 채널이 아닌
            # 개인방(send_stock)으로 간다 — 라우팅 자체는 아래 TestRunMlTrainingChannelRouting
            # 에서 따로 검증하고, 여기서는 보고서 내용만 본다.
            ok = await trader._run_ml_training()
        return ok, sent, saved

    async def test_report_counts_names_and_legacy_memory_format(self):
        async def train(symbol, ohlcv):
            if symbol == "005930":
                return {"success": True, "accuracy": 58.3, "samples": 400}
            if symbol == "000660":
                return {"success": True, "accuracy": 47.0, "samples": 100}
            if symbol == "111111":
                return {"success": False, "error": "학습 데이터 부족 (30개, 최소 50개 필요)"}
            if symbol == "222222":
                return {"success": False, "error": "이상한 오류"}
            raise RuntimeError("boom")

        symbols = ["005930", "000660", "111111", "222222", "333333", "444444"]
        ohlcv_len = {s: 100 for s in symbols}
        ohlcv_len["444444"] = 10  # OHLCV 부족 스킵
        ok, sent, saved = await self._run(symbols, ohlcv_len, train)

        self.assertTrue(ok)
        self.assertEqual(len(sent), 1)
        text = sent[0]
        self.assertIn("감시 6종목 중 학습 성공 2, 실패 4 (데이터 부족 2, 기타 2)", text)
        self.assertIn("· 삼성전자 58.3%", text)
        self.assertIn("· SK하이닉스 47.0% (동전보다 낮음) ⚠️표본 적음", text)
        # _save_ml_memory 에 넘기는 형식은 기존 그대로
        self.assertEqual(saved[0][0], [
            "✅ 005930: 58.3%", "✅ 000660: 47.0%",
            "⚠️ 111111: 학습 데이터 부족 (30개, 최소 50개 필요)", "⚠️ 222222: 이상한 오류"])
        self.assertEqual(saved[0][1], symbols)

    async def test_more_than_15_all_listed(self):
        symbols = [f"{i:06d}" for i in range(30)]

        async def train(symbol, ohlcv):
            return {"success": True, "accuracy": 55.0 + int(symbol) * 0.1, "samples": 300}

        _, sent, _ = await self._run(symbols, {s: 100 for s in symbols}, train)
        body = sent[0].split("📊 결과 (정확도 높은 순)")[1]
        self.assertEqual(sum(1 for l in body.split("\n") if l.startswith("· ")), 30)


class TestRunMlTrainingChannelRouting(unittest.IsolatedAsyncioTestCase):
    """[AT] feat/channel-slim: ML 학습 결과(신뢰도 낮은 문서)는 CHANNEL_SLIM=True(기본)면
    채널이 아닌 개인방으로, False(되돌림)면 기존처럼 채널로 간다."""

    async def _run(self, channel_slim: bool):
        async def train(symbol, ohlcv):
            return {"success": True, "accuracy": 55.0, "samples": 300}

        fake_db = mock.Mock(pool=None)
        fake_db.get_watchlist_symbols = mock.AsyncMock(return_value=["005930"])
        fake_db.get_recent_ohlcv = mock.AsyncMock(return_value=[{}] * 100)
        sent_personal, sent_channel = [], []
        trader = _trader()
        trader._save_ml_memory = mock.AsyncMock()
        with _fake_ml(train), \
                mock.patch.object(main_mod, "db", fake_db), \
                mock.patch.object(main_mod, "_resolve_stock_name", mock.AsyncMock(side_effect=lambda c: c)), \
                mock.patch("common.telegram.CHANNEL_SLIM", channel_slim), \
                mock.patch("common.telegram.send_stock", mock.AsyncMock(side_effect=sent_personal.append)), \
                mock.patch("common.telegram.send_report", mock.AsyncMock(side_effect=sent_channel.append)):
            ok = await trader._run_ml_training()
        return ok, sent_personal, sent_channel

    async def test_channel_slim_true_sends_personal_only(self):
        ok, sent_personal, sent_channel = await self._run(channel_slim=True)
        self.assertTrue(ok)
        self.assertEqual(len(sent_personal), 1)
        self.assertEqual(sent_channel, [])

    async def test_channel_slim_false_sends_channel_only(self):
        ok, sent_personal, sent_channel = await self._run(channel_slim=False)
        self.assertTrue(ok)
        self.assertEqual(len(sent_channel), 1)
        self.assertEqual(sent_personal, [])


class TestMlOncePerDay(unittest.IsolatedAsyncioTestCase):
    TODAY = date(2026, 9, 28)

    async def _tick(self, trader, fake_redis):
        """_loop 의 학습 트리거 구간을 그대로 흉내: 확인 → (학습 → Redis 기록) → 메모리 기록"""
        if trader.ml_trained_date != self.TODAY:
            if await trader._ml_already_trained(self.TODAY):
                pass
            elif await trader._run_ml_training():
                await trader._mark_ml_trained(self.TODAY)
            trader.ml_trained_date = self.TODAY

    def _new_trader(self, calls):
        t = _trader()
        t.ml_trained_date = None
        t._run_ml_training = mock.AsyncMock(side_effect=lambda: calls.append(1) or True)
        return t

    async def test_restart_does_not_retrain_with_shared_redis(self):
        redis = FakeRedis()
        calls = []
        with mock.patch.object(main_mod, "cache", mock.Mock(client=redis)):
            first = self._new_trader(calls)
            await self._tick(first, redis)
            self.assertEqual(len(calls), 1)
            self.assertIn("ml:trained:2026-09-28", redis.store)
            self.assertEqual(redis.ttls["ml:trained:2026-09-28"], 3 * 86400)

            restarted = self._new_trader(calls)  # 재시작: 메모리 값 초기화
            await self._tick(restarted, redis)
            self.assertEqual(len(calls), 1)  # 재학습·재전송 없음
            self.assertEqual(restarted.ml_trained_date, self.TODAY)

    async def test_next_day_trains_again(self):
        redis = FakeRedis()
        redis.store["ml:trained:2026-09-27"] = "1"
        with mock.patch.object(main_mod, "cache", mock.Mock(client=redis)):
            t = self._new_trader([])
            self.assertFalse(await t._ml_already_trained(self.TODAY))

    async def test_memory_value_alone_skips(self):
        t = _trader()
        t.ml_trained_date = self.TODAY
        with mock.patch.object(main_mod, "cache", mock.Mock(client=BrokenRedis())):
            self.assertTrue(await t._ml_already_trained(self.TODAY))

    async def test_redis_failure_falls_back_to_memory_with_warning(self):
        calls = []
        with mock.patch.object(main_mod, "cache", mock.Mock(client=BrokenRedis())):
            t = self._new_trader(calls)
            with self.assertLogs(main_mod.logger, level=logging.WARNING) as logs:
                await self._tick(t, None)  # 예외 없이 학습은 진행, 기록 실패는 경고
            self.assertEqual(len(calls), 1)
            self.assertEqual(sum("Redis" in o for o in logs.output), 2)  # 확인 실패 + 기록 실패
            await self._tick(t, None)  # 같은 프로세스에서는 메모리 값으로 재학습 안 함
            self.assertEqual(len(calls), 1)

    async def test_failed_training_is_not_marked_in_redis(self):
        redis = FakeRedis()
        with mock.patch.object(main_mod, "cache", mock.Mock(client=redis)):
            t = _trader()
            t.ml_trained_date = None
            t._run_ml_training = mock.AsyncMock(return_value=False)
            await self._tick(t, redis)
            self.assertEqual(redis.store, {})


if __name__ == "__main__":
    unittest.main()
