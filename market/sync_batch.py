"""
market/sync_batch.py — 전종목 동기화 배치.

dashboard/main.py의 _load_stock_cache에 있던 전종목 캐시 적재 로직을 독립
모듈로 이관했다. 우선순위는 원본과 동일하게 유지한다:
  1) stocks 테이블(정본) 즉시 로드 — 7일 이내 & 2000개 이상이면 네트워크
     호출 없이 그대로 사용
  2) pykrx로 KOSPI/KOSDAQ 갱신 (정확한 한글 종목명, 시장별 독립 실행)
  3) pykrx 결과가 부족하면 KIS 마스터 ZIP(kospi_code.mst, kosdaq_code.mst)
     고정폭 파싱으로 보완 — Rate Limit을 피하려고 일괄 zip 다운로드 방식을
     그대로 쓴다.
갱신된 결과는 stocks 테이블에 upsert하고 Universe 캐시를 교체한다.

dashboard/main.py는 startup 훅과 /api/stock/reload_cache,
/api/stock/purge_and_reload 에서 sync_stock_universe()를 호출하도록
위임한다. `python -m market.sync_batch` 로 독립 배치 스크립트로도 실행
가능하다.
"""
import asyncio
import io
import logging
import ssl
import zipfile
from typing import Any, Dict, Optional, Tuple

from market.universe import Universe

logger = logging.getLogger(__name__)

_KIS_MASTER_URLS = [
    ("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip", "cp949"),
    ("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip", "cp949"),
]

# 한국 상장종목은 최소 이 이상이어야 정상. 그보다 적으면 부분실패 데이터로 보고
# '최신이니 그냥 씀' 판정을 내리지 않고 반드시 재적재를 시도한다.
_MIN_EXPECTED_COUNT = 2000


async def _fetch_from_pykrx() -> Dict[str, str]:
    """pykrx로 KOSPI/KOSDAQ 종목명 조회. 시장별 독립 실행 — 한쪽 실패해도 다른 쪽은 살림."""
    name_map: Dict[str, str] = {}
    try:
        from pykrx import stock as pykrx_stock
    except Exception as e:
        logger.warning(f"pykrx 종목 로드 실패: {e}")
        return name_map

    loop = asyncio.get_event_loop()

    def _fetch_market(market: str) -> Dict[str, str]:
        result = {}
        tickers = pykrx_stock.get_market_ticker_list(market=market)
        for ticker in tickers:
            try:
                name = pykrx_stock.get_market_ticker_name(ticker)
                if name:
                    result[name] = ticker
            except Exception:
                continue
        return result

    for market in ["KOSPI", "KOSDAQ"]:
        try:
            part = await loop.run_in_executor(None, _fetch_market, market)
            name_map.update(part)
            logger.info(f"pykrx {market} 로드: {len(part)}개")
        except Exception as me:
            logger.warning(f"pykrx {market} 로드 실패: {me}")
    return name_map


def _parse_kis_master_bytes(raw_bytes: bytes, enc: str, name_map: Dict[str, str]) -> int:
    """KIS 마스터파일(.mst) 원본 바이트를 라인 단위 고정폭으로 파싱해 name_map에 채운다.
    주의: 종목명은 정확히 40바이트(cp949) 고정폭 필드. 단순 split()으로 자르면
    ISIN/숫자 필드가 이름에 섞여 깨짐 — 반드시 바이트 오프셋으로 슬라이스한다
    (텍스트로 디코딩 후 자르면 멀티바이트 경계가 깨짐).
    반환값: 이번 호출에서 새로 추가된 종목 수."""
    added = 0
    for line_bytes in raw_bytes.split(b"\n"):
        if len(line_bytes) < 30:
            continue
        try:
            # 표준코드(ISIN) 12바이트 + 단축코드 6바이트 뒤에 종목명(cp949, 최대 40바이트) 위치
            code_part = line_bytes[9:21].decode("ascii", errors="ignore").strip()
            code = "".join(ch for ch in code_part if ch.isdigit())[-6:]
            name_bytes = line_bytes[21:61]
            name = name_bytes.decode(enc, errors="ignore").strip()
            if code and name and code.isdigit() and len(code) == 6 and \
               not any(c.isdigit() for c in name[:1]) and code not in name_map.values():
                name_map[name] = code
                added += 1
        except Exception:
            continue
    return added


async def _fetch_from_kis_master() -> Dict[str, str]:
    """KIS 마스터파일(zip) 폴백 — pykrx 실패/부족 시에만 사용. 다운로드·압축해제 후
    실제 파싱은 _parse_kis_master_bytes()에 위임한다."""
    name_map: Dict[str, str] = {}
    try:
        import aiohttp
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ctx)) as sess:
            for url, enc in _KIS_MASTER_URLS:
                try:
                    async with sess.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        raw = await resp.read()
                    zf = zipfile.ZipFile(io.BytesIO(raw))
                    fn = zf.namelist()[0]
                    raw_bytes = zf.read(fn)
                    added = _parse_kis_master_bytes(raw_bytes, enc, name_map)
                    logger.info(f"KIS 마스터 보완({url.split('/')[-1]}): +{added}개")
                except Exception as ie:
                    logger.warning(f"KIS 마스터 다운로드 실패({url}): {ie}")
    except Exception as e:
        logger.warning(f"KIS 마스터 처리 실패: {e}")
    return name_map


async def _load_from_db(pool: Any) -> Tuple[Optional[Dict[str, str]], bool]:
    """stocks 테이블에서 즉시 로드.
    반환값: (name→code 맵 또는 None, 네트워크 갱신 없이 써도 될 만큼 신선한지 여부)."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT symbol, name FROM stocks")
            age = await conn.fetchval("SELECT NOW() - MAX(updated_at) FROM stocks")
    except Exception as e:
        logger.warning(f"종목 캐시 DB 로드 실패: {e}")
        return None, False
    if not rows:
        return None, False
    name_map = {r["name"]: r["symbol"] for r in rows if r["name"]}
    logger.info(f"✅ 종목 캐시 DB 로드: {len(rows)}개")
    is_fresh = age is not None and age.days < 7 and len(rows) >= _MIN_EXPECTED_COUNT
    return name_map, is_fresh


async def _persist_to_db(pool: Any, name_map: Dict[str, str]) -> None:
    try:
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO stocks (symbol, name, updated_at) VALUES ($1, $2, NOW()) "
                "ON CONFLICT (symbol) DO UPDATE SET name=EXCLUDED.name, updated_at=NOW()",
                [(v, k) for k, v in name_map.items()])
    except Exception as e:
        logger.warning(f"종목 캐시 DB 저장 실패: {e}")


async def sync_stock_universe(pool: Any, universe: Universe, *, force: bool = False) -> int:
    """전종목 캐시를 최신화하고 universe에 반영한다. 반환값: 적재된 종목 수.

    force=False(기본): stocks 테이블이 7일 이내·2000개 이상이면 네트워크 호출 없이 그대로 사용.
    force=True: DB 캐시 신선도와 무관하게 pykrx/KIS 재조회를 강제한다.
    """
    name_map_db, is_fresh = await _load_from_db(pool)
    if name_map_db:
        universe.replace_cache(name_map_db)
    if not force and is_fresh:
        return len(name_map_db)

    name_map = await _fetch_from_pykrx()
    if len(name_map) < _MIN_EXPECTED_COUNT:
        logger.warning(f"pykrx 결과가 부족함({len(name_map)}개) — KIS 마스터파일로 보완 시도")
        name_map.update(await _fetch_from_kis_master())

    if not name_map:
        logger.warning("종목 캐시 갱신 실패 (무시) — 기존 캐시 유지")
        return len(universe.code_cache)

    universe.replace_cache(name_map)
    logger.info(f"✅ 전체 종목 캐시 갱신: {len(name_map)}개")
    await _persist_to_db(pool, name_map)
    return len(name_map)


async def _standalone_main() -> None:
    """python -m market.sync_batch 로 실행하는 독립 배치 스크립트 진입점."""
    logging.basicConfig(level=logging.INFO)
    from common.database import db

    await db.connect()
    try:
        universe = Universe(db.pool)
        count = await sync_stock_universe(db.pool, universe, force=True)
        logger.info(f"🎉 배치 완료 — {count}개 종목 동기화")
    finally:
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(_standalone_main())
