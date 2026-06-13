"""
data-collector — AutoTrader
시세 수집 메인 프로세스
KIS (주식) + 업비트 (코인) → PostgreSQL + Redis
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime

from common.config import config
from common.database import db, cache
from collectors.kis_collector import KISCollector
from collectors.upbit_collector import UpbitCollector

# ── 로깅 설정 ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("data-collector")


class DataCollector:
    def __init__(self):
        self.running = False
        self.kis = KISCollector()
        self.upbit = UpbitCollector()

    async def start(self):
        logger.info("=" * 50)
        logger.info("🚀 AutoTrader data-collector 시작")
        logger.info(f"   수집 주기: {config.COLLECT_INTERVAL_SEC}초")
        logger.info(f"   주식 종목: {len(config.STOCK_SYMBOLS)}개")
        logger.info(f"   코인 페어: {len(config.CRYPTO_PAIRS)}개")
        logger.info("=" * 50)

        # DB / Redis 연결
        await db.connect()
        await cache.connect()

        # 수집기 시작
        await self.kis.start()
        await self.upbit.start()

        # 봇 상태 Redis에 등록
        await cache.set_bot_status("data_collector", {
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "stock_symbols": len(config.STOCK_SYMBOLS),
            "crypto_pairs": len(config.CRYPTO_PAIRS),
        })

        self.running = True
        await self._loop()

    async def _loop(self):
        """메인 수집 루프"""
        while self.running:
            start_time = asyncio.get_event_loop().time()
            logger.info(f"🔄 수집 사이클 시작 [{datetime.now().strftime('%H:%M:%S')}]")

            try:
                # 주식 + 코인 동시 수집
                await asyncio.gather(
                    self.kis.collect_all(),
                    self.upbit.collect_all(),
                    return_exceptions=True,
                )

                # 상태 업데이트
                await cache.set_bot_status("data_collector", {
                    "status": "running",
                    "last_collect": datetime.now().isoformat(),
                    "stock_symbols": len(config.STOCK_SYMBOLS),
                    "crypto_pairs": len(config.CRYPTO_PAIRS),
                })

            except Exception as e:
                logger.error(f"❌ 수집 오류: {e}")
                await cache.set_bot_status("data_collector", {
                    "status": "error",
                    "error": str(e),
                    "ts": datetime.now().isoformat(),
                })

            # 다음 수집까지 대기
            elapsed = asyncio.get_event_loop().time() - start_time
            wait = max(0, config.COLLECT_INTERVAL_SEC - elapsed)
            logger.info(f"⏳ 다음 수집까지 {wait:.1f}초 대기")
            await asyncio.sleep(wait)

    async def stop(self):
        logger.info("🛑 data-collector 종료 중...")
        self.running = False
        await self.kis.stop()
        await self.upbit.stop()
        await db.disconnect()
        await cache.disconnect()
        logger.info("✅ 종료 완료")


# ── 진입점 ──────────────────────────────────────────────
async def main():
    collector = DataCollector()

    # 시그널 처리 (Railway 컨테이너 종료 시)
    loop = asyncio.get_event_loop()

    def shutdown():
        logger.info("SIGTERM 수신 — 종료 시작")
        loop.create_task(collector.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown)

    await collector.start()


if __name__ == "__main__":
    asyncio.run(main())
