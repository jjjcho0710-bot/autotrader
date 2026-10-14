"""
tests/test_dockerfile_dashboard_imports.py - [AT] fix/dashboard-image-stock-trader 재발 방지

배경: dashboard/main.py가 `from stock_trader.kis_trader import KISTrader`를
import하지만 Dockerfile.dashboard가 stock_trader/를 COPY하지 않아 dashboard
서비스에서 ImportError가 발생했다. dashboard/*.py가 import하는 저장소 내
최상위 패키지(디렉터리)가 모두 Dockerfile.dashboard의 COPY 대상에 포함되는지
검사해 같은 누락이 재발하면 테스트가 실패하도록 한다.

이 테스트를 작성하며 별도의 기존 누락을 하나 더 발견했다: dashboard/main.py의
_morning_routine()이 `from data_collector.collectors.dart_collector import
DARTCollector`를 지연 import하지만 Dockerfile.dashboard는 data_collector/도
COPY하지 않는다(try/except로 감싸여 있어 크래시 대신 "공시 확인 실패" 경고만
남기고 조용히 실패해왔다). 이번 fix/dashboard-image-stock-trader 작업은
stock_trader 누락만 승인된 범위이므로 data_collector 수정은 포함하지 않았다.
별도 PM 승인 전까지는 고치지 않을 것이므로, 회귀를 추적하되 이 PR을 막지
않도록 KNOWN_UNFIXED_GAPS로 명시적으로 예외 처리한다.
"""
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DOCKERFILE_DASHBOARD = REPO_ROOT / "Dockerfile.dashboard"

# 확인됐지만 아직 PM 승인 전이라 고치지 않은 기존 COPY 누락.
# data_collector: dashboard/main.py의 _morning_routine() 공시 확인 단계가
# data_collector.collectors.dart_collector를 지연 import하지만 Dockerfile.dashboard가
# data_collector/를 COPY하지 않아 매번 ImportError → try/except로 조용히 실패한다.
# fix/dashboard-image-stock-trader PR 보고에서 별도 승인 필요 항목으로 보고됨.
KNOWN_UNFIXED_GAPS = {"data_collector"}

# 저장소 최상위의 "내부 패키지" 디렉터리만 대상으로 한다 (tests/docs/migrations 등 제외).
INTERNAL_PACKAGES = {
    p.name
    for p in REPO_ROOT.iterdir()
    if p.is_dir()
    and not p.name.startswith(".")
    and p.name not in {"tests", "docs", "migrations"}
}

IMPORT_RE = re.compile(
    r"^\s*(?:from|import)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\.[a-zA-Z0-9_.]+)?\s+import\b|^\s*import\s+([a-zA-Z_][a-zA-Z0-9_]*)\b"
)


def _imported_internal_packages(py_file: Path) -> set:
    found = set()
    for line in py_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = IMPORT_RE.match(line)
        if not m:
            continue
        pkg = m.group(1) or m.group(2)
        if pkg in INTERNAL_PACKAGES:
            found.add(pkg)
    return found


def _copied_dirs(dockerfile: Path) -> set:
    copied = set()
    for line in dockerfile.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\s*COPY\s+([A-Za-z0-9_./]+)/\s+", line)
        if m:
            copied.add(m.group(1).strip("./"))
    return copied


class DockerfileDashboardImportsTest(unittest.TestCase):
    def test_dashboard_imports_are_all_copied_in_dockerfile(self):
        required = set()
        for py_file in DASHBOARD_DIR.glob("*.py"):
            required |= _imported_internal_packages(py_file)

        copied = _copied_dirs(DOCKERFILE_DASHBOARD)
        # dashboard 자신은 COPY dashboard/ ./dashboard/ 로 별도 처리되므로 제외.
        required.discard("dashboard")

        missing = sorted(required - copied - KNOWN_UNFIXED_GAPS)
        self.assertEqual(
            missing,
            [],
            f"dashboard/*.py가 import하지만 Dockerfile.dashboard에 COPY되지 않은 "
            f"저장소 내부 패키지: {missing}",
        )

    def test_known_unfixed_gaps_do_not_silently_grow(self):
        """KNOWN_UNFIXED_GAPS에 적힌 패키지가 실제로 여전히 누락 상태인지 확인한다.
        누군가 data_collector를 COPY에 추가해 고치면 이 테스트가 실패해 예외
        목록을 정리하라고 알려준다(허용 목록이 죽은 코드로 남지 않도록)."""
        required = set()
        for py_file in DASHBOARD_DIR.glob("*.py"):
            required |= _imported_internal_packages(py_file)
        copied = _copied_dirs(DOCKERFILE_DASHBOARD)

        stale = KNOWN_UNFIXED_GAPS - (required - copied)
        self.assertEqual(
            stale,
            set(),
            f"더 이상 누락 상태가 아닌 KNOWN_UNFIXED_GAPS 항목: {stale} — "
            f"목록에서 제거하세요.",
        )


if __name__ == "__main__":
    unittest.main()
