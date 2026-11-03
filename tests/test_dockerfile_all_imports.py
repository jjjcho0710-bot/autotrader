"""[AT] fix/dockerfile-data-collector — 서비스별 Dockerfile COPY 누락 전수 검사.

각 서비스 이미지가 실행하는 디렉터리(및 COPY된 공용 패키지)의 .py가 import하는 저장소 내부
최상위 패키지가 모두 해당 Dockerfile에 COPY돼 있는지 AST로 검사한다(지연 import 포함).
루트 Dockerfile은 레거시(crypto_trader/ 디렉터리가 저장소에 없음)라 검사 대상에서 제외한다.
"""
import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INTERNAL = {p.name for p in ROOT.iterdir()
            if p.is_dir() and not p.name.startswith(".") and p.name not in {"tests", "docs", "migrations"}}
# 서비스 이미지 -> (Dockerfile, 코드를 검사할 COPY된 디렉터리 범위)
IMAGES = {
    "dashboard": "Dockerfile.dashboard",
    "stock_trader": "Dockerfile.stock",
    "data_collector": "Dockerfile.data",
}
# 알려진 미해결 누락 {dockerfile: {패키지: 사유}} — 비어 있어야 한다.
KNOWN_GAPS = {}


def _copied(dockerfile):
    out = set()
    for line in (ROOT / dockerfile).read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\s*COPY\s+([A-Za-z0-9_./]+)/\s+", line)
        if m:
            out.add(m.group(1).strip("./"))
    return out


def _imports(pkg_dir):
    found = {}
    for py in (ROOT / pkg_dir).rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for n in ast.walk(tree):
            mods = []
            if isinstance(n, ast.Import):
                mods = [a.name for a in n.names]
            elif isinstance(n, ast.ImportFrom) and n.module and n.level == 0:
                mods = [n.module]
            for m in mods:
                top = m.split(".")[0]
                if top in INTERNAL:
                    found.setdefault(top, f"{py.relative_to(ROOT)}:{n.lineno}")
    return found


class TestDockerfileImports(unittest.TestCase):
    def test_every_service_image_copies_what_its_code_imports(self):
        problems = []
        for service, dockerfile in IMAGES.items():
            copied = _copied(dockerfile)
            for pkg in sorted(copied):
                if not (ROOT / pkg).is_dir():
                    problems.append(f"{dockerfile}: COPY {pkg}/ 대상 디렉터리가 없음")
                    continue
                for top, where in _imports(pkg).items():
                    if top == pkg or top in copied or top in KNOWN_GAPS.get(dockerfile, {}):
                        continue
                    problems.append(f"{dockerfile}: {pkg} 코드({where})가 {top}를 import하지만 COPY 없음")
        self.assertEqual(problems, [], "\n".join(problems))
