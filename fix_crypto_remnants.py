import re
import traceback

print("Fixing config.py...")
with open('/tmp/autotrader_full/common/config.py', 'r') as f:
    text = f.read()
text = re.sub(r'\s*# 수집할 코인 페어.*?\n\s*\]\n', '\n', text, flags=re.DOTALL)
with open('/tmp/autotrader_full/common/config.py', 'w') as f:
    f.write(text)

print("Fixing database.py...")
with open('/tmp/autotrader_full/common/database.py', 'r') as f:
    text = f.read()
text = re.sub(r'table = "stock_ohlcv" if asset == "stock" else "crypto_ohlcv"', 'table = "stock_ohlcv"', text)
text = re.sub(r'col = "symbol" if asset == "stock" else "pair"', 'col = "symbol"', text)
with open('/tmp/autotrader_full/common/database.py', 'w') as f:
    f.write(text)

print("Fixing dashboard/main.py...")
with open('/tmp/autotrader_full/dashboard/main.py', 'r') as f:
    content = f.read()

routes_to_remove = [
    r'@app\.get\("/crypto",.*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.get\("/crypto\.html".*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.get\("/api/prices/crypto"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.get\("/api/ohlcv/crypto/\{pair\}"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.get\("/api/crypto/trade-mode"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.post\("/api/crypto/trade-mode"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.post\("/api/collect/crypto/reset"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.post\("/api/crypto/chat"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.get\("/api/crypto/stats"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.get\("/api/positions/crypto"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.get\("/api/balance/crypto"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
    r'@app\.get\("/api/mock/prices/crypto"\).*?(?=\n@app|\n# ──|\nasync def \w|\Z)',
]

for pat in routes_to_remove:
    content = re.sub(pat, '', content, flags=re.DOTALL)

with open('/tmp/autotrader_full/dashboard/main.py', 'w') as f:
    f.write(content)

print("Done.")
