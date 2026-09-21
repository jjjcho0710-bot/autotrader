import re

with open('/tmp/autotrader_full/dashboard/main.py', 'r') as f:
    content = f.read()

funcs = [
    "crypto", "crypto_html", "get_crypto_prices", "get_crypto_ohlcv",
    "get_trade_mode", "set_trade_mode", "reset_crypto_pairs",
    "crypto_chat", "get_crypto_stats", "get_crypto_positions",
    "get_crypto_balance", "mock_crypto_prices"
]

for fn in funcs:
    # Match from "async def fn(" up to the next top-level declaration or section comment
    pat = r'^async def ' + fn + r'\(.*?(?=^@|^async def |^# ──|\Z)'
    content = re.sub(pat, '', content, flags=re.MULTILINE|re.DOTALL)

with open('/tmp/autotrader_full/dashboard/main.py', 'w') as f:
    f.write(content)

