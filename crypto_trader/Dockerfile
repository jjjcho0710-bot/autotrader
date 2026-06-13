FROM python:3.12-slim
WORKDIR /app
COPY common/ ./common/
COPY crypto_trader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY crypto_trader/ ./crypto_trader/
WORKDIR /app/crypto_trader
CMD ["python", "main.py"]
