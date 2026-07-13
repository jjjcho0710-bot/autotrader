FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app
COPY common/ ./common/
COPY ml/ ./ml/
COPY stock_trader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY stock_trader/ ./stock_trader/
WORKDIR /app/stock_trader
CMD ["python", "main.py"]
