FROM python:3.12-slim
WORKDIR /app
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
COPY common/ ./common/
COPY data_collector/ ./data_collector/
COPY stock_trader/ ./stock_trader/
COPY crypto_trader/ ./crypto_trader/
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dashboard/ ./dashboard/
ENV PYTHONPATH=/app
WORKDIR /app/dashboard
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
