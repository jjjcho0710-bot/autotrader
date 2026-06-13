FROM python:3.12-slim
WORKDIR /app

# common + data_collector 복사
COPY common/ ./common/
COPY data_collector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY data_collector/ ./data_collector/

# Python path에 /app 추가 → common 모듈 인식
ENV PYTHONPATH=/app

WORKDIR /app/data_collector
CMD ["python", "main.py"]
