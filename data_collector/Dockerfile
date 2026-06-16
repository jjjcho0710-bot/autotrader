FROM python:3.12-slim
WORKDIR /app
# 공통 모듈 복사
COPY common/ ./common/
COPY ml/ ./ml/
# data_collector 복사
COPY data_collector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY data_collector/ ./data_collector/
WORKDIR /app/data_collector
CMD ["python", "main.py"]
