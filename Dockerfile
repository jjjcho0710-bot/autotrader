FROM python:3.12-slim
WORKDIR /app

COPY common/ ./common/
COPY data_collector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY data_collector/ ./data_collector/

WORKDIR /app/data_collector
CMD ["python", "main.py"]
