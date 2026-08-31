FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY detection ./detection
COPY runner ./runner

CMD ["python", "-u", "runner/bridge_ingestion.py"]
