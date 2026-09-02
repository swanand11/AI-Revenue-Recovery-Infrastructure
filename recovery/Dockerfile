FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY recovery ./recovery

CMD ["python", "-u", "-m", "recovery.consumer.main"]
