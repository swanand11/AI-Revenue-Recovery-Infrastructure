FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/capture ./services/capture

CMD ["python", "-u", "services/capture/main.py"]
