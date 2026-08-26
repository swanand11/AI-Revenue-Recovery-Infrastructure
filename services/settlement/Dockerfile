FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/settlement ./services/settlement

CMD ["python", "-u", "services/settlement/main.py"]
