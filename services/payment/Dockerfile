FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/payment ./services/payment

CMD ["python", "-u", "services/payment/main.py"]
