FROM python:3.12-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY common ./common
COPY dashboard ./dashboard
COPY recovery ./recovery
COPY runner ./runner
COPY services/payment ./services/payment

EXPOSE 8080

CMD ["python", "-u", "-m", "dashboard.server"]
