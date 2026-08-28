FROM python:3.12-slim

WORKDIR /app

ENV PYTHONPATH=/app

COPY common ./common
COPY dashboard ./dashboard

EXPOSE 8080

CMD ["python", "-u", "-m", "dashboard.server"]

