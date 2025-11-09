FROM python:3.11-slim

WORKDIR /app
COPY router.py /app/router.py

# Opcional: acelera DNS dentro do container
RUN pip install --no-cache-dir --upgrade pip

# UDP 6000 é usado internamente entre containers
EXPOSE 6000/udp

ENTRYPOINT ["python", "/app/router.py"]