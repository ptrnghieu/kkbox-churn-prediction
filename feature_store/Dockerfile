FROM python:3.11-slim

WORKDIR /app

RUN pip install "feast[gcp,redis]" python-dotenv

COPY . .

CMD ["sleep", "infinity"]
