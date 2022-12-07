FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN apt update && apt -y install libpq-dev gcc

RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "main.py"]