
FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY . /app
WORKDIR /app

COPY ./src /app/src
COPY ./requirements.txt /app/requirements.txt

RUN pip install --upgrade pip && \
	pip install -r requirements.txt

WORKDIR /app/src