FROM python:3.7 AS base

WORKDIR /app

COPY ./requirements.txt /app

RUN pip install -r requirements.txt

COPY . /app
