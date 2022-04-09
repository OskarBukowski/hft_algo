FROM python:3.8-slim-buster

RUN mkdir /opt/hft

COPY requirements.txt /opt/hft/requirements.txt

RUN pip install -r /opt/hft/requirements.txt

COPY . /opt/hft

