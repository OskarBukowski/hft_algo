FROM python:3.8-slim-buster

RUN addgroup docker && adduser --no-create-home \
--disabled-password --ingroup docker docker

RUN mkdir /opt/hft

COPY requirements.txt /opt/hft/requirements.txt

RUN pip install -r /opt/hft/requirements.txt

COPY . /opt/hft

USER docker

CMD ["/bin/bash", "/opt/hft/db_ex_connections/start_all.sh"]