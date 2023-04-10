FROM python:3.7-alpine

RUN apk add --no-cache \
      build-base \
      libffi-dev \
      openssl-dev \
 && pip install --upgrade pip setuptools wheel \
 && pip install snowflake-connector-python==2.9.0 --no-use-pep517
 
COPY requirements.txt /

RUN pip install -r /requirements.txt

COPY ./RapidAPIFramework.py /app/RapidAPIFramework.py

COPY ./crontab /app/crontab

RUN crontab /app/crontab

RUN touch /tmp/out.log

CMD crond && tail -f /tmp/out.log