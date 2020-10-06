# image name: trial-sync-env

FROM python:3.7-alpine
LABEL maintainer="bo.yan@csiro.au"

WORKDIR /setup

ADD requirements.txt /setup

RUN pip install -r /setup/requirements.txt

VOLUME /app
WORKDIR /app
