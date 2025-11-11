FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y python3 python3-pip vim software-properties-common

WORKDIR /app
COPY . /app

RUN pip3 install -r requirements.txt
