FROM ubuntu:bionic-20180426

MAINTAINER Jeremiah H. Savage <jeremiahsavage@gmail.com>

env VERSION 0.46

RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y \
       python3-pandas \
       python3-pip \
       python3-sqlalchemy \
       wget \
    && apt-get clean \
    && pip3 install picard_metrics_sqlite \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*