FROM ubuntu:bionic-20180426

env VERSION 0.51

RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y \
       python3-pandas \
       python3-pip \
       python3-sqlalchemy \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY picard_metrics_sqlite setup.cfg setup.py /opt/
RUN cd /opt/ && python3 setup.py install