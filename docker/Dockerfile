FROM ubuntu:xenial-20160809

MAINTAINER Jeremiah H. Savage <jeremiahsavage@gmail.com>

USER root

RUN apt-get update && apt-get install -y \
    python3-pandas \
    python3-pip \
    python3-sqlalchemy \
    wget

RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV version 0.16

RUN pip3 install picard_metrics_sqlite

#RUN git clone https://github.com/jeremiahsavage/exome_intervals.git
