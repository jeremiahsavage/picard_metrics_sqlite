FROM ubuntu:16.04

MAINTAINER Jeremiah H. Savage <jeremiahsavage@gmail.com>

ENV picard_metrics_sqlite 0.1

USER root

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get upgrade -y --force-yes

RUN apt-get update && apt-get install -y --force-yes \
    git \
    python-pip \
    python3-dev \
    sudo

RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN adduser --disabled-password --gecos '' ubuntu && adduser ubuntu sudo && echo "ubuntu	ALL=(ALL)	NOPASSWD:ALL" >> /etc/sudoers.d/ubuntu

RUN pip install picard_metrics_sqlite

WORKDIR /usr/local

RUN git clone https://github.com/jeremiahsavage/exome_intervals.git

USER ubuntu

ENV HOME /home/ubuntu

WORKDIR ${HOME}