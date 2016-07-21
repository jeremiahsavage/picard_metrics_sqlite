FROM ubuntu:16.04

MAINTAINER Jeremiah H. Savage <jeremiahsavage@gmail.com>

USER root

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get upgrade -y

RUN apt-get update && apt-get install -y \
    git \
    python3-pip \
    python3-pandas \
    python3-sqlalchemy \
    sudo

RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV version 0.6

RUN pip3 install picard_metrics_sqlite

WORKDIR /usr/local

RUN git clone https://github.com/jeremiahsavage/exome_intervals.git

RUN adduser --disabled-password --gecos '' ubuntu && adduser ubuntu sudo && echo "ubuntu	ALL=(ALL)	NOPASSWD:ALL" >> /etc/sudoers.d/ubuntu

USER ubuntu

ENV HOME /home/ubuntu

WORKDIR ${HOME}