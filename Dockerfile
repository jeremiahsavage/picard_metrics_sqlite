# FROM ubuntu:bionic-20180426
FROM quay.io/ncigdc/python38-builder as builder

COPY ./ /opt

WORKDIR /opt

env VERSION 0.51

RUN pip install .

# RUN apt-get update \
#     && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get install -y \
#        python3-pandas \
#        python3-pip \
#        python3-sqlalchemy \
#        wget \
#     && apt-get clean \
#     && pip3 install /opt/picard_metrics_sqlite \
#     && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*