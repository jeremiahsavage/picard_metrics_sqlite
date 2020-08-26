FROM python:3.6-slim

COPY picard_metrics_sqlite setup.cfg setup.py /opt/picard_metrics_sqlite/
RUN cd /opt/picard_metrics_sqlite/ && python3 setup.py install
