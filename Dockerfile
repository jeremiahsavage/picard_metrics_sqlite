# FROM ubuntu:bionic-20180426
FROM quay.io/ncigdc/python38-builder as builder

COPY ./ /opt

WORKDIR /opt

RUN pip install tox && tox -p

FROM quay.io/ncigdc/python38

COPY --from=builder /opt/dist/*.tar.gz /opt
COPY requirements.txt /opt

WORKDIR /opt

RUN pip install -r requirements.txt *.tar.gz \
	&& rm -f *.tar.gz requirements.txt

ENTRYPOINT ["picard_metrics_sqlite"]

CMD ["--help"]
