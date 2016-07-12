FROM debian:stretch

ENV GOPATH /go

RUN mkdir /go && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        golang \
        gcc \
        build-essential \
        ca-certificates \
        libsystemd-dev && \
    go get github.com/nutmegdevelopment/sumologic/journalstream && \
    cp /go/bin/journalstream / && \
    apt-get remove -y \
        git \
        golang \
        gcc \
        build-essential \
        ca-certificates \
        libsystemd-dev && \
    apt-get clean && \
    rm -rf /go

CMD [ "/journalstream" ]
ENTRYPOINT [ "/journalstream" ]