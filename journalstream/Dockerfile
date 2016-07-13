FROM debian:stretch

ENV GOPATH /go

RUN mkdir /go

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        golang \
        gcc \
        build-essential \
        ca-certificates \
        libsystemd-dev

RUN go get -d -v github.com/nutmegdevelopment/sumologic/...
ADD . /go/src/github.com/nutmegdevelopment/sumologic/journalstream
   
RUN go get -d -v github.com/nutmegdevelopment/sumologic/journalstream/... && \
    go install github.com/nutmegdevelopment/sumologic/journalstream && \
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