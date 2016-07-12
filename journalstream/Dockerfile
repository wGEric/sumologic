FROM fedora:23

ADD https://curl.haxx.se/ca/cacert.pem /etc/ssl/certs/cacert.pem
ADD journalstream /journalstream

CMD [ "/journalstream" ]
ENTRYPOINT [ "/journalstream" ]