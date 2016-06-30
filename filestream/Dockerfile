FROM scratch

ADD https://curl.haxx.se/ca/cacert.pem /etc/ssl/certs/cacert.pem
ADD filestream /filestream

CMD [ "/filestream" ]
ENTRYPOINT [ "/filestream" ]