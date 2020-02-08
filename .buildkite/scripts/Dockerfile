FROM ubuntu:18.04

EXPOSE 8080

COPY blockchain-http*.deb /tmp
RUN dpkg -i /tmp/blockchain-http*.deb
RUN rm -f /tmp/blockchain-http*.deb

RUN apt update
RUN apt install -y libssl1.1
RUN apt clean

CMD /var/helium/blockchain_http/bin/blockchain_http foreground
