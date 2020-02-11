FROM ubuntu:18.04

EXPOSE 8080

COPY blockchain-http*.deb /tmp
RUN dpkg -i /tmp/blockchain-http*.deb
RUN rm -f /tmp/blockchain-http*.deb
RUN touch /var/helium/.env

RUN apt update
RUN apt install -y libssl1.1 libsodium23
RUN apt clean
