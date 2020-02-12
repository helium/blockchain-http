FROM ubuntu:18.04

EXPOSE 8080

COPY blockchain-http*.deb /
RUN dpkg -i blockchain-http*.deb
RUN rm -f blockchain-http*.deb
RUN touch /var/helium/blockchain_http/.env

RUN apt update
RUN apt install -y libssl1.1 libsodium23
RUN apt clean
