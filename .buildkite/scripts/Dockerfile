FROM ubuntu:18.04

EXPOSE 8080

COPY blockchain-http*.deb /
RUN dpkg -i blockchain-http*.deb
RUN rm -f blockchain-http*.deb
RUN touch /var/helium/blockchain_http/.env

COPY .buildkite/scripts/lager_console_setup.pl /
RUN perl -i lager_console_setup.pl /var/helium/blockchain_http/releases/0.1.0/sys.config
RUN rm -f lager_console_setup.pl

RUN apt update
RUN apt install -y libssl1.1 libsodium23
RUN apt clean
