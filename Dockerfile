FROM erlang:24.1.6.0-alpine as builder

WORKDIR /app
ENV REBAR_BASE_DIR /app/_build

RUN apk add --no-cache --update \
    git tar build-base linux-headers autoconf automake libtool pkgconfig \
    dbus-dev bzip2 bison flex gmp-dev cmake lz4 libsodium-dev openssl-dev \
    sed curl cargo

# Install Rust toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# build and cache dependencies as their own layer
COPY rebar3 rebar.config rebar.lock ./
RUN ./rebar3 compile

COPY . .
RUN ./rebar3 compile

RUN mkdir -p /opt/rel && \
    ./rebar3 as prod tar && \
    tar -zxvf $REBAR_BASE_DIR/prod/rel/*/*.tar.gz -C /opt/rel

FROM alpine:3.14 as runner

RUN apk add --update openssl libsodium ncurses libstdc++

ENV COOKIE=blockchain_http \
    RELX_OUT_FILE_PATH=/tmp

WORKDIR /opt/blockchain_http
EXPOSE 8080

COPY --from=builder /opt/rel .

ENTRYPOINT ["/opt/blockchain_http/bin/blockchain_http"]
CMD ["foreground"]
