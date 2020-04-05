# blockchain-http

[![Build status](https://badge.buildkite.com/dccd9ab02cd8867a21f53479adcdc692edf426f6bb63732038.svg?branch=master)](https://buildkite.com/helium/blockchain-http)
[![codecov](https://codecov.io/gh/helium/blockchain-http/branch/master/graph/badge.svg)](https://codecov.io/gh/helium/blockchain-http)

This is an Erlang application to serve up the Helium blockchain as
stored by the
[blockchain-etl](https://github.com/helium/blockchain-etl) service and
schema. The two applications rely on the schema being compatible to
work


## Developer Usage

* Clone this repository
* Create `.env` file by copying `.env.template` and editing it to
  reflect your postgres read-only and read-write access URLs

* Run `make release` in the top level folder

* Run `make start` to start the application. Logs will be at
  `_build/default/rel/blockchain_http/log/*`.

Once started the application will start serving up the blockchain
through a number of routes. Documentation for these routes will be
added soon.

### Installing Ubuntu Required Packages

If running on Ubuntu, you will need the following packages installed before
running `make release`:

```bash
wget https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
sudo dpkg -i erlang-solutions_1.0_all.deb
sudo apt-get update
sudo apt install esl-erlang cmake libsodium-dev libssl-dev
```


## WARNING

This application does NOT serve up over TLS, and does NOT rate
control, or access control clients. Please run this service behind a
load balancer that terminates SSL and does some rate and access
control.
