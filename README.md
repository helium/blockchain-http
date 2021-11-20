# blockchain-http

[![CI](https://github.com/helium/blockchain-http/actions/workflows/ci.yml/badge.svg)](https://github.com/helium/blockchain-http/actions/workflows/ci.yml)
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
wget https://packages.erlang-solutions.com/erlang-solutions_2.0_all.deb
sudo dpkg -i erlang-solutions_2.0_all.deb
sudo apt-get update
sudo apt install esl-erlang=1:23.2.3-1 cmake libsodium-dev libssl-dev
sudo apt install build-essential
```


## WARNING

This application does NOT serve up over TLS, and does NOT rate
control, or access control clients. Please run this service behind a
load balancer that terminates SSL and does some rate and access
control.

## Using Docker

### Building the Docker Image

`docker build -t helium/api .`

### Running the Docker Container

```
docker run -d --init \
--restart unless-stopped \
--publish 8080:8080/tcp \
--name api \
--mount type=bind,source=$HOME/api_data,target=/var/data \
-e DATABASE_RO_URL=postgresql://user:pass@127.0.0.1:5432/helium_blockchain \
-e DATABASE_RW_URL=postgresql://user:pass@127.0.0.1:5432/helium_blockchain \
-e DATABASE_RO_POOL_SIZE=10 \
helium/api
```
### Updating Docker

#### Navigate to your copy of the `blockchain-http` repository.

`cd /path/to/blockchain-http`

#### Stop the Docker container.

`docker stop api`

#### Remove the existing Docker container.

`docker rm api`

#### Update the repository.

`git pull`

#### Rebuild the Docker image.

`docker build -t helium/api .`

#### Run the updated Docker container.

```
docker run -d --init \
--restart unless-stopped \
--publish 8080:8080/tcp \
--name api \
--mount type=bind,source=$HOME/api_data,target=/var/data \
-e DATABASE_RO_URL=postgresql://user:pass@127.0.0.1:5432/helium_blockchain \
-e DATABASE_RW_URL=postgresql://user:pass@127.0.0.1:5432/helium_blockchain \
-e DATABASE_RO_POOL_SIZE=10 \
helium/api
```
