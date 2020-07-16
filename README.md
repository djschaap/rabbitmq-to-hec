# rabbitmq-to-hec

[![Build Status - master](https://travis-ci.com/djschaap/rabbitmq-to-hec.svg?branch=master)](https://travis-ci.com/djschaap/rabbitmq-to-hec)

Project Home: https://github.com/djschaap/rabbitmq-to-hec

Docker Hub: https://cloud.docker.com/repository/docker/djschaap/rabbitmq-to-hec

## Overview

rabbitmq-to-hec reads messages from a RabbitMQ queue and submits them to
Splunk HTTP Event Collector (HEC).

## Run Tests

```bash
go test ./...

# compute/show coverage
go test -coverprofile=coverage.out ./... \
  && go tool cover -html=coverage.out
```

## Build/Release

### Prepare Release / Compile Locally

```bash
go fmt ./...
go mod tidy
go test ./...
# commit any changes
BUILD_DT=`date +%FT%T%z`
COMMIT_HASH=`git rev-parse --short HEAD`
FULL_COMMIT=`git log -1`
VER=0.0.0
go build -ldflags \
  "-X main.buildDt=${BUILD_DT} -X main.commit=${COMMIT_HASH} -X main.version=${VER}" \
  -o cli cmd/cli/main.go
```

### Build Container (Manually)

```bash
docker build -t rabbitmq-to-hec .
```

### Run Container

```bash
export MQ_URL=amqp://guest:guest@localhost:5672
docker run -d -e MQ_URL \
  -e SRC_QUEUE=q_name \
  -e HEC_URL=https://splunk.example.com:8088 \
  -e HEC_TOKEN=00000000-0000-0000-0000-000000000000 \
  rabbitmq-to-hec
# append "/cli -lifetime 5" to run for 5 seconds and exit
```
