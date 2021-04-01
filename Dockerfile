FROM golang:1.15 as builder
ARG SOURCE_COMMIT=unset
ARG VER=1.0.2
RUN mkdir /app
COPY . /app/
WORKDIR /app
RUN \
  BUILD_DT=`date +%FT%T%z` \
  && COMMIT_HASH=$(echo $SOURCE_COMMIT | cut -c 1-7) \
  && echo "BUILD build_dt=${BUILD_DT}" \
  && echo "BUILD commit_hash=${COMMIT_HASH}" \
  && echo "BUILD version=${VER}" \
  && CGO_ENABLED=0 GOOS=linux go build -ldflags \
    "-X main.buildDt=${BUILD_DT} -X main.commit=${COMMIT_HASH} -X main.version=${VER}" \
    -o cli cmd/cli/main.go

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/.env /
COPY --from=builder /app/cli /
USER 999:999
CMD ["/cli","-lifetime","0"]
