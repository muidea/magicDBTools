FROM golang:1.22.8 AS building

COPY . /building
WORKDIR /building

RUN make build

FROM busybox:latest

ARG AppName=busybox-database
LABEL Author="rangh"
LABEL Application=$AppName

COPY --from=building /building/bin/mysqlClone /bin/
COPY --from=building /building/bin/mysqlExecute /bin/
COPY --from=building /building/bin/mongoClone /bin/
COPY --from=building /building/bin/redisClone /bin/
