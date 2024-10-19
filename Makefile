export PATH := $(GOPATH)/bin:$(PATH)
export GO111MODULE=on
LDFLAGS := -X 'main.time=$(date -u --rfc-3339=seconds)' -X 'main.git=$(git log --pretty=format:"%h" -1)'

all: fmt vet build

build: mysqlClone mysqlExecute mongoClone redisClone

mysqlClone:
	env CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/mysqlClone ./mysqlClone

mysqlExecute:
	env CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/mysqlExecute ./mysqlExecute

mongoClone:
	env CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/mongoClone ./mongoClone

redisClone:
	env CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/redisClone ./redisClone

fmt:
	go fmt ./...

vet:
	go vet ./...
