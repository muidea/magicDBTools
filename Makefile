export PATH := $(GOPATH)/bin:$(PATH)
export GO111MODULE=on
LDFLAGS := -X 'main.time=$(date -u --rfc-3339=seconds)' -X 'main.git=$(git log --pretty=format:"%h" -1)'

all: fmt vet build

build: mysqlCloneBuild mysqlExecuteBuild mongoCloneBuild redisCloneBuild

mysqlCloneBuild:
	env CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/mysqlClone ./mysqlClone

mysqlExecuteBuild:
	env CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/mysqlExecute ./mysqlExecute

mongoCloneBuild:
	env CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/mongoClone ./mongoClone

redisCloneBuild:
	env CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/redisClone ./redisClone

fmt:
	go fmt ./...

vet:
	go vet ./...
