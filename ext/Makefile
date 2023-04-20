OS := $(shell uname)

ifeq ($(OS),Darwin)
	GOFILE = go1.20.3.darwin-arm64.tar.gz
else
	GOFILE = go1.20.3.linux-amd64.tar.gz
endif

build:
	curl -O https://dl.google.com/go/${GOFILE}
	tar -xzf ${GOFILE}
	go/bin/go build -buildmode=c-shared -o ruby_snowflake_client.so ruby_snowflake.go

install:
clean:


.PHONY: build
