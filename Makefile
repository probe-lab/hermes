REPO_SERVER=019120760881.dkr.ecr.us-east-1.amazonaws.com

GIT_DIRTY := $(shell git diff --quiet || echo '-dirty')
GIT_SHA := $(shell git rev-parse --short HEAD)
GIT_TAG := ${GIT_SHA}${GIT_DIRTY}

GOCC=go
TARGET_PATH=./cmd/hermes
BIN_PATH=./build
BIN=./build/hermes
GIT_PACKAGE=github.com/probe-lab/hermes

.PHONY: install uninstall build clean tidy format docker docker-push

build: 
	$(GOCC) get $(TARGET_PATH)
	$(GOCC) build -o $(BIN) $(TARGET_PATH)

install:
	$(GOCC) install $(GIT_PACKAGE)

uninstall:
	$(GOCC) clean $(GIT_PACKAGE)

format:
	gofumpt -w -l .

docker:
	docker build --platform linux/amd64 -t "${REPO_SERVER}/probelab:hermes-${GIT_TAG}" .

docker-push: docker
	docker push "${REPO_SERVER}/probelab:hermes-${GIT_TAG}"
	docker rmi "${REPO_SERVER}/probelab:hermes-${GIT_TAG}"
