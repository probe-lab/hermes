GOCC := "go"
BIN := "./build/hermes"
TARGET_PATH := "./cmd/hermes"

GIT_PACKAGE := "github.com/probe-lab/hermes"
REPO_SERVER := "019120760881.dkr.ecr.us-east-1.amazonaws.com"
COMMIT := `git rev-parse --short HEAD`

default:
    @just --list --justfile {{ justfile() }}

run:
    {{GOCC}} run {{BIN}} run

build:
	{{GOCC}} build -o {{TARGET_PATH}} {{BIN}}

clean:
	@rm -r $(BIN_PATH)

format:
	{{GOCC}} fmt ./...
	{{GOCC}} mod tidy -v

lint:
	{{GOCC}} mod verify
	{{GOCC}} vet ./...
	{{GOCC}} run honnef.co/go/tools/cmd/staticcheck@latest ./...

test:
    {{GOCC}} test -race -buildvcs -vet=off ./...
    {{GOCC}} test ./...

docker:
	docker build -t probe-lab/hermes:latest -t probe-lab/hermes-{{COMMIT}} -t {{REPO_SERVER}}/probelab:hermes-{{COMMIT}} .

docker-push:
	docker push {{REPO_SERVER}}/probelab:hermes-{{COMMIT}}
	docker rmi {{REPO_SERVER}}/probelab:hermes-{{COMMIT}}
