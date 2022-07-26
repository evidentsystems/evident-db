SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

GRADLE := ./gradlew

default: build

.PHONY: clean
clean:
	$(GRADLE) clean

app/build/docker/main/Dockerfile:
	$(GRADLE) dockerfile

.PHONY: dockerfile
dockerfile: app/build/docker/main/Dockerfile

app/build/libs/app-*-all.jar:
	$(GRADLE) assemble

.PHONY: build
build: app/build/libs/app-*-all.jar

.PHONY: loc
loc:
	tokei adapters app domain proto service transactor
