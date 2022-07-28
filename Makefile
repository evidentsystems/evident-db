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

.PHONY: redpanda-topics
redpanda-topics:
	-rpk topic create evidentdb-internal-commands -p 12 -r 3 -c min.insync.replicas=2 -c compression.type=snappy -c retention.ms="-1" $(REDPANDA_BROKERS)
	-rpk topic create evidentdb-internal-events -p 12 -r 3 -c min.insync.replicas=2 -c compression.type=snappy -c retention.ms="-1" $(REDPANDA_BROKERS)
	-rpk topic create evidentdb-databases -p 12 -r 3 -c min.insync.replicas=2 -c compression.type=snappy -c cleanup.policy=compact $(REDPANDA_BROKERS)
	-rpk topic create evidentdb-database-names -p 12 -r 3 -c min.insync.replicas=2 -c compression.type=snappy -c cleanup.policy=compact $(REDPANDA_BROKERS)
	-rpk topic create evidentdb-batches -p 12 -r 3 -c min.insync.replicas=2 -c compression.type=snappy -c cleanup.policy=compact $(REDPANDA_BROKERS)
	-rpk topic create evidentdb-streams -p 12 -r 3 -c min.insync.replicas=2 -c compression.type=snappy -c cleanup.policy=compact $(REDPANDA_BROKERS)
	-rpk topic create evidentdb-events -p 12 -r 3 -c min.insync.replicas=2 -c compression.type=snappy -c cleanup.policy=compact $(REDPANDA_BROKERS)

.PHONY: loc
loc:
	tokei adapters app domain proto service transactor
