SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

GRADLE ?= ./gradlew
CARGO  ?= cargo

default: build

app/build/docker/main/Dockerfile:
	$(GRADLE) dockerfile

.PHONY: dockerfile
dockerfile: app/build/docker/main/Dockerfile

app/build/libs/app-*-all.jar:
	$(GRADLE) assemble

.PHONY: build
build: app/build/libs/app-*-all.jar

REPLICATION_FACTOR ?= 1
KAFKA_BOOTSTRAP_SERVERS ?= localhost:9092

DOCKER_COMPOSE ?= docker compose

.PHONY: start-kafka
start-kafka:
	$(DOCKER_COMPOSE) up -d

.PHONY: stop-kafka
stop-kafka:
	$(DOCKER_COMPOSE) stop

.PHONY: kafka-topics
kafka-topics: start-kafka
	-kafka-topics --create --if-not-exists --topic evidentdb-internal-commands --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config retention.ms="-1" --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-internal-events --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config retention.ms="-1" --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-databases --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-database-names --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-batches --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-streams --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-events --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)

# Testing and Performance

.PHONY: test
test:
	$(GRADLE) test
	cd perf/ && $(CARGO) test

.PHONY: run
run: kafka-topics
	$(GRADLE) run

.PHONY: perf
perf:
	cd perf/ && $(CARGO) run

# Clean up

.PHONY: clean
clean:
	$(GRADLE) clean
	cd perf/ && $(CARGO) clean

.PHONY: clean-kafka-topics
clean-kafka-topics:
	-kafka-topics --delete --if-exists --topic evidentdb-internal-commands --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-internal-events --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-databases --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-database-names --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-batches --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-streams --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-events --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)

.PHONY: clean-kafka-streams-data
clean-kafka-streams-data:
	rm -rf data/service/* data/transactor/*
	rm -rf app/data/service/* app/data/transactor/*

.PHONY: clean-kafka
clean-kafka:
	$(DOCKER_COMPOSE) down

.PHONY: clean-all
clean-all: clean clean-kafka-topics clean-kafka clean-kafka-streams-data

# Util

.PHONY: loc
loc:
	tokei adapters app domain perf proto service transactor

# .PHONY: redpanda-topics
# redpanda-topics:
# 	-rpk topic create evidentdb-internal-commands -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c retention.ms="-1" $(KAFKA_BOOTSTRAP_SERVERS)
# 	-rpk topic create evidentdb-internal-events -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c retention.ms="-1" $(KAFKA_BOOTSTRAP_SERVERS)
# 	-rpk topic create evidentdb-databases -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact $(KAFKA_BOOTSTRAP_SERVERS)
# 	-rpk topic create evidentdb-database-names -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact $(KAFKA_BOOTSTRAP_SERVERS)
# 	-rpk topic create evidentdb-batches -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact $(KAFKA_BOOTSTRAP_SERVERS)
# 	-rpk topic create evidentdb-streams -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact $(KAFKA_BOOTSTRAP_SERVERS)
# 	-rpk topic create evidentdb-events -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact $(KAFKA_BOOTSTRAP_SERVERS)

