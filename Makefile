SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

GRADLE ?= ./gradlew
CARGO  ?= cargo
# or redpanda
CLUSTER_TYPE ?= kafka
REPLICATION_FACTOR ?= 1
KAFKA_BOOTSTRAP_SERVERS ?= localhost:9092
DOCKER_COMPOSE ?= docker compose

default: build

app/build/docker/main/Dockerfile:
	$(GRADLE) dockerfile

.PHONY: dockerfile
dockerfile: app/build/docker/main/Dockerfile

app/build/libs/app-*-all.jar:
	$(GRADLE) assemble

.PHONY: build
build: app/build/libs/app-*-all.jar

# Kafka Cluster

.PHONY: start-kafka
start-kafka:
	$(DOCKER_COMPOSE) -p evident-db-kafka -f docker-compose.kafka.yml up -d

.PHONY: stop-kafka
stop-kafka:
	$(DOCKER_COMPOSE) -p evident-db-kafka -f docker-compose.kafka.yml stop

.PHONY: clean-kafka
clean-kafka:
	$(DOCKER_COMPOSE) -p evident-db-kafka -f docker-compose.kafka.yml down

.PHONY: kafka-topics
kafka-topics: start-kafka
	-kafka-topics --create --if-not-exists --topic evidentdb-internal-commands --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config retention.ms="-1" --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-internal-events --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config retention.ms="-1" --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-databases --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-database-names --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-batches --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-streams --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-events --partitions 12 --replication-factor $(REPLICATION_FACTOR) --config compression.type=snappy --config cleanup.policy=compact --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)

.PHONY: clean-kafka-topics
clean-kafka-topics:
	-kafka-topics --delete --if-exists --topic evidentdb-internal-commands --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-internal-events --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-databases --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-database-names --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-batches --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-streams --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-events --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)

# Redpanda Cluster

.PHONY: start-redpanda
start-redpanda:
	$(DOCKER_COMPOSE) -p evident-db-redpanda -f docker-compose.redpanda.yml up -d

.PHONY: stop-redpanda
stop-redpanda:
	$(DOCKER_COMPOSE) -p evident-db-redpanda -f docker-compose.redpanda.yml stop

.PHONY: clean-redpanda
clean-redpanda:
	$(DOCKER_COMPOSE) -p evident-db-redpanda -f docker-compose.redpanda.yml down

.PHONY: redpanda-topics
redpanda-topics: start-redpanda
	-rpk topic create evidentdb-internal-commands -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c retention.ms="-1" --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic create evidentdb-internal-events -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c retention.ms="-1" --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic create evidentdb-databases -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic create evidentdb-database-names -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic create evidentdb-batches -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic create evidentdb-streams -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic create evidentdb-events -p 12 -r $(REPLICATION_FACTOR) -c compression.type=snappy -c cleanup.policy=compact --brokers $(KAFKA_BOOTSTRAP_SERVERS)

.PHONY: clean-redpanda-topics
clean-redpanda-topics:
	-rpk topic delete evidentdb-internal-commands --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic delete evidentdb-internal-events --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic delete evidentdb-databases --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic delete evidentdb-database-names --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic delete evidentdb-batches --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic delete evidentdb-streams --brokers $(KAFKA_BOOTSTRAP_SERVERS)
	-rpk topic delete evidentdb-events --brokers $(KAFKA_BOOTSTRAP_SERVERS)

# Testing and Performance

.PHONY: test
test:
	$(GRADLE) test
	cd perf/ && $(CARGO) test

.PHONY: run
run: $(CLUSTER_TYPE)-topics
	LOGGER_LEVELS_COM_EVIDENTDB=DEBUG $(GRADLE) run

.PHONY: perf
perf:
	cd perf/ && $(CARGO) run

# Clean up

.PHONY: clean
clean:
	$(GRADLE) clean
	cd perf/ && $(CARGO) clean

TRANSACTOR_APP_ID ?= evident-db-transactor

.PHONY: clean-topology-data
clean-topology-data:
	kafka-streams-application-reset --application-id $(TRANSACTOR_APP_ID) --bootstrap-servers $(KAFKA_BOOTSTRAP_SERVERS)
	rm -rf data/service/* data/transactor/*
	rm -rf app/data/service/* app/data/transactor/*

.PHONY: clean-all
clean-all: clean clean-$(CLUSTER_TYPE)-topics clean-topology-data clean-$(CLUSTER_TYPE)

# Util

.PHONY: loc
loc:
	tokei adapters app domain perf proto service transactor
