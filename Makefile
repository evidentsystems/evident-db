SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

GRADLE ?= ./gradlew
CARGO  ?= cargo
CLUSTER_TYPE ?= kafka# or redpanda
REPLICATION_FACTOR ?= 1
KAFKA_BOOTSTRAP_SERVERS ?= localhost:9092
PARTITION_COUNT ?= 4
COMPRESSION_TYPE ?= uncompressed# snappy
DOCKER_COMPOSE ?= docker compose
RPK ?= docker exec -it redpanda-0 rpk

# Load testing
GHZ ?= ghz --insecure --proto ./proto/service.proto
LOAD_TEST_GRPC_ENDPOINT ?= localhost:50051
LOAD_TEST_DB_COUNT ?= 20
LOAD_TEST_TRANSACT_BATCH_REQUEST := "{\
  \"database\":\"load-test-{{randomInt 0 $(LOAD_TEST_DB_COUNT)}}\",\
  \"events\":[\
    {\
      \"stream\": \"load-test-stream\",\
      \"stream_state\": 0,\
      \"event\": {\
        \"id\": \"will be overwritten\",\
        \"source\": \"https://localhost:8080/foo/bar\",\
        \"spec_version\": \"1.0\",\
        \"type\": \"demo.event\"\
      }\
    }\
  ]\
}"

LOAD_TEST_QUERY_REQUEST := "{\"name\":\"load-test-{{randomInt 0 $(LOAD_TEST_DB_COUNT)}}\"}"

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
	$(DOCKER_COMPOSE) -p evident-db-kafka -f docker-compose.kafka.yml up -d --remove-orphans

.PHONY: stop-kafka
stop-kafka:
	$(DOCKER_COMPOSE) -p evident-db-kafka -f docker-compose.kafka.yml stop

.PHONY: clean-kafka
clean-kafka:
	-$(DOCKER_COMPOSE) -p evident-db-kafka -f docker-compose.kafka.yml down

.PHONY: kafka-topics
kafka-topics:
	-kafka-topics --create --if-not-exists --topic evidentdb-internal-commands --partitions $(PARTITION_COUNT) --replication-factor $(REPLICATION_FACTOR) --config compression.type=$(COMPRESSION_TYPE) --config retention.ms="-1" --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --create --if-not-exists --topic evidentdb-internal-events --partitions $(PARTITION_COUNT) --replication-factor $(REPLICATION_FACTOR) --config compression.type=$(COMPRESSION_TYPE) --config retention.ms="-1" --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)

.PHONY: clean-kafka-topics
clean-kafka-topics:
	-kafka-topics --delete --if-exists --topic evidentdb-internal-commands --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-internal-events --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)

# Redpanda Cluster

.PHONY: start-redpanda
start-redpanda:
	$(DOCKER_COMPOSE) -p evident-db-redpanda -f docker-compose.redpanda.yml up -d
	$(RPK) cluster config set enable_transactions true

.PHONY: stop-redpanda
stop-redpanda:
	$(DOCKER_COMPOSE) -p evident-db-redpanda -f docker-compose.redpanda.yml stop

.PHONY: clean-redpanda
clean-redpanda:
	-$(DOCKER_COMPOSE) -p evident-db-redpanda -f docker-compose.redpanda.yml down

.PHONY: redpanda-topics
redpanda-topics: kafka-topics

.PHONY: clean-redpanda-topics
clean-redpanda-topics: clean-kafka-topics

# Testing and Performance

.PHONY: test
test:
	$(GRADLE) test
	cd perf/ && $(CARGO) test

.PHONY: run
run: start-$(CLUSTER_TYPE) $(CLUSTER_TYPE)-topics
	LOGGER_LEVELS_COM_EVIDENTDB=DEBUG $(GRADLE) run

.PHONY: perf
perf:
	-$(GHZ) --call com.evidentdb.EvidentDb/createDatabase -n $(LOAD_TEST_DB_COUNT) -d '{"name": "load-test-{{.RequestNumber}}"}' $(LOAD_TEST_GRPC_ENDPOINT) &>/dev/null
	-$(GHZ) --call com.evidentdb.EvidentDb/transactBatch -c $$(( $(LOAD_TEST_DB_COUNT) / 2 )) -n 1000 -d $(LOAD_TEST_TRANSACT_BATCH_REQUEST) $(LOAD_TEST_GRPC_ENDPOINT)
	-$(GHZ) --call com.evidentdb.EvidentDb/database -c $$(( $(LOAD_TEST_DB_COUNT) * 2 )) -n 9000 -d $(LOAD_TEST_QUERY_REQUEST) $(LOAD_TEST_GRPC_ENDPOINT)
	-$(GHZ) --call com.evidentdb.EvidentDb/deleteDatabase -n $(LOAD_TEST_DB_COUNT) -d '{"name": "load-test-{{.RequestNumber}}"}' $(LOAD_TEST_GRPC_ENDPOINT) &>/dev/null

# Clean up

.PHONY: clean
clean:
	$(GRADLE) clean
	cd perf/ && $(CARGO) clean

TRANSACTOR_APP_ID ?= evident-db-transactor

.PHONY: clean-topology-data
clean-topology-data:
	kafka-streams-application-reset --force --application-id $(TRANSACTOR_APP_ID) --bootstrap-servers $(KAFKA_BOOTSTRAP_SERVERS)
	rm -rf data/service/* data/transactor/*
	rm -rf app/data/service/* app/data/transactor/*

.PHONY: clean-all
clean-all: clean start-$(CLUSTER_TYPE) clean-kafka-topics clean-topology-data clean-$(CLUSTER_TYPE)

# Util

.PHONY: repl
repl:
	cd clojure-repl/ && ../$(GRADLE) clojureRepl

.PHONY: loc
loc:
	tokei adapters app domain perf proto service transactor
