SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

GRADLE ?= ./gradlew
CARGO  ?= cargo
CLUSTER_TYPE ?= kafka# or redpanda
KAFKA_BOOTSTRAP_SERVERS ?= localhost:9092
PARTITION_COUNT ?= 4
REPLICATION_FACTOR ?= 1
COMPRESSION_TYPE ?= uncompressed# or snappy
DOCKER_COMPOSE ?= docker compose
RPK ?= docker exec -it redpanda-0 rpk

default: build

# Server

.PHONY: test-server
test-server:
	cd server && ../$(GRADLE) test

.PHONY: jar
jar:
	cd server && ../$(GRADLE) app:build

.PHONY: native
native:
	cd server && ../$(GRADLE) app:nativeCompile

.PHONY: build
build: jar native

# Client

.PHONY: install-java-client
install-java-client:
	cd clients && ../$(GRADLE) client:publishToMavenLocal

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
	cd server && ../$(GRADLE) run --args="-k $(KAFKA_BOOTSTRAP_SERVERS) bootstrap -p $(PARTITION_COUNT) -r $(REPLICATION_FACTOR) -c $(COMPRESSION_TYPE)"

.PHONY: clean-kafka-topics
clean-kafka-topics:
	-kafka-topics --delete --if-exists --topic evidentdb-default-tenant-internal-commands --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)
	-kafka-topics --delete --if-exists --topic evidentdb-default-tenant-internal-events   --bootstrap-server $(KAFKA_BOOTSTRAP_SERVERS)

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

GHZ ?= ghz --insecure --proto ./proto/service.proto
LOAD_TEST_GRPC_ENDPOINT ?= localhost:50051
LOAD_TEST_DB_COUNT ?= 20
LOAD_TEST_TRANSACT_BATCH_REQUEST := "{\
  \"database\":\"load-test-{{randomInt 0 $(LOAD_TEST_DB_COUNT)}}\",\
  \"events\":[\
    {\
      \"stream\": \"load-test-stream-{{randomInt 0 $(LOAD_TEST_DB_COUNT)}}\",\
      \"stream_state\": 0,\
      \"event\": {\
        \"id\": \"will be overwritten\",\
        \"source\": \"https://localhost:8080/foo/bar\",\
        \"spec_version\": \"1.0\",\
        \"type\": \"demo.event\",\
        \"attributes\": {\"subject\": {\"ce_string\": \"subject-{{randomInt 0 1000}}\"}}\
      }\
    }\
  ]\
}"

LOAD_TEST_QUERY_REQUEST := "{\"name\":\"load-test-{{randomInt 0 $(LOAD_TEST_DB_COUNT)}}\"}"

.PHONY: test
test: test-server

.PHONY: run
run: start-$(CLUSTER_TYPE) $(CLUSTER_TYPE)-topics
	cd server && LOGGER_LEVELS_COM_EVIDENTDB=DEBUG ../$(GRADLE) run --args="node -r $(REPLICATION_FACTOR) -c $(COMPRESSION_TYPE)"

.PHONY: perf
perf:
	-$(GHZ) --call com.evidentdb.EvidentDb/createDatabase -n $(LOAD_TEST_DB_COUNT) -d '{"name": "load-test-{{.RequestNumber}}"}' $(LOAD_TEST_GRPC_ENDPOINT) &>/dev/null
	-$(GHZ) --call com.evidentdb.EvidentDb/transactBatch -c $$(( $(LOAD_TEST_DB_COUNT) / 2 )) -n 1000 -d $(LOAD_TEST_TRANSACT_BATCH_REQUEST) $(LOAD_TEST_GRPC_ENDPOINT)
	-$(GHZ) --call com.evidentdb.EvidentDb/database -c $$(( $(LOAD_TEST_DB_COUNT) * 2 )) -n 9000 -d $(LOAD_TEST_QUERY_REQUEST) $(LOAD_TEST_GRPC_ENDPOINT)
	-$(GHZ) --call com.evidentdb.EvidentDb/deleteDatabase -n $(LOAD_TEST_DB_COUNT) -d '{"name": "load-test-{{.RequestNumber}}"}' $(LOAD_TEST_GRPC_ENDPOINT) &>/dev/null

# Clean up

.PHONY: clean
clean:
	cd clients && ../$(GRADLE) clean
	cd ../server && ../$(GRADLE) clean

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
	cd clients/clojure/ && ../../$(GRADLE) clojureRepl

.PHONY: loc
loc:
	tokei server clients interface
