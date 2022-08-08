# EvidentDB

EvidentDB is an event store for use in event sourcing written in Kotlin and built atop Apache Kafka.

## Prerequesites
$ sudo apt-get install autoconf automake libtool curl make g++ unzip

## Installation
From this page, download the protobuf-all-[VERSION].tar.gz.
Extract the contents and change in the directory
./configure
make
make check
sudo make install
sudo ldconfig # refresh shared library cache.

## Alternative Installation
http://google.github.io/proto-lens/installing-protoc.html

## Building & Running

EvidentDB uses Gradle as its build system, with some tasks orchestrated in a Makefile.

``` bash
make            # builds application JAR
make dockerfile # builds app/build/docker/main/Dockerfile
make run        # Runs local kafka cluster, creates topics, and runs application
make perf       # Runs the external perf/correctness tests via Rust application in perf/
make clean      # Cleans the build artifacts
make clean-all  # Cleans up all Kafka cluster and Streams state
```
