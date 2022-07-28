# EvidentDB

EvidentDB is an event store for use in event sourcing written in Kotlin and built atop Apache Kafka.

## Building

EvidentDB uses Gradle as its build system, with some tasks orchestrated in a Makefile.

``` bash
make            # builds application JAR
make dockerfile # builds app/build/docker/main/Dockerfile
```
