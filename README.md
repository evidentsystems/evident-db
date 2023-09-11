# EvidentDB

EvidentDB is an event store for use in event sourcing written in
Kotlin and built atop Apache Kafka.

## Building & Running

EvidentDB uses Gradle as its build system, with some tasks
orchestrated in the top-level Makefile.

``` bash
make            # builds application JAR
make dockerfile # builds app/build/docker/main/Dockerfile
make run        # Runs local kafka cluster, creates topics, and runs application
make perf       # Runs the external perf/correctness tests via Rust application in perf/
make clean      # Cleans the build artifacts
make clean-all  # Cleans up all Kafka cluster and Streams state
```

## License & Copyright

Per the [./LICENSE](./LICENSE) file, this project is licensed under
the [Apache Software License, version
2.0](https://www.apache.org/licenses/LICENSE-2.0.txt).

Per the first paragraph in the [./NOTICE](./NOTICE) file, this project is:

Copyright 2022 Evident Systems LLC
