# EvidentDB

EvidentDB is an event store for use in event sourcing.  It is written in
Kotlin and built atop Apache Kafka.

## Status

EvidentDB is currently ALPHA quality software, and shouldn't be used
in production, and so we're not yet publishing builds to artifact
repositories. The API is mostly stable, but is still subject to
change before we begin publishing our BETA and stable releases.

## Building & Running the Server

EvidentDB uses Gradle as its build system, with some tasks
orchestrated in the top-level Makefile.

``` bash
make            # builds an application JAR and native-executable binaries
make run        # Runs local kafka cluster, creates topics, and runs application
make perf       # Runs the external perf/correctness tests via Rust application in perf/
make clean      # Cleans the build artifacts
make clean-all  # Cleans up all Kafka cluster and Streams state
```

## Building & installing the Java/Kotlin client JAR

To use the EvidentDB [JVM client](./client), you'll need to build it and install to your local Maven repo:

``` bash
make install-client
```

For more information on client usage, see the [JVM client
README](./client/README.md).

## License & Copyright

Per the [./LICENSE](./LICENSE) file, this project is licensed under
the [Apache Software License, version
2.0](https://www.apache.org/licenses/LICENSE-2.0.txt).

Per the first paragraph in the [./NOTICE](./NOTICE) file, this project is:

Copyright 2022 Evident Systems LLC
