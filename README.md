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
make            # builds an application uber-JAR and native-executable binaries
make run        # Runs local kafka cluster, creates topics, and runs application
make perf       # Runs a quick and dirty perf test
make clean      # Cleans the build artifacts
make clean-all  # Cleans up all Kafka cluster and Streams state
```

## Building & installing the Java/Kotlin client JAR

To use the EvidentDB [JVM client](./clients/jvm), you'll need to build it and install to your local Maven repo:

``` bash
make install-client
```

Then include the dependency in your build config.

Gradle:

``` kotlin
implementation("com.evidentdb:client:0.1.0-alpha-SNAPSHOT")
```

Maven:

``` xml
<dependency>
    <groupId>com.evidentdb</groupId>
    <artifactId>client</artifactId>
    <version>0.1.0-alpha-SNAPSHOT</version>
</dependency>
```

For more information on client usage, see the [JVM client
README](./clients/jvm/README.md).

## License & Copyright

Per the [./LICENSE](./LICENSE) file, this project is licensed under
the [Apache Software License, version
2.0](https://www.apache.org/licenses/LICENSE-2.0.txt).

Per the first paragraph in the [./NOTICE](./NOTICE) file, this project is:

Copyright 2022 Evident Systems LLC
