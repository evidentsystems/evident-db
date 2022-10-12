# EvidentDB Java Client

This client is based on gRPC, and supports both Android and non-Android Java
environments. As such, this client requires several
[peer dependencies](https://github.com/grpc/grpc-java) to be added to your application
build configuration.  For non-Android Gradle, try:

```groovy
implementation 'com.evidentdb:client:LATEST'
implementation 'io.grpc:grpc-netty-shaded:1.49.2'
implementation 'io.cloudevents:cloudevents-core:2.3.0'
```
