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

## Usage

``` kotlin
import com.evidentdb.client.EvidentDB

val client = EvidentDB(ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext())
client.createDatabase("foo") // => true
val conn = client.connect("foo")
val db1 = conn.db()
db1.stream("my-stream").get() // => []
val batch = conn.transactBatch(
  listOf(EvidentDB.eventProposal("event-type.occurred", "my-stream"))
).get()
val db2 = conn.db(batch.revision).get()
db1.stream("my-stream").get() // => []
db2.stream("my-stream").get() // => [CloudEvent(...)]
```