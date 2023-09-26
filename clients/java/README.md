# EvidentDB JVM Client

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

### Java

``` java
import io.grpc.ManagedChannelBuilder;
import com.evidentdb.client.EvidentDB;
import com.evidentdb.client.java.Client;
import com.evidentdb.client.java.Connection;
import com.evidentdb.client.java.Database;
import com.evidentdb.client.Batch;
import com.evidentdb.client.CloseableIterator;

class EvidentDBDemo {
    public static void main(String[] args) {
        Client client = EvidentDB.javaClient(ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext());
        client.createDatabase("foo"); // => true

        Connection conn = client.connectDatabase("foo");
        Database db1 = conn.db();

        try (CloseableIterator iterator = db1.stream("my-stream")) {
            while(iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        } // => No print statements, empty stream

        Batch result = conn.transactBatch(
            Arrays.listOf(
                EvidentDB.eventProposal("event-type.occurred", "my-stream")
            )
        ).get(); // Await future

        Database db2 = conn.sync(batch.getRevision());

        try (CloseableIterator iterator = db1.stream("my-stream")) {
            while(iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        } // => No print statements, databases are immutable!

        try (CloseableIterator iterator = db2.stream("my-stream")) {
            while(iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        } // => One printed event in db2

        try (CloseableIterator iterator = conn.log()) {
            while(iterator.hasNext()) {
                Batch batch = iterator.next();
                System.out.println(batch);
                System.out.println(batch == result); // => true, since this batch is the same as the transaction result above
            }
        } // => Walk the log of batches, this prints the single batch we transacted above

        client.shutdown();
    }
}
```

### Kotlin

``` kotlin
import io.grpc.ManagedChannelBuilder
import com.evidentdb.client.EvidentDB

val client = EvidentDB.kotlinClient(ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext())****

runBlocking {
  client.createDatabase("foo") // => true
  val conn = client.connectDatabase("foo")
  val db1 = conn.db()

  db1.stream("my-stream").toList() // => []

  val batch = conn.transactBatch(
    listOf(EvidentDB.eventProposal("event-type.occurred", "my-stream"))
  )
  val db2 = conn.sync(batch.revision)

  db1.stream("my-stream").toList() // => []
  db2.stream("my-stream").toList() // => [CloudEvent(...)]

  conn.log().toList() // => [Batch(...)]
}

client.shutdown()
```
