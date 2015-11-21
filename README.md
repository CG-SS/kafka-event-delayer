# kafka-event-delayer

Consumes events from the given topics and delays them based on the configurations.

Currently, we require that a timestamp field is added to the event, as Kafka does not support custom header attributes yet.

## How it works

The concept is that, after we receive an event from Kafka, we check a field inside of that event that contains a timestamp which represents when that event is created. We then save the event in a permanent storage, if it contains the given timestamp.

After the event is considered expired, we delete it from the storage and send it back to Kafka.

## Running

### Maven

This project was made using Maven, so in order to build, just run:

```bash
mvn clean package
```

And you should have a Jar available under the `./target` directory.

```bash
java -jar ./target/kafka-event-delayer-1.0-SNAPSHOT.jar
```

### Docker

A Dockerfile is available under `./docker`, in order to build it:

```bash
docker build -f ./docker/Dockerfile -t cgss/kafka-event-delayer:0.0.1
```
