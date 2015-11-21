FROM maven:3.2.3-jdk-8 AS build

WORKDIR /app

COPY ./src/main ./src/main
COPY ./pom.xml ./pom.xml

RUN mvn clean package

FROM maven:3.2.3-jdk-8 as runner

WORKDIR /app
COPY --from=build /app/target/kafka-event-delayer-1.0-SNAPSHOT.jar .

CMD ["java", "-jar", "kafka-event-delayer-1.0-SNAPSHOT.jar"]
