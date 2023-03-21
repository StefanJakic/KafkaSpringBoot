FROM openjdk:latest
LABEL maintainer="stefan.net"
ADD target/SpringKafkaTask-0.0.1-SNAPSHOT.jar spring-kafka.jar
COPY src/main/resources/application.properties /app/
ENTRYPOINT ["java", "-jar", "spring-kafka.jar"]