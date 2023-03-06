FROM openjdk:latest
LABEL maintainer="stefan.net"
ADD target/SpringKafkaTask-0.0.1-SNAPSHOT.jar spring-kafka.jar
ENTRYPOINT ["java", "-jar", "spring-kafka.jar"]