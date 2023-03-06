CREATE DATABASE Kafka;

CREATE TABLE messages_kafka (
  id SERIAL PRIMARY KEY,
  callid VARCHAR(255) NOT NULL,
  callstarttimestamp BIGINT NOT NULL,
  callendtimestamp BIGINT NOT NULL,
  callduration BIGINT NOT NULL
);