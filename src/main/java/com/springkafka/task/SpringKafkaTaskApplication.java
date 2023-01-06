package com.springkafka.task;

import org.apache.kafka.clients.admin.NewTopic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class SpringKafkaTaskApplication {

	private static final String TOPIC_ONE = "topicTask1";
	private static final String TOPIC_TWO = "topicTask2";

	public static Logger logger = LoggerFactory.getLogger(SpringKafkaTaskApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaTaskApplication.class, args); // .close();
	}

	@Bean
	public NewTopic topicTask1() {
		return TopicBuilder.name(TOPIC_ONE).partitions(3).replicas(2).compact().build();
	}

	@Bean
	public NewTopic topicTask2() {
		return TopicBuilder.name(TOPIC_TWO).partitions(3).replicas(2).compact().build();
	}

	@Autowired
	private KafkaEventMessageListener eventFilterResponseMessage;
}
