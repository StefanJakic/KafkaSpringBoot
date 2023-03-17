package com.springkafka.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventKafkaFlow {

	private static Logger logger = LoggerFactory.getLogger(EventKafkaFlow.class);

	@Value("${msg_start_event}")
	private String msgStartEvent;
	@Value("${msg_end_event}")
	private String msgEndEvent;
	
	@Bean
	public EventKafkaFilter eventKafkaFilter() {
		EventKafkaFilter eventKafkaFilter = new EventKafkaFilter();
		return eventKafkaFilter;
	}
	
	@Bean
	public EventMessageHandler eventMessageHandler() {
		EventMessageHandler eventMessageHandler = new EventMessageHandler(msgStartEvent, msgEndEvent);
		return eventMessageHandler;
	}
	
	@Bean
	public DatabaseMessageHandler databaseMessageHandler() {
		DatabaseMessageHandler eventMessageHandlerWritingToDataBase = new DatabaseMessageHandler();
		return eventMessageHandlerWritingToDataBase;
	}

}