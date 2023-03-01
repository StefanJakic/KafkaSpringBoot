package com.springkafka.task;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.Transformers;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import com.springkafka.task.messages.EventMessage;
import com.springkafka.task.messages.ResponseMsg;

@Configuration
public class EventKafkaFlow {
	private static final String INPUT = "topicTask1";
	private static final String OUTPUT = "topicTask2";
	private static final String ERROR_CHANNEL_NAME = "exceptionChannel";

	private static Logger logger = LoggerFactory.getLogger(EventKafkaFlow.class);

	@Value("${msg_start_event}")
	private String msgStartEvent;
	@Value("${msg_end_event}")
	private String msgEndEvent;
	
//	probe
//	@Bean
//	public EventMessageHandlerWritingToDataBase2 eventMessageHandlerWritingToDataBase2() {
//		EventMessageHandlerWritingToDataBase2 e = new EventMessageHandlerWritingToDataBase2();
//		return e;
//	}
	
	
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
	

	@Bean
	public IntegrationFlow errorChannel() {
		return IntegrationFlow.from(MessageChannels.direct(ERROR_CHANNEL_NAME))
				.handle(p -> logger.error("errorChanell message invalid format {}", p)).get();
	}

	@Bean
	public IntegrationFlow fromKafkaFlow(ConsumerFactory<String, Object> consumerFactory) {
		return IntegrationFlow
				.from(Kafka.messageDrivenChannelAdapter(consumerFactory, INPUT).errorChannel(ERROR_CHANNEL_NAME))

				.transform(Transformers.fromJson(EventMessage.class))
				.filter(eventKafkaFilter())
				.handle(eventMessageHandler())
				.transform(Transformers.toJson())
				.channel(kafkaOutputChannell())
				.get();

	}

	@Bean
	public PublishSubscribeChannel kafkaOutputChannell() {
		return new PublishSubscribeChannel();
	}

	@Bean
	public IntegrationFlow toKafkaFlow(KafkaTemplate<String, HashMap<String, Object>> kafkaTemplate) {
		return IntegrationFlow.from(kafkaOutputChannell())
				.handle(Kafka.outboundChannelAdapter(kafkaTemplate).topic(OUTPUT))
				.get();
	}
	
	@Bean
	public IntegrationFlow toDBFlow(JdbcTemplate jdbcTemplate) {
		return IntegrationFlow.from(kafkaOutputChannell())
				.transform(Transformers.fromJson(ResponseMsg.class))
				.handle(databaseMessageHandler())
			//	.handle(eventMessageHandlerWritingToDataBase2())
				.get();
	}
	
}