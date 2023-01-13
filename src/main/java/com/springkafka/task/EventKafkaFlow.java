package com.springkafka.task;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.GenericSelector;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Transformers;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.integration.kafka.dsl.Kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafka.task.messages.EventMessage;

@Configuration
@EnableIntegration
@ConfigurationProperties("kafka")
@EnableKafka
public class EventKafkaFlow {
	private static final String INPUT = "topicTask1";
	private static final String OUTPUT = "topicTask2";
	
	@Value("${MSG_START_EVENT}")
	private String MSG_START_EVENT;
	@Value("${MSG_END_EVENT}")
	private String MSG_END_EVENT;
	
	private static Logger logger = LoggerFactory.getLogger(EventKafkaFlow.class);
	
	private ObjectMapper objectMapper = new ObjectMapper();

	//private EventMessageHandler someHandler = new EventMessageHandler();
	
	@Bean
    public MyFilter myFilter(){
        MyFilter myFilter = new MyFilter();
        myFilter.setObjectMapper(objectMapper);
        return myFilter;
    }
	
	@Bean
	public EventMessageHandler someHandler() {
		EventMessageHandler eventMessageHandler = new EventMessageHandler();
		eventMessageHandler.setObjectMapper(objectMapper);
		return eventMessageHandler;
	}
	
	
	@Bean
	public IntegrationFlow fromKafkaFlow(ConsumerFactory<String, Object> consumerFactory) {
		return IntegrationFlow.from(Kafka.messageDrivenChannelAdapter(consumerFactory, INPUT))
				.filter(myFilter())
				.handle(someHandler())
				.get();
	}

	@Bean
	public PublishSubscribeChannel someOutputChannell() {
		return new PublishSubscribeChannel();
	}

	@Bean
	public IntegrationFlow toKafkaFlow(KafkaTemplate<String, HashMap<String, Object>> kafkaTemplate) {
		return IntegrationFlow.from(someOutputChannell())
				.handle(Kafka.outboundChannelAdapter(kafkaTemplate).topic(OUTPUT)).get();
	}

}
