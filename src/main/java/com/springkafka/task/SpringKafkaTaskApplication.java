package com.springkafka.task;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafka.task.messages.EventMessage;
import com.springkafka.task.messages.ResponseMsg;

@SpringBootApplication
public class SpringKafkaTaskApplication {

	private static final String TOPIC_ONE = "topicTask1";

	private static final String TOPIC_TWO = "topicTask2";

	@Autowired
	private KafkaTemplate<String, String> template;

	@Autowired
	EventKafkaFilter eventKafkaFilter;

	@Autowired
	EventMessageHandler eventMessageHandler;

	@Autowired
	DatabaseMessageHandler databaseMessageHandler;

	public static Logger logger = LoggerFactory.getLogger(SpringKafkaTaskApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaTaskApplication.class, args);
	}

	// override default configuration and create topic if not existing with two
	// partition
	@Bean
	public NewTopic topicTask1() {
		return TopicBuilder.name(TOPIC_ONE).partitions(2).build();
	}

	// it use default configuration and create topic if not existing with default
	// number of partition(in our case one)
	@Bean
	public NewTopic topicTask2() {
		return TopicBuilder.name(TOPIC_TWO).build();
	}

	
	// when create java application or script, we send message with key, because we
	// want to force to messages with same call id go to same partition
	// this.template.send(TOPIC_TWO, "key" ,msgForTopic2); for example middle
	// element is for key, when we send a message with duration we use without
	// key(anyway we have just one partiton)
	// key can be
	@KafkaListener(topics = TOPIC_ONE)
	public void listen(ConsumerRecord<String, String> cr) throws Exception {
		logger.info(cr.toString());

		// TODO:
		// because we will have more instances of the application, should we reject
		// messages with the key null in the application itself? it can be another
		// docker image
		/*
		 * if(cr.key()==null) { logger.error("Message need to have key!) return; }
		 */

		String msgForTopic2 = cr.value().toString();

		EventMessage msg = null;
		ObjectMapper mapper = new ObjectMapper();
		ResponseMsg responseMsg = new ResponseMsg();

		try {
			msg = mapper.readValue(msgForTopic2, EventMessage.class); // if message is json format, then it will return
																		// EventMessage object, otherwise it still be
																		// null
		} catch (Exception e) {
			logger.error("Message need be in JSON format! ", e.getMessage());
			return;
		}

		// if json message have have valid inputs, then it will return EventMessage
		// object, otherwise it will return null, for example: timestamp: -50, or call
		// status is not start or end then it will return null, if we pass method null
		// as parameter it will return null
		msg = eventKafkaFilter.accept(msg);

		if (msg == null) {
			return;
		}
		responseMsg = eventMessageHandler.handleMessage(msg);

		if (responseMsg == null) {
			return;
		}
		
		databaseMessageHandler.handleMessage(responseMsg);

		sendMessageToTopicTwo(responseMsg.toString());

	}

	private void sendMessageToTopicTwo(String msg) {
		this.template.send(TOPIC_TWO, msg);
	}
   
	/*
	@KafkaListener(topics = TOPIC_TWO)
	public void listen2(ConsumerRecord<String, String> cr) throws Exception {
		logger.info("Messages from topic 2 ");
		logger.info(cr.value());
	}
	*/

}
