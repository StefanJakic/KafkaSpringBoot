package com.springkafka.task;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

@SpringBootApplication
public class SpringKafkaTaskApplication {

	private static final String TOPIC_ONE = "topicTask1";
	private static final String TOPIC_TWO = "topicTask2";
	private static final String MSG_START_EVENT = "START";
	private static final String MSG_END_EVENT = "END";
	private static final Integer SCHEDULE_DELETE_TIMEOUT = 5;
	private static final Integer DELETE_AFTER_SCHEDULE_TIMEOUT = 10;

	// private static final InputStream TopicMsg = null;
	public static Logger logger = LoggerFactory.getLogger(SpringKafkaTaskApplication.class);
	private static Map<String, TopicMsg> cache = new HashMap<>();

	private static Map<String, TopicMsg> messagesForDeleteCache = new HashMap<>();

	private static ObjectMapper objectMapper = new ObjectMapper();
	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

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
	private KafkaTemplate<String, String> template;

	@KafkaListener(topics = TOPIC_TWO)
	public void listen2(ConsumerRecord<?, ?> cr) throws Exception {
		logger.info("RECIVE MESSAGE ON TOPIC2: {}", cr.value().toString());
	}

	@KafkaListener(topics = TOPIC_ONE)
	public void listen(ConsumerRecord<?, ?> cr) throws Exception {

		TopicMsg msg = null;

		try {
			msg = objectMapper.readValue(cr.value().toString(), TopicMsg.class);
		} catch (Exception e) {
			logger.error("Invalid message format!", e);
		}
		if (msg == null || msg.getTimestamp() == null || msg.getTimestamp() < 0
				|| !(msg.getCallStatus().equalsIgnoreCase(MSG_START_EVENT)
						|| msg.getCallStatus().equalsIgnoreCase(MSG_END_EVENT))
				|| msg.getCallId() == null || msg.getCallId().isBlank()) {

			System.out.println("ERORR: Invalid message inputs!");
			return;

		}

		TopicMsg previousMessage = cache.get(msg.getCallId());

		if (previousMessage == null) {
			// Receive start and do not exist in cache with same id end/start .......
			cache.put(msg.getCallId(), msg);
			if (msg.getCallStatus().equals(MSG_END_EVENT)) {
				scheduleMessageDelete(msg);
			}
			logger.info("Recived message: {}", msg);

		} else {

			// If Receive message but message exist in cache with same status id
			if (previousMessage.getCallStatus().equalsIgnoreCase(msg.getCallStatus())) {

				// if two messages in row Receive for example: start-id-300,start-id300 //
				// end-id-250, end-id-250 : TODO Then? DELETE ALL OF THEM!
				// if Received message is start, but already we have start message in cache
				logger.error("Recive duplicate event");
				cleanCacheForCallId(msg.getCallId());
				return;
			}

			Long startTimeTamp = null;
			Long endTimeTamp = null;

			if (previousMessage.getCallStatus().equalsIgnoreCase("START")) {
				startTimeTamp = previousMessage.getTimestamp();
				endTimeTamp = msg.getTimestamp();
			} else {
				startTimeTamp = msg.getTimestamp();
				endTimeTamp = previousMessage.getTimestamp();
			}

			// Here we check case end less than start
			if (startTimeTamp > endTimeTamp) {
				logger.error("End timestamp < Start timestamp");
				cleanCacheForCallId(msg.getCallId());
				return;
			}

			// Receive end event, and exist start in cache then calculate duration HAPPY
			// PATH

			ResponseMsg responseMsg = new ResponseMsg(msg.getCallId(), startTimeTamp, endTimeTamp);

			String msgAsJSON = objectMapper.writeValueAsString(responseMsg);

			template.send(TOPIC_TWO, msgAsJSON);
			logger.info("JSON:  -------->" + msgAsJSON);
			logger.info("Sending response to topic2");
			cleanCacheForCallId(msg.getCallId());
		}

	}

	private void cleanCacheForCallId(String callId) {
		cache.remove(callId);
		messagesForDeleteCache.remove(callId);
		logger.info("Clean cache for callId {}", callId);

	}

	private void scheduleMessageDelete(TopicMsg message) {
		logger.info("scheduleMessageDelete is called");
		scheduler.schedule(() -> {
			messagesForDeleteCache.put(message.getCallId(), message);
			logger.info("Message schedule for delete: {}", message);

			scheduler.schedule(() -> {
				logger.info("Deleting message via schedule");

				cleanCacheForCallId(message.getCallId());
			}, DELETE_AFTER_SCHEDULE_TIMEOUT, TimeUnit.MINUTES);
		}, SCHEDULE_DELETE_TIMEOUT, TimeUnit.MINUTES);
	}
}
