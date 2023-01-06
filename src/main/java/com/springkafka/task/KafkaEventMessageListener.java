package com.springkafka.task;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafka.task.cache.EventMessageCache;
import com.springkafka.task.messages.EventMessage;
import com.springkafka.task.messages.ResponseMsg;

//@Scope("singleton")
@Component
public class KafkaEventMessageListener {

	private static final String TOPIC_ONE = "topicTask1";
	private static final String TOPIC_TWO = "topicTask2";
	private static final String MSG_START_EVENT = "START";
	private static final String MSG_END_EVENT = "END";
	
	private static Logger logger = LoggerFactory.getLogger(KafkaEventMessageListener.class);
	private ObjectMapper objectMapper = new ObjectMapper();

	@Autowired
	private KafkaTemplate<String, String> template;
	@Autowired
	private EventMessageCache eventMessageCache;

	public KafkaEventMessageListener() {
		logger.info("CREATED EventFilterResponseMessage");
	}

	

	@KafkaListener(topics = TOPIC_ONE)
	public void listen(ConsumerRecord<?, ?> cr) throws Exception {

		EventMessage msg = null;

		try {
			msg = objectMapper.readValue(cr.value().toString(), EventMessage.class);
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
		EventMessage previousMessage = eventMessageCache.getEventMessage(msg);

		if (previousMessage == null) {
			// Receive start and do not exist in cache with same id end/start .......
			eventMessageCache.putEventMessage(msg);
			if (msg.getCallStatus().equals(MSG_END_EVENT)) {
				eventMessageCache.scheduleMessageDelete(msg);
			}
			logger.info("Recived message: {}", msg);

		} else {

			// If Receive message but message exist in cache with same status id
			if (previousMessage.getCallStatus().equalsIgnoreCase(msg.getCallStatus())) {

				// if two messages in row Receive for example: start-id-300,start-id300 //
				// end-id-250, end-id-250 : TODO Then? DELETE ALL OF THEM!
				// if Received message is start, but already we have start message in cache
				logger.error("Recive duplicate event");
				eventMessageCache.cleanCacheForEventMessage(msg);
				return;
			}

			Long startTimeTamp = null;
			Long endTimeTamp = null;

			if (previousMessage.getCallStatus().equalsIgnoreCase(MSG_START_EVENT)) {
				startTimeTamp = previousMessage.getTimestamp();
				endTimeTamp = msg.getTimestamp();
			} else {
				startTimeTamp = msg.getTimestamp();
				endTimeTamp = previousMessage.getTimestamp();
			}

			// Here we check case end less than start
			if (startTimeTamp > endTimeTamp) {
				logger.error("Start timestamp > End timestamp");
				eventMessageCache.cleanCacheForEventMessage(msg);
				return;
			}

			// Receive end event, and exist start in cache then calculate duration HAPPY
			// PATH

			ResponseMsg responseMsg = new ResponseMsg(msg.getCallId(), startTimeTamp, endTimeTamp);

			String msgAsJSON = objectMapper.writeValueAsString(responseMsg);

			template.send(TOPIC_TWO, msgAsJSON);
			logger.info("Sending response to topic2");

			logger.info("Topic2 Recived ResponseMesg.json: " + msgAsJSON);

			eventMessageCache.cleanCacheForEventMessage(msg);
		}

	}
}
