package com.springkafka.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafka.task.cache.EventMessageCache;
import com.springkafka.task.messages.EventMessage;
import com.springkafka.task.messages.ResponseMsg;

public class EventMessageHandler implements MessageHandler {
	
	private static Logger logger = LoggerFactory.getLogger(EventMessageHandler.class);
	
	@Value("${MSG_START_EVENT}")
	private String MSG_START_EVENT;
	@Value("${MSG_END_EVENT}")
	private String MSG_END_EVENT;
	@Autowired
	IntegrationFlow toKafkaFlow;
	
	@Autowired
	private EventMessageCache eventMessageCache;
	
	private ObjectMapper objectMapper;

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {

		EventMessage msg = null;
		System.out.println("Message from handleMessage : " + message);
		
		String str = (String) message.getPayload();
		try {
			msg = objectMapper.readValue(str, EventMessage.class);
		} catch (Exception e) {
			logger.error("Error :", e);
		}
		//Main logic
		EventMessage previousMessage = eventMessageCache.getEventMessage(msg);

		if (previousMessage == null) {
			// Receive start and do not exist in cache with same id end/start .......
			eventMessageCache.putEventMessage(msg);
			if (msg.getCallStatus().equals(MSG_END_EVENT)) {
				eventMessageCache.scheduleMessageDelete(msg);
			}
			logger.info("Received message: {}", msg);

		} else {

			// If Receive message but message exist in cache with same status id
			if (previousMessage.getCallStatus().equalsIgnoreCase(msg.getCallStatus())) {
				// if Received message is start, but already we have start message in cache
				logger.error("Receive duplicate event");
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
			
			String msgAsJSON= "";
			try {
				msgAsJSON = objectMapper.writeValueAsString(responseMsg);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			//template.send(TOPIC_TWO, msgAsJSON);
			logger.info("Sending response to topic2");

			logger.info("Topic2 Received ResponseMesg.json: " + msgAsJSON);

			eventMessageCache.cleanCacheForEventMessage(msg);
			if(!msgAsJSON.isEmpty() || !msgAsJSON.isBlank()) {
				toKafkaFlow.getInputChannel().send(MessageBuilder.withPayload(msgAsJSON).build());
			} else {
				logger.error("Fail to send toKafkaFlow");
			}
		}
		
		
		
		
	}

}
