package com.springkafka.task;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.springkafka.task.cache.EventMessageCache;
import com.springkafka.task.messages.EventMessage;
import com.springkafka.task.messages.ResponseMsg;

public class EventMessageHandler{

	private static Logger logger = LoggerFactory.getLogger(EventMessageHandler.class);

	private String msgStartEvent;
	private String msgEndEvent;

	@Autowired
	private EventMessageCache eventMessageCache;

	public EventMessageHandler(String msgStartEvent, String msgEndEvent) {
		super();
		this.msgStartEvent = msgStartEvent;
		this.msgEndEvent = msgEndEvent;
	}

	public ResponseMsg handleMessage(EventMessage message) {
		EventMessage msg = message;
		logger.info("Message from handleMessage : {}", msg);

		// Main logic
		EventMessage previousMessage = eventMessageCache.getEventMessage(msg);

		if (previousMessage == null) {
			// Receive start and do not exist in cache with same id end/start .......
			eventMessageCache.putEventMessage(msg);
			if (msg.getCallStatus().equals(msgEndEvent)) {
				eventMessageCache.scheduleMessageDelete(msg);
			}
			logger.info("Received message: {}", msg);
			return null;
		} else {

			// If Receive message but message exist in cache with same status id
			if (previousMessage.getCallStatus().equalsIgnoreCase(msg.getCallStatus())) {
				eventMessageCache.putEventMessage(msg);

				logger.error("Receive duplicate event");
				return null;
			}

			Long startTimeTamp = null;
			Long endTimeTamp = null;

			if (previousMessage.getCallStatus().equalsIgnoreCase(msgStartEvent)) {
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
				return null;
			}

			// Receive end event, and exist start in cache then calculate duration HAPPY
			// PATH

			ResponseMsg responseMsg = new ResponseMsg(msg.getCallId(), startTimeTamp, endTimeTamp);
			
			//then we return this message to our main program, and send it to db, and topic to and after it clean cache

			logger.info("Sending response to topic2");

			logger.info("Topic2 Received ResponseMesg.json: " + responseMsg);

			eventMessageCache.cleanCacheForEventMessage(msg);
			
			
			return responseMsg;
			//sendMessage(MessageBuilder.withPayload(responseMsg).build());

		}

	}

}
