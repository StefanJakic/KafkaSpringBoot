package com.springkafka.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.core.GenericHandler;
import org.springframework.messaging.MessageHeaders;

import com.springkafka.task.cache.EventMessageCache;
import com.springkafka.task.messages.EventMessage;
import com.springkafka.task.messages.ResponseMsg;

public class EventMessageHandler implements GenericHandler<EventMessage> {

	private static Logger logger = LoggerFactory.getLogger(EventMessageHandler.class);

	private String msg_start_event;
	private String msg_end_event;

	@Autowired
	private EventMessageCache eventMessageCache;

	public EventMessageHandler(String msg_start_event, String msg_end_event) {
		this.msg_start_event = msg_start_event;
		this.msg_end_event = msg_end_event;
	}

	@Override
	public Object handle(EventMessage payload, MessageHeaders headers) {

		EventMessage msg = payload;
		logger.info("Message from handleMessage : {}", payload);

		// Main logic
		EventMessage previousMessage = eventMessageCache.getEventMessage(msg);

		if (previousMessage == null) {
			// Receive start and do not exist in cache with same id end/start .......
			eventMessageCache.putEventMessage(msg);
			if (msg.getCallStatus().equals(msg_end_event)) {
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

			if (previousMessage.getCallStatus().equalsIgnoreCase(msg_start_event)) {
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

			logger.info("Sending response to topic2");

			logger.info("Topic2 Received ResponseMesg.json: " + responseMsg);

			eventMessageCache.cleanCacheForEventMessage(msg);

			return responseMsg;
		}
	}
}
