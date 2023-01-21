package com.springkafka.task;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import com.springkafka.task.cache.EventMessageCache;
import com.springkafka.task.messages.EventMessage;
import com.springkafka.task.messages.ResponseMsg;

public class EventMessageHandler extends MessageProducerSupport implements MessageHandler {

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

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		EventMessage msg = (EventMessage) message.getPayload();
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
			return;
		} else {

			// If Receive message but message exist in cache with same status id
			if (previousMessage.getCallStatus().equalsIgnoreCase(msg.getCallStatus())) {
				eventMessageCache.putEventMessage(msg);

				logger.error("Receive duplicate event");
				return;
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
				return;
			}

			// Receive end event, and exist start in cache then calculate duration HAPPY
			// PATH

			ResponseMsg responseMsg = new ResponseMsg(msg.getCallId(), startTimeTamp, endTimeTamp);

			logger.info("Sending response to topic2");

			logger.info("Topic2 Received ResponseMesg.json: " + responseMsg);

			eventMessageCache.cleanCacheForEventMessage(msg);

			sendMessage(MessageBuilder.withPayload(responseMsg).build());

		}

	}

}
