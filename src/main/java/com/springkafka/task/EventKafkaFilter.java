package com.springkafka.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.core.GenericSelector;

import com.springkafka.task.messages.EventMessage;

public class EventKafkaFilter implements GenericSelector<EventMessage> {

	public static Logger logger = LoggerFactory.getLogger(EventKafkaFilter.class);

	@Value("${msg_start_event}")
	private String msg_start_event;
	@Value("${msg_end_event}")
	private String msg_end_event;

	@Override
	public boolean accept(EventMessage source) {
		EventMessage msg = source;
		logger.info("FILTER !");

		if (msg == null || msg.getTimestamp() == null || msg.getCallId() == null) {
			logger.error("Invalid message inputs!");
			return false;
		}

		if (!(msg.getCallStatus().equalsIgnoreCase(msg_start_event)
				|| msg.getCallStatus().equalsIgnoreCase(msg_end_event))) {
			logger.error("Invalid message inputs!");
			return false;
		}

		if (msg.getTimestamp() < 0 || msg.getCallId().isBlank()) {
			logger.error("Invalid message inputs!");
			return false;
		}

		return true;
	}
}
