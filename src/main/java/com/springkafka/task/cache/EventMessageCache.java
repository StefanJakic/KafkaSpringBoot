package com.springkafka.task.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.springkafka.task.messages.EventMessage;

@Component
public class EventMessageCache {

	private static final Integer SCHEDULE_DELETE_TIMEOUT = 5;
	private static final Integer DELETE_AFTER_SCHEDULE_TIMEOUT = 10;

	private static Logger logger = LoggerFactory.getLogger(EventMessageCache.class);

	private Map<String, EventMessage> cache = new ConcurrentHashMap<>();

	private Map<String, EventMessage> messagesForDeleteCache = new ConcurrentHashMap<>();

	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public void putEventMessage(EventMessage message) {
		cache.put(message.getCallId(), message);
	}

	public EventMessage getEventMessage(EventMessage message) {
		return cache.get(message.getCallId());
	}

	public void cleanCacheForEventMessage(EventMessage message) {
		String callId = message.getCallId();
		cache.remove(callId);
		messagesForDeleteCache.remove(callId);
		logger.info("Clean cache for callId {}", callId);
	}

	public void scheduleMessageDelete(EventMessage message) {
		logger.info("scheduleMessageDelete is called");
		scheduler.schedule(() -> {

			// TODO I should check if message was handled already and clean it
			messagesForDeleteCache.put(message.getCallId(), message);
			logger.info("Message schedule for delete: {}", message);

			scheduler.schedule(() -> {
				logger.info("Deleting message via schedule");

				cleanCacheForEventMessage(message);

			}, DELETE_AFTER_SCHEDULE_TIMEOUT, TimeUnit.MINUTES);
		}, SCHEDULE_DELETE_TIMEOUT, TimeUnit.MINUTES);
	}

}