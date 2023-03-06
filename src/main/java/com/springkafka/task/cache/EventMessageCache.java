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

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Component
public class EventMessageCache {

	private static final Integer SCHEDULE_DELETE_TIMEOUT = 1;//5;
	private static final Integer DELETE_AFTER_SCHEDULE_TIMEOUT = 2;//10;
	private static final long EXECUTOR_SHUTDOWN_TIMEOUT_MINUTES = 5;

	private static Logger logger = LoggerFactory.getLogger(EventMessageCache.class);

	private Map<String, EventMessage> cache = new ConcurrentHashMap<>();

	private ScheduledExecutorService scheduler = null;

	@PostConstruct
	private void init() {
		scheduler = Executors.newScheduledThreadPool(3);

	}

	@PreDestroy
	public void destroy() {
		scheduler.shutdown();
		try {
			if (!scheduler.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
				scheduler.shutdownNow();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			scheduler.shutdownNow();
		}
	}

	public void putEventMessage(EventMessage message) {
		cache.put(message.getCallId(), message);
	}

	public EventMessage getEventMessage(EventMessage message) {
		return cache.get(message.getCallId());
	}

	public boolean cleanCacheForEventMessage(EventMessage message) {
		String callId = message.getCallId();
		if(null != cache.remove(callId)) {
			logger.info("Clean cache for callId {}", callId);
			return true;
		}
		return false;
	}


	public void scheduleMessageDelete(EventMessage message) {
		logger.info("scheduleMessageDelete is called");
		scheduler.schedule(() -> {
			
			if(cache.get(message.getCallId()) != null) {
				logger.info("Message schedule for delete: {}", message);
				deleteAfterTimeout(message);
			}
			
		}, SCHEDULE_DELETE_TIMEOUT, TimeUnit.MINUTES);
	}

	private void deleteAfterTimeout(EventMessage message) {
		scheduler.schedule(() -> {
			
			
			if(cleanCacheForEventMessage(message) == true) {
				logger.info("Message is deleted via schedule");
			}
			
			
		}, DELETE_AFTER_SCHEDULE_TIMEOUT, TimeUnit.MINUTES);
	}

}
