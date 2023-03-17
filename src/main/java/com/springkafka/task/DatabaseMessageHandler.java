package com.springkafka.task;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.MessagingException;

import com.springkafka.task.messages.ResponseMsg;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

public class DatabaseMessageHandler {

	private static final Integer SCHEDULE_TRY_AGAIN_WRITE_TIMEOUT = 5;

	private static final Integer CACHE_SIZE = 10;

	private static final long EXECUTOR_SHUTDOWN_TIMEOUT_MINUTES = 5;

	private static Logger logger = LoggerFactory.getLogger(DatabaseMessageHandler.class);

	private static ArrayBlockingQueue<ResponseMsg> queueCache = new ArrayBlockingQueue<>(CACHE_SIZE);

	private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@PostConstruct
	private void init() {
		scheduleSendingMessagesFromCache();
	}

	@PreDestroy
	private void destroy() {
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

	private void saveResponseMessageToDatabase(ResponseMsg message) {
		if (message == null) {
			return;
		}
		
		logger.info("TRYING INSERT INTO kafka_messages MESSAGE --> ", message);
		jdbcTemplate.update(
				"INSERT INTO messages_kafka (callId, callStartTimestamp, callEndTimestamp, callDuration) "
						+ "VALUES (?, ?, ?, ?)",
				message.getCallId(), message.getCallStartTimestamp(), message.getCallEndTimestamp(),
				message.getCallDuration());
	}

	public void putResponseMessageToCacheQueue(ResponseMsg message) {

		if (queueCache.size() >= CACHE_SIZE) {
			queueCache.poll();
		}
		try {
			logger.info("Put message to Cache!");
			queueCache.put(message);
		} catch (InterruptedException e) {
			logger.error("Fail to write message to cache!");
		}
	}

	public void handleMessage(ResponseMsg message) throws MessagingException {
		ResponseMsg msg = message;

		logger.info("Response message from handleMessage for database! : {}", msg);

		tryToSendToDatabase(msg);

	}

	private void tryToSendToDatabase(ResponseMsg message) {

		if (queueCache.isEmpty()) {
			try {
				saveResponseMessageToDatabase(message);
			} catch (Exception e) {
				putResponseMessageToCacheQueue(message);
				logger.error("Fail to send message to database " + e.getMessage());
			}
		} else {
			putResponseMessageToCacheQueue(message);
		}

	}

	private void scheduleSendingMessagesFromCache() {
		scheduler.scheduleAtFixedRate(() -> {
			if (!queueCache.isEmpty()) {
				while (true) {
					
					ResponseMsg msg = queueCache.peek();
					if (msg == null) {
						break;
					}
					try {
						saveResponseMessageToDatabase(msg);
						queueCache.remove();
					} catch (Exception e) {
						logger.error("FAILD TO AGAIN SEND TO DB", e);
						break;
					}
					//TODO:
					//If database is full then catch this exception and handle it
				}
			}
		}, 0, SCHEDULE_TRY_AGAIN_WRITE_TIMEOUT, TimeUnit.SECONDS);
	}

}
