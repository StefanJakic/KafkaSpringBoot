package com.springkafka.task;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import com.springkafka.task.messages.ResponseMsg;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

public class DatabaseMessageHandler implements MessageHandler {

	private static final Integer SCHEDULE_TRY_AGAIN_WRITE_TIMEOUT = 5;

	private static final Integer CACHE_SIZE = 10;

	private static Logger logger = LoggerFactory.getLogger(DatabaseMessageHandler.class);

	private static ArrayBlockingQueue<ResponseMsg> queueCache = new ArrayBlockingQueue<>(CACHE_SIZE);

	private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(); //newScheduledThreadPool(11);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@PostConstruct
	private void init() {
		scheduler.scheduleAtFixedRate(new CacheQueueMessagesForRetryingToSendToDatabase(), 0,
				SCHEDULE_TRY_AGAIN_WRITE_TIMEOUT, TimeUnit.SECONDS);

	}

	@PreDestroy
	private void destroy() {
		scheduler.shutdown();
	}

	private void saveResponseMessageToDatabase(ResponseMsg message) {

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

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		ResponseMsg msg = (ResponseMsg) message.getPayload();

		logger.info("Message from handleMessage FOR DATABASE! : {}", msg);

		tryToSendToDatabase(msg);

	}

	private void tryToSendToDatabase(ResponseMsg message) {

		if (queueCache.isEmpty()) {
			try {
				saveResponseMessageToDatabase(message);
			} catch (Exception e) {
				putResponseMessageToCacheQueue(message);
				logger.error("WHAT ERROR WE GET " + e.getMessage());
			}
		} else {
			putResponseMessageToCacheQueue(message);
		}

	}

	private class CacheQueueMessagesForRetryingToSendToDatabase implements Runnable {
		@Override
		public void run() {
			logger.error("USAO U RUN");
			if (!queueCache.isEmpty()) {
			//	while (true) {
					logger.error("WHILE1");

					ResponseMsg msg = queueCache.peek();
					try {
						saveResponseMessageToDatabase(msg);
						queueCache.remove();
					} catch (Exception e) {
						// putResponseMessageToCacheQueue(msg);
						logger.error("FAILD TO AGAIN SEND TO DB", e.getMessage());
				//		break;
					}
			//	}
			}

		}
	}
}
