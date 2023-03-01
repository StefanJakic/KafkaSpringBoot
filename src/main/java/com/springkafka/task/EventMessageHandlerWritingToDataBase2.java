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
import org.springframework.stereotype.Component;

import com.springkafka.task.messages.ResponseMsg;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

public class EventMessageHandlerWritingToDataBase2 implements MessageHandler {

	private static final Integer SCHEDULE_TRY_AGAIN_WRITE_TIMEOUT = 5;

	private static final Integer CACHE_SIZE = 10;

	private static Logger logger = LoggerFactory.getLogger(EventMessageHandlerWritingToDataBase2.class);
	
	
	//methods that are thread safe is put()/take() do not use add/remove
	private static ArrayBlockingQueue<ResponseMsg> queueCache = new ArrayBlockingQueue<>(CACHE_SIZE);

	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

	@PostConstruct
	private void init() {
		CacheQueueMessagesForRetryingToSendToDataBase cleaner = new CacheQueueMessagesForRetryingToSendToDataBase();
		scheduler.scheduleAtFixedRate(cleaner, 0,
				SCHEDULE_TRY_AGAIN_WRITE_TIMEOUT, TimeUnit.SECONDS);

	}

	@PreDestroy
	private void destroy() {
		scheduler.shutdown();
	}

	//class is singleton
	private class CacheQueueMessagesForRetryingToSendToDataBase implements Runnable {
		
		
		
		@Override
		public void run() {
			//
			logger.info("CacheQueueMessagesForRetryingToSendToDataBase is: [EXECUTEDDDD33] ");
//			try {
//				queueCache.take();
//			} catch (InterruptedException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
			
			if (!queueCache.isEmpty()) {
				ResponseMsg msg = queueCache.poll();
				try {
					jdbcTemplateWithQueryForWritingToDataBase(msg);
				} catch (Exception e) {
					putResponseMessageToCacheQueue(msg);
					logger.error("FAILD TO AGAIN SEND TO DB", e.getMessage());
				}
			}
		}
	}

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private void jdbcTemplateWithQueryForWritingToDataBase(ResponseMsg message) {

		logger.info("TRYING INSERT INTO kafka_messages MESSAGEE@ --> ", message);

		jdbcTemplate.update(
				"INSERT INTO kafka_messages (callId, callStartTimestamp, callEndTimestamp, callDuration) "
						+ "VALUES (?, ?, ?, ?)",
				message.getCallId(), message.getCallStartTimestamp().toString(),
				message.getCallEndTimestamp().toString(), message.getCallDuration().toString());
	}

	public EventMessageHandlerWritingToDataBase2() {
	}

	public void putResponseMessageToCacheQueue(ResponseMsg message) {

		if (queueCache.size() >= CACHE_SIZE) {
			queueCache.poll();
		}
		try {
			logger.info("PUT MESSAGE TO Cache!!!!");
			queueCache.put(message);
		} catch (InterruptedException e) {
			logger.error("Fail to write message to cache");
		}
	}
	
	
	//TODO: there can be implementation of take
	public void cleanCacheResponseMessageFromQueue(ResponseMsg message) {
		if (!queueCache.isEmpty()) {
			queueCache.remove();
		}
	}

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		ResponseMsg msg = (ResponseMsg) message.getPayload();

		logger.info("Message from handleMessage FOR DATABASE! : {}", msg);

		tryToSendToDataBase(msg);

	}

	private void tryToSendToDataBase(ResponseMsg message) {

		if (queueCache.isEmpty()) {
			try {
				jdbcTemplateWithQueryForWritingToDataBase(message);
			} catch (Exception e) {
				putResponseMessageToCacheQueue(message);
				logger.error("WHAT ERROR WE GET " + e.getMessage());
			}
		} else {
			putResponseMessageToCacheQueue(message);
		}

	}

}
