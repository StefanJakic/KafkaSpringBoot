package com.springkafka.task;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.integration.support.MessageBuilder;

import com.springkafka.task.cache.EventMessageCache;
import com.springkafka.task.messages.EventMessage;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EventMessageHandlerTest {

	private static final Long TIMESTAMP_START = 1673864779966L;
	private static final Long TIMESTAMP_END = 1673864889966L;
	private static final Long TIMESTAMP_INCORRECT = 1673864779966L;

	private static final String CALL_ID = "testCallId";

	private static final String STATUS_START = "START";
	private static final String STATUS_END = "END";

	@Mock
	private EventMessage startEvent;
	@Mock
	private EventMessage endEvent;
	@Mock
	private EventMessage incorrectEvent;

	@InjectMocks
	private EventMessageHandler eventMessageHandler = new EventMessageHandler(STATUS_START, STATUS_END);

	@Spy
	private EventMessageCache eventMessageCache;

	@BeforeEach
	public void setUp() {
		initEvents();
	}

	@Test
	void testStartEventSaveToCache() {
		eventMessageHandler.handleMessage(MessageBuilder.withPayload(startEvent).build());
		verify(eventMessageCache).putEventMessage(startEvent);

	}

	@Test
	void testEndEventScheduleForDeletion() {
		eventMessageHandler.handleMessage(MessageBuilder.withPayload(endEvent).build());
		verify(eventMessageCache).scheduleMessageDelete(endEvent);
	}

//	@Test
//	void testThatCacheIsEmptyAfterHappyPath() {
//		eventMessageHandler.handleMessage(MessageBuilder.withPayload(startEvent).build());
//		eventMessageHandler.handleMessage(MessageBuilder.withPayload(endEvent).build());
//		assertEquals(null, eventMessageCache.getEventMessage(startEvent));
//		assertEquals(null, eventMessageCache.getEventMessage(endEvent));
//	}

	private void initEvents() {
		when(startEvent.getTimestamp()).thenReturn(TIMESTAMP_START);
		when(startEvent.getCallId()).thenReturn(CALL_ID);
		when(startEvent.getCallStatus()).thenReturn(STATUS_START);

		when(endEvent.getTimestamp()).thenReturn(TIMESTAMP_END);
		when(endEvent.getCallId()).thenReturn(CALL_ID);
		when(endEvent.getCallStatus()).thenReturn(STATUS_END);

		when(incorrectEvent.getTimestamp()).thenReturn(TIMESTAMP_INCORRECT);
		when(incorrectEvent.getCallId()).thenReturn("incorrect");
		when(incorrectEvent.getCallStatus()).thenReturn("incorrect");
	}
}
