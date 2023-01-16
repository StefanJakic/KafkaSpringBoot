package com.springkafka.task;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

import com.springkafka.task.cache.EventMessageCache;
import com.springkafka.task.messages.EventMessage;
import com.springkafka.task.messages.ResponseMsg;

import static org.junit.jupiter.api.Assertions.*;

@TestPropertySource(properties = { "msg_start_event=START", "msg_end_event=END" }) 
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
		//initEvents();
	}
	
	@Test
	void testHandleFirstTimeStartEventShouldWork() {
		// set up
		when(startEvent.getCallId()).thenReturn(CALL_ID);
		when(startEvent.getCallStatus()).thenReturn(STATUS_START);
		
		// execution
		Object obj = eventMessageHandler.handle(startEvent, null);
		
		// check and verify
		assertNull(obj);
		verify(startEvent, times(2)).getCallId();
		verify(startEvent).getCallStatus();
	}
	
	
	@Test
	void testStartEventSaveToCache() {
		eventMessageHandler.handle(startEvent, null);
		verify(eventMessageCache).putEventMessage(startEvent);
		
	}
	
//	@Test
//	void testEndEventScheduleForDeletion() {
//		eventMessageHandler.handle(endEvent, null);
//		verify(eventMessageCache).scheduleMessageDelete(endEvent);
//	}

	
/*
	@Test
	void testResponseMessageAfterStartAndEnd() {
		eventMessageHandler.handle(startEvent, null);
		ResponseMsg  responseMsg= (ResponseMsg) eventMessageHandler.handle(endEvent, null);
		verify(eventMessageCache).scheduleMessageDelete(endEvent);
		//verify clean cache
		//is resMsg is equal to start,end - start-end
		
	}
*/
	private void initEvents() {
		when(startEvent.getTimestamp()).thenReturn(TIMESTAMP_START);
		when(startEvent.getCallId()).thenReturn(CALL_ID);
		when(startEvent.getCallStatus()).thenReturn(STATUS_START);
		
		when(endEvent.getTimestamp()).thenReturn(TIMESTAMP_END);
		when(endEvent.getCallId()).thenReturn(CALL_ID);
		when(endEvent.getCallStatus()).thenReturn(STATUS_END);
		
		when(endEvent.getTimestamp()).thenReturn(TIMESTAMP_END);
		when(endEvent.getCallId()).thenReturn("incorrect");
		when(endEvent.getCallStatus()).thenReturn("incorrect");
	}

	// @Mock object you re mocking
	//
	// @InjectMocks object your re injecting mock into

	// when(mock.method().then)
	// assert...
	// verify
}
