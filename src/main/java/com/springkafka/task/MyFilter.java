package com.springkafka.task;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.core.GenericSelector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafka.task.messages.EventMessage;




public class MyFilter implements GenericSelector<String>{

	@Value("${MSG_START_EVENT}")
	private String MSG_START_EVENT;
	@Value("${MSG_END_EVENT}")
	private String MSG_END_EVENT;
	
	private ObjectMapper objectMapper;// = new ObjectMapper();
	
	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public boolean accept(String source) {
		EventMessage msg = null;
		System.out.println("FILTER !");
		try {
			msg = objectMapper.readValue(source, EventMessage.class);
		} catch (Exception e) {
			//logger.error("Invalid message format!!");
			return false;
		}
		System.out.println("OKEJ JE STEFANE");
		
		if (msg == null || msg.getTimestamp() == null || msg.getTimestamp() < 0
				|| !(msg.getCallStatus().equalsIgnoreCase(MSG_START_EVENT)
						|| msg.getCallStatus().equalsIgnoreCase(MSG_END_EVENT)) 
				|| msg.getCallId() == null || msg.getCallId().isBlank()) {

			System.out.println("ERORR: Invalid message inputs!");
			return false;

		}
		return true;
	}
}
