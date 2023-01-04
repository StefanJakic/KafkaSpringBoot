package com.springkafka.task;

public class TopicMsg {

	private String callStatus;
	private String callId;
	private Long timestamp;

	public String getCallStatus() {
		return callStatus;
	}

	public void setCallStatus(String callStatus) {
		this.callStatus = callStatus;
	}

	public String getCallId() {
		return callId;
	}

	public void setCallId(String callId) {
		this.callId = callId;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = Long.parseLong(timestamp);
	}

	@Override
	public String toString() {
		return "TopicMsg [callStatus=" + callStatus + ", callId=" + callId + ", timestamp=" + timestamp;
	}
}
