package com.springkafka.task.messages;

public class ResponseMsg {
	private String callId;
	private Long callStartTimestamp;
	private Long callEndTimestamp;
	private Long callDuration;

	public ResponseMsg(String callId, Long callStartTimestamp, Long callEndTimestamp) {
		this.callId = callId;
		this.callStartTimestamp = callStartTimestamp;
		this.callEndTimestamp = callEndTimestamp;

		this.callDuration = callEndTimestamp - callStartTimestamp;
	}

	public String getCallId() {
		return callId;
	}

	public Long getCallStartTimestamp() {
		return callStartTimestamp;
	}

	public Long getCallEndTimestamp() {
		return callEndTimestamp;
	}

	public Long getCallDuration() {
		return callDuration;
	}

}
