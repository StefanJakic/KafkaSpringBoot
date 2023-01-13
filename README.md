# Kafka Event Task

This is a Spring Boot application which consumes JSON messages from one Kafka Topic, process them, filters the invalid messages, and sends the messages that are valid to another Kafka Topic.

Spring Integration is using a message-driven channel adapter to receive messages from an external system, in our case from Kafka. Then, it is passing the messages to our custom filter, and then valid messages to our handler for processing.

The message from the inbound channel is in the following JSON format:

	{"callStatus": "START", "timestamp": "1668670990536" (some unix timestamp), "callId": "1232131"}

The message for the outbound channel is in the following JSON format:

	{"call_start_timestamp": "1668670990536", "call_end_timestamp": "1668670990538", "call_duration": 2, "callId": "1232131"}

Status value can be `START` or `END`.

The handler will check received messages in the following fashion:

- If the message is a `START` event then save it to the cash.
- If the message is an `END` event and the cash has the `START` event with the same `callId`, then calculate the call duration, and pass it to the outbound channel using the output format. The cash for this `callId` should be cleaned up after sending the message.
- If the message is an `END` event but doesn't have the matching `START` message, it should be marked for deletion after 5 minutes, and deleted after 10 minutes.
- If the message is an `END` event but the timestamp is less than the `START` event timestamp, and error should be logged and all messages with this `callId` should be deleted from the cash.

The cash is a custom class with two maps, one for caching `START` messages, and one for the messages that are marked for deletion.

After processing a message, the handler is passing messages to the outbound channel. The outbound channel adapter listens for messages from this channel and is passing them further to the output Kafka topic.

## Technologies

- Spring Boot
- Spring Integration
- Kafka
- Logback
- Maven