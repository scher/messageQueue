package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.Message;

/**
 * @author Alexander Pronin
 * @since 06/11/2017
 */
public class MessageRecord {
    private static final String SEP = ":";

    private final String messageId;
    private final long inflightSince;
    private final String receiptHandle;
    private final String body;

    private MessageRecord(String messageId, String receiptHandle, String body) {
        this(messageId, System.currentTimeMillis(), receiptHandle, body);
    }

    private MessageRecord(String messageId, long inflightSince, String receiptHandle, String body) {
        this.body = body;
        this.messageId = messageId;
        this.receiptHandle = receiptHandle;
        this.inflightSince = inflightSince;
    }

    public static MessageRecord fromMessage(Message message) {
        return new MessageRecord(message.getMessageId(), message.getReceiptHandle(), message.getBody());
    }

    public static MessageRecord fromString(String messageRecordStr) {
        String[] splited = messageRecordStr.split(SEP);
        return new MessageRecord(splited[0], Long.valueOf(splited[1]), splited[2], splited[3]);
    }

    public static Message fromStringToMessage(String messageStr) {
        return fromString(messageStr).toMessage();

    }

    public static String fromMessageToString(Message message) {
        return fromMessage(message).toString();
    }

    @Override
    public String toString() {
        return messageId + SEP + inflightSince + SEP + receiptHandle + SEP + body;
    }

    public Message toMessage() {
        return new Message().withMessageId(messageId).withReceiptHandle(receiptHandle).withBody(body);
    }

    public long getInflightSince() {
        return inflightSince;
    }
}
