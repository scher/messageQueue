package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;

/**
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public interface Queue {
    String sendMessage(String messageBody);
    Message receiveMessage();
    void deleteMessage(String receiptHandle);
    void cleanup();
}
