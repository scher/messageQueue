package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageResult;

/**
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public interface Queue {
    String sendMessage(String messageBody);
    Message receiveMessage();
    void deleteMessage(String receiptHandle);

}
