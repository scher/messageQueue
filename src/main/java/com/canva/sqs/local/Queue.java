package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public interface Queue {
    @Nonnull
    String sendMessage(String messageBody);

    Optional<Message> receiveMessage();

    void deleteMessage(String receiptHandle);

    void cleanup();
}
