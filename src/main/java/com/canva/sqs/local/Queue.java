package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Queue abstraction that handles logic connected to messages management.
 * <p>
 * I decided to abstract Queue from QueueService.
 * Let the service focus on queues management and response mapping
 *
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public interface Queue {
    @Nonnull
    String sendMessage(String queueUrl, String messageBody);

    Optional<Message> receiveMessage(String queueUrl);

    void deleteMessage(String queueUrl, String receiptHandle);

    void cleanup(String queueUrl);
}
