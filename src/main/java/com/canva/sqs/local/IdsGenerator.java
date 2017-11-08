package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;

/**
 * Common interface for message id and receipt handler generation
 *
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public interface IdsGenerator {
    /**
     * Generates message id
     *
     * @return unique message id
     */
    String generateMessageId();


    /**
     * Generate receipt handler based on message passed as input parameter.
     * @param message message content to generate receipt handler for
     * @return receipt handler based on message
     */
    String generateRecipientHandlerId(Message message);
}
