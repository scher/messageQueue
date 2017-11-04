package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;

/**
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public interface IdsGenerator {
    String generateMessageId();

    String generateRecipientHandlerId(Message message);
}
