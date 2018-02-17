package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple by efficient ids generator based on atomics.
 * Think it is enough at least for in-memory solution.
 *
 * @author Alexander Pronin
 * @since 04/11/2017
 */
@ThreadSafe
public class SimpleIdsGenerator implements IdsGenerator {

    private static final AtomicInteger messageIds= new AtomicInteger();
    private static final AtomicInteger recipientsHandleIds = new AtomicInteger();

    public String generateMessageId() {
        return String.valueOf(messageIds.getAndIncrement());
    }

    /**
     * Probably recipientHandler should be calculated based on message content somehow,
     * but something more tricky than just taking MD5.
     * Let's ignore it for the purpose of simplicity
     */
    public String generateRecipientHandlerId(Message message) {
        return String.valueOf(recipientsHandleIds.getAndIncrement());
    }
}
