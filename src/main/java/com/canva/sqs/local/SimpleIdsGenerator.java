package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;
import org.apache.http.annotation.ThreadSafe;

import java.util.concurrent.atomic.AtomicInteger;

/**
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
     * Ignores message content
     * @param message
     * @return
     */
    public String generateRecipientHandlerId(Message message) {
        return String.valueOf(recipientsHandleIds.getAndIncrement());
    }
}
