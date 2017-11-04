package com.canva.sqs.local.memory;

import com.amazonaws.services.sqs.model.Message;
import com.canva.sqs.local.IdsGenerator;
import com.canva.sqs.local.Queue;
import com.canva.sqs.local.SimpleIdsGenerator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public class InMemoryQueue implements Queue {
    private static final String INFLIGHT_TIMEOUT_SECONDS_KEY = "sqs.inflight.timeout";
    private final Properties props;

    private final ConcurrentLinkedDeque<Message> messages = new ConcurrentLinkedDeque<>();
    private final ConcurrentHashMap<String, Message> inFlight = new ConcurrentHashMap<>();
    private final ScheduledExecutorService invalidator = Executors.newSingleThreadScheduledExecutor();

    private final IdsGenerator idsGenerator = new SimpleIdsGenerator();

    public InMemoryQueue(Properties props) {
        this.props = props;
    }

    @Override
    public @Nonnull String sendMessage(String messageBody) {
        String messageId = idsGenerator.generateMessageId();
        messages.add(new Message().withBody(messageBody).withMessageId(String.valueOf(messageId)));
        return messageId;
    }

    @Override
    public @Nullable Message receiveMessage() {
        Message message = messages.poll();
        if (message == null) {
            return null;
        } else {
            String receiptHandle = idsGenerator.generateRecipientHandlerId(message);
            message.withReceiptHandle(receiptHandle);

            invalidator.schedule(
                    () -> Optional.ofNullable(inFlight.remove(receiptHandle)).ifPresent(m -> messages.addFirst(m.withReceiptHandle(null))),
                    Long.valueOf(props.getProperty(INFLIGHT_TIMEOUT_SECONDS_KEY)), TimeUnit.SECONDS);

            return message;
        }
    }

    @Override
    public void deleteMessage(String receiptHandle) {
        inFlight.remove(receiptHandle);
    }

    @Override
    public void cleanup() {
        invalidator.shutdown();
    }
}
