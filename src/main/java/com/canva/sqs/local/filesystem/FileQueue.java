package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.Message;
import com.canva.sqs.local.IdsGenerator;
import com.canva.sqs.local.Queue;
import com.canva.sqs.local.SimpleIdsGenerator;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Queue size is not limited
 *
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public class FileQueue implements Queue {
    private static final String INFLIGHT_TIMEOUT_SECONDS_KEY = "sqs.inflight.timeout";
    private final Properties props;

    private final ConcurrentLinkedDeque<Message> messages = new ConcurrentLinkedDeque<>();
    // receiptHandle -> Message
    private final ConcurrentHashMap<String, Message> inFlight = new ConcurrentHashMap<>();
    private final ScheduledExecutorService invalidator = Executors.newSingleThreadScheduledExecutor();

    private final IdsGenerator idsGenerator = new SimpleIdsGenerator();

    FileQueue(Properties props) {
        this.props = props;
    }

    @Override
    @Nonnull
    public String sendMessage(String messageBody) {
        String messageId = idsGenerator.generateMessageId();
        messages.add(new Message().withBody(messageBody).withMessageId(String.valueOf(messageId)));
        return messageId;
    }

    @Override
    public Optional<Message> receiveMessage() {
        return Optional.ofNullable(messages.poll()).map(message -> {
            String receiptHandle = idsGenerator.generateRecipientHandlerId(message);
            message.withReceiptHandle(receiptHandle);

            // move message back to messages queue and clear receiptHandle
            // does nothing if message was deleted by recipient
            invalidator.schedule(
                    () -> Optional.ofNullable(inFlight.remove(receiptHandle))
                            .ifPresent(m -> messages.addFirst(m.withReceiptHandle(null))),
                    Long.valueOf(props.getProperty(INFLIGHT_TIMEOUT_SECONDS_KEY)),
                    TimeUnit.SECONDS);
            return message;
        });
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
