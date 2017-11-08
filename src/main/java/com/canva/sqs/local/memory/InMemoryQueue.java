package com.canva.sqs.local.memory;

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
public class InMemoryQueue implements Queue {
    public static final String INFLIGHT_TIMEOUT_SECONDS_KEY = "sqs.inflight.timeout";
    private final Properties props;

    private final ConcurrentLinkedDeque<Message> messages = new ConcurrentLinkedDeque<>();
    // receiptHandle -> Message
    private final ConcurrentHashMap<String, Message> inFlight = new ConcurrentHashMap<>();
    private final ScheduledExecutorService invalidator = Executors.newSingleThreadScheduledExecutor();

    private final IdsGenerator idsGenerator = new SimpleIdsGenerator();

    InMemoryQueue(Properties props) {
        this.props = props;
    }

    @Override
    @Nonnull
    public String sendMessage(String ignored, String messageBody) {
        String messageId = idsGenerator.generateMessageId();
        messages.add(new Message().withBody(messageBody).withMessageId(String.valueOf(messageId)));
        return messageId;
    }

    @Override
    public Optional<Message> receiveMessage(String queueUrl) {
        return Optional.ofNullable(messages.poll()).map(message -> {
            String receiptHandle = idsGenerator.generateRecipientHandlerId(message);
            message.withReceiptHandle(receiptHandle);

            inFlight.put(receiptHandle, message);
            // move message back to messages queue and clear receiptHandle
            // does nothing if message was deleted by recipient
            invalidator.schedule(
                    new InvalidationTask(receiptHandle),
                    Long.valueOf(props.getProperty(INFLIGHT_TIMEOUT_SECONDS_KEY)),
                    TimeUnit.SECONDS);
            return message;
        });
    }

    /**
     * TESTING purpose only!
     *
     * @param queueUrl
     * @param receiptHandle message to invalidate inflight
     */
    public void invalidateNow(String queueUrl, String receiptHandle) {
        new InvalidationTask(receiptHandle).run();
    }

    @Override
    public void deleteMessage(String ignored, String receiptHandle) {
        inFlight.remove(receiptHandle);
    }

    @Override
    public void cleanup(String ignored) {
        invalidator.shutdown();
    }

    // pulled out to inner class for testing purpose
    private class InvalidationTask implements Runnable {

        private String receiptHandle;

        private InvalidationTask(String receiptHandle) {
            this.receiptHandle = receiptHandle;
        }

        @Override
        public void run() {
            Optional.ofNullable(inFlight.remove(receiptHandle))
                    .ifPresent(m -> messages.addFirst(m.withReceiptHandle(null)));
        }
    }
}
