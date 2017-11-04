package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.*;
import com.canva.sqs.QueueService;
import com.canva.sqs.common.CloseableLock;
import org.apache.http.annotation.ThreadSafe;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * Queue size is not limited
 * QueueUrl = QueueName
 *
 */
@ThreadSafe
public class InMemoryQueueService implements QueueService {
    private static final String INFLIGHT_TIMEOUT_SECONDS_KEY = "sqs.inflight.timeout";
    private ConcurrentHashMap<String, ConcurrentLinkedDeque<Message>> queues = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> inFlight = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ScheduledExecutorService> invalidators = new ConcurrentHashMap<>();

    // creation/deletion of queue must use write lock
    // operations on messages that use several collections must use readLock
    private ReadWriteLock managementLock = new ReentrantReadWriteLock();

    private IdsGenerator idsGenerator = new SimpleIdsGenerator();
    private Properties props;

    public InMemoryQueueService(Properties props) {
        System.out.println("Initializing In-memory queue service");
        this.props = props;
    }

    @Override
    public SendMessageResult sendMessage(String queueUrl, String messageBody) {
        try (CloseableLock ignored = new CloseableLock(managementLock.readLock()).lock()) {
            String messageId = idsGenerator.generateMessageId();
            queues.get(queueUrl).add(new Message().withBody(messageBody).withMessageId(String.valueOf(messageId)));
            return new SendMessageResult().withMessageId(messageId); // md5 is omitted for simplicity
        }
    }

    /**
     * Retrieves one message for simplicity
     *
     * @param queueUrl
     * @return
     */
    @Override
    public ReceiveMessageResult receiveMessage(String queueUrl) {
        try (CloseableLock ignored = new CloseableLock(managementLock.readLock()).lock()) {
            Message message = queues.get(queueUrl).poll();
            if (message == null) {
                return new ReceiveMessageResult();
            } else {
                String receiptHandle = idsGenerator.generateRecipientHandlerId(message);
                message.withReceiptHandle(receiptHandle);
                scheduleInvalidationTask(queueUrl, message);
                return new ReceiveMessageResult().withMessages(message);
            }
        }
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        try (CloseableLock ignored = new CloseableLock(managementLock.readLock()).lock()) {
            inFlight.get(queueUrl).remove(receiptHandle);
        }
    }

    /**
     * Creates queue specified by it queueName parameter.
     * Does nothing if queue with such name already exists.
     *
     * @param queueName
     * @return
     */
    @Override
    public CreateQueueResult createQueue(String queueName) {
        try (CloseableLock ignored = new CloseableLock(managementLock.writeLock()).lock()) {
            queues.computeIfAbsent(queueName, s -> new ConcurrentLinkedDeque<>());
            inFlight.computeIfAbsent(queueName, s -> new ConcurrentHashMap<>());
            invalidators.computeIfAbsent(queueName, s -> Executors.newSingleThreadScheduledExecutor());
        }
        return new CreateQueueResult().withQueueUrl(queueName);
    }

    @Override
    public void deleteQueue(String queueUrl) {
        try (CloseableLock ignored = new CloseableLock(managementLock.writeLock()).lock()) {
            queues.remove(queueUrl);
            inFlight.remove(queueUrl);
            Optional.ofNullable(invalidators.remove(queueUrl)).ifPresent(ScheduledExecutorService::shutdown);
        }
    }

    @Override
    public ListQueuesResult listQueues() {
        return new ListQueuesResult().withQueueUrls(queues.keySet());
    }

    @Override
    public GetQueueUrlResult getQueueUrl(String queueName) {
        return new GetQueueUrlResult().withQueueUrl(queueName);
    }

    private void scheduleInvalidationTask(String queueUrl, Message message) {
        try (CloseableLock ignored = new CloseableLock(managementLock.readLock()).lock()) {
            invalidators.get(queueUrl).schedule(
                    new TransferTask(inFlight.get(queueUrl), queues.get(queueUrl), message.getReceiptHandle(), managementLock.readLock()),
                    Long.valueOf(props.getProperty(INFLIGHT_TIMEOUT_SECONDS_KEY)), TimeUnit.SECONDS);
        }
    }

}
