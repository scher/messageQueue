package com.canva.sqs.local.memory;

import com.amazonaws.services.sqs.model.*;
import com.canva.sqs.QueueService;
import com.canva.sqs.common.CloseableLock;
import com.canva.sqs.local.Queue;
import com.canva.sqs.local.memory.InMemoryQueue;
import org.apache.http.annotation.ThreadSafe;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manges queues and wraps queue responses to SQS API Results;
 * Queue size is not limited
 * QueueUrl = QueueName
 */
@ThreadSafe
public class InMemoryQueueService implements QueueService {
    private final ConcurrentHashMap<String, Queue> queues = new ConcurrentHashMap<>();

    // creation/deletion of queue must use writeLock!
    // operations on messages must use readLock
    private final ReadWriteLock managementLock = new ReentrantReadWriteLock();

    private final Properties props;

    public InMemoryQueueService(Properties props) {
        System.out.println("Initializing In-memory queue service");
        this.props = props;
    }

    /**
     * @param queueUrl
     * @param messageBody
     * @return
     * @throws {@link QueueDoesNotExistException} if specified queue does not exist. This part is missing in AWS
     *         SQS Documentaion, so I decided to throw an execption as with {@link #getQueueUrl(String)} method
     */
    @SuppressWarnings("JavaDoc")
    @Override
    public SendMessageResult sendMessage(String queueUrl, String messageBody) {
        try (CloseableLock ignored = new CloseableLock(managementLock.readLock()).lock()) {
            return Optional.ofNullable(queues.get(queueUrl))
                    .map(queue -> queue.sendMessage(messageBody))
                    .map(new SendMessageResult()::withMessageId)
                    .orElseThrow(() -> new QueueDoesNotExistException("Queue: " +queueUrl + "does not exists"));
        }
    }

    /**
     * Retrieves just one message for simplicity
     *
     * @param queueUrl
     * @return
     */
    @Override
    public ReceiveMessageResult receiveMessage(String queueUrl) {
        try (CloseableLock ignored = new CloseableLock(managementLock.readLock()).lock()) {
            return Optional.ofNullable(queues.get(queueUrl))
                    .map(Queue::receiveMessage)
                    .map(new ReceiveMessageResult()::withMessages)
                    .orElse(new ReceiveMessageResult());
        }
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        try (CloseableLock ignored = new CloseableLock(managementLock.readLock()).lock()) {
            Optional.ofNullable(queues.get(queueUrl)).ifPresent(q -> q.deleteMessage(receiptHandle));
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
            queues.computeIfAbsent(queueName, q -> new InMemoryQueue(props));
        }
        return new CreateQueueResult().withQueueUrl(queueName);
    }

    @Override
    public void deleteQueue(String queueUrl) {
        try (CloseableLock ignored = new CloseableLock(managementLock.writeLock()).lock()) {
            Optional.ofNullable(queues.remove(queueUrl)).ifPresent(Queue::cleanup);
        }
    }

    @Override
    public ListQueuesResult listQueues() {
        return new ListQueuesResult().withQueueUrls(queues.keySet());
    }

    @Override
    public GetQueueUrlResult getQueueUrl(String queueName) {
        if (queues.containsKey(queueName)) {
            return new GetQueueUrlResult().withQueueUrl(queueName);
        } else {
            throw new QueueDoesNotExistException("Queue: " + queueName + " does not exist");
        }
    }
}
