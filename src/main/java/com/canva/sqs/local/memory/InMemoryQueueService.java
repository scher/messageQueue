package com.canva.sqs.local.memory;

import com.amazonaws.services.sqs.model.*;
import com.canva.sqs.QueueService;
import com.canva.sqs.local.Queue;
import org.apache.http.annotation.ThreadSafe;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In Memory implementation of SQS
 * <p>
 * Manages queues and wraps queue responses to SQS API Results;
 * <p>
 * For some methods (According to AWS SQS Documentation) it is not clear how to react if queue does not exists.
 * I decided to do nothing (do not throw exception in this case)
 * <p>
 * QueueUrl = QueueName
 */
@ThreadSafe
public class InMemoryQueueService implements QueueService {
    private final ConcurrentHashMap<String, Queue> queues = new ConcurrentHashMap<>();
    private final Properties props;

    public InMemoryQueueService(Properties props) {
        System.out.println("Initializing In-memory queue service");
        this.props = props;
    }

    /**
     * Pushes message to queue and returns it's id.
     * Returns empty result if queueUrl does not exists.
     */
    @Override
    public SendMessageResult sendMessage(String queueUrl, String messageBody) {
        return Optional.ofNullable(queues.get(queueUrl))
                .map(queue -> queue.sendMessage(messageBody))
                .map(new SendMessageResult()::withMessageId)
                .orElse(new SendMessageResult());
    }

    /**
     * Retrieves just one message for simplicity
     * Returns empty result if queueUrl does not exists
     */
    @Override
    public ReceiveMessageResult receiveMessage(String queueUrl) {
        return Optional.ofNullable(queues.get(queueUrl))
                .flatMap(Queue::receiveMessage)
                .map(new ReceiveMessageResult()::withMessages)
                .orElse(new ReceiveMessageResult());
    }

    /**
     * Deletes message from queue
     * Does nothing if queue does not exists or if "inflight" timeout already expired
     */
    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        Optional.ofNullable(queues.get(queueUrl)).ifPresent(q -> q.deleteMessage(receiptHandle));
    }

    /**
     * Creates queue specified by it queueName parameter.
     * Does nothing if queue with such name already exists.
     */
    @Override
    public CreateQueueResult createQueue(String queueName) {
        queues.computeIfAbsent(queueName, q -> new InMemoryQueue(props));
        return new CreateQueueResult().withQueueUrl(queueName);
    }

    /**
     * Deletes queue even if there are message in this queue.
     * Let clients, currently working with this queue finish there work.
     * All subsequent requests to this queue by its name or url will achieve visibility of this action.
     *
     * @param queueUrl queueUrl
     */
    @Override
    public void deleteQueue(String queueUrl) {
        Optional.ofNullable(queues.remove(queueUrl)).ifPresent(Queue::cleanup);
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
            throw new QueueDoesNotExistException("Queue: " + queueName + " does not exist"); //according to AWS SQS
        }
    }
}
