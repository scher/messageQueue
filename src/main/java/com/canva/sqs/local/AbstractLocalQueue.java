package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.canva.sqs.QueueService;

import java.util.Optional;

/**
 * QueueServices abstraction relevant both for File-based SQS and Inmemory SQS
 *
 * @author Alexander Pronin
 * @since 06/11/2017
 */
public abstract class AbstractLocalQueue implements QueueService {
    /**
     * Pushes message to queue and returns it's id.
     * Returns empty result if queueUrl does not exists.
     */
    @Override
    public SendMessageResult sendMessage(String queueUrl, String messageBody) {
        return Optional.ofNullable(getQueue(queueUrl))
                .map(queue -> queue.sendMessage(queueUrl, messageBody))
                .map(new SendMessageResult()::withMessageId)
                .orElse(new SendMessageResult());
    }

    /**
     * Retrieves just one message for simplicity
     * Returns empty result if queueUrl does not exists
     */
    @Override
    public ReceiveMessageResult receiveMessage(String queueUrl) {
        return Optional.ofNullable(getQueue(queueUrl))
                .flatMap(q -> q.receiveMessage(queueUrl))
                .map(m -> new Message()
                        .withMessageId(m.getMessageId())
                        .withBody(m.getBody())
                        .withReceiptHandle(m.getReceiptHandle()))
                .map(new ReceiveMessageResult()::withMessages)
                .orElse(new ReceiveMessageResult());
    }

    /**
     * Deletes message from queue
     * Does nothing if queue does not exists or if "inflight" timeout already expired
     */
    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        Optional.ofNullable(getQueue(queueUrl)).ifPresent(q -> q.deleteMessage(queueUrl, receiptHandle));
    }

    protected abstract Queue getQueue(String queueUrl);

    /**
     * TESTING purpose only!
     */
    public void invalidateNow(String queueUrl, String receiptHandler) {
        getQueue(queueUrl).invalidateNow(queueUrl, receiptHandler);
    }
}
