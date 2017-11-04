package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;
import com.canva.sqs.common.CloseableLock;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;

/**
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public class TransferTask implements Runnable {
    private final ConcurrentHashMap<String, Message> from;
    private final ConcurrentLinkedDeque<Message> to;
    private final String receiptHandle;
    private final Lock transferLock;

    public TransferTask(ConcurrentHashMap<String, Message> from,
                        ConcurrentLinkedDeque<Message> to, String receiptHandle,
                        Lock transferLock) {
        this.from = from;
        this.to = to;
        this.receiptHandle = receiptHandle;
        this.transferLock = transferLock;
    }


    @Override
    public void run() {
        try (CloseableLock ignored = new CloseableLock(transferLock).lock()) {
            Optional.ofNullable(from.remove(receiptHandle))
                    .ifPresent(message -> to.addFirst(message.withReceiptHandle(null)));
        }
    }
}
