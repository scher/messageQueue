package com.canva.sqs.local;

import com.amazonaws.services.sqs.model.Message;
import com.canva.sqs.QueueService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public class InMemoryQueue implements Queue {
    ConcurrentLinkedDeque<Message> messages = new ConcurrentLinkedDeque<>();
    ConcurrentHashMap<String, Message> inFlight = new ConcurrentHashMap<>();
    ScheduledExecutorService invalidator = Executors.newSingleThreadScheduledExecutor();

    @Override
    public String sendMessage(String messageBody) {
        return null;
    }

    @Override
    public Message receiveMessage() {
        return null;
    }

    @Override
    public void deleteMessage(String receiptHandle) {

    }
}
