package com.canva.sqs.prod;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import com.canva.sqs.QueueService;

public class SqsQueueService implements QueueService {
    private final AmazonSQSClient sqsClient;

    //
    // Task 4: Optionally implement parts of me.
    //
    // This file is a placeholder for an AWS-backed implementation of QueueService.  It is included
    // primarily so you can quickly assess your choices for method signatures in QueueService in
    // terms of how well they map to the implementation intended for a production environment.
    //
    public SqsQueueService() {
        System.out.println("I'm alive Master SqsQueueService");
        this.sqsClient = new AmazonSQSClient();
    }

    public SendMessageResult sendMessage(String queueUrl, String messageBody) {
        return sqsClient.sendMessage(queueUrl, messageBody);
    }

    public ReceiveMessageResult receiveMessage(String queueUrl) {
        return sqsClient.receiveMessage(queueUrl);
    }

    public void deleteMessage(String queueUrl, String receiptHandle) {
        sqsClient.deleteMessage(queueUrl, receiptHandle);
    }

    @Override
    public CreateQueueResult createQueue(String queueName) {
        return sqsClient.createQueue(queueName);
    }

    @Override
    public void deleteQueue(String queueUrl) {
        sqsClient.deleteQueue(queueUrl);
    }

    @Override
    public ListQueuesResult listQueues() {
        return sqsClient.listQueues();
    }

    @Override
    public GetQueueUrlResult getQueueUrl(String queueName) {
        return sqsClient.getQueueUrl(queueName);
    }

    @Override
    public void invalidateNow(String queueUrl, String receiptHandle) {

    }
}
