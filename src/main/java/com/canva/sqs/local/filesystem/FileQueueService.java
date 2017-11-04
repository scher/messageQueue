package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.*;
import com.canva.sqs.QueueService;

public class FileQueueService implements QueueService {
    public FileQueueService() {
        System.out.println("Initializing File System Queue Service");
    }

    @Override
    public SendMessageResult sendMessage(String queueUrl, String messageBody) {
        return null;
    }

    @Override
    public ReceiveMessageResult receiveMessage(String queueUrl) {
        return null;
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {

    }

    @Override
    public CreateQueueResult createQueue(String queueName) {
        return null;
    }

    @Override
    public void deleteQueue(String queueUrl) {

    }

    @Override
    public ListQueuesResult listQueues() {
        return null;
    }

    @Override
    public GetQueueUrlResult getQueueUrl(String queueName) {
        return null;
    }
    //
  // Task 3: Implement me if you have time.
  //
}
