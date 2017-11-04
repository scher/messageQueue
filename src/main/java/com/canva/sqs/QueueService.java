package com.canva.sqs;

import com.amazonaws.services.sqs.model.*;

public interface QueueService {

    //
    // Task 1: Define me.
    //
    // This interface should include the following methods.  You should choose appropriate
    // signatures for these methods that prioritise simplicity of implementation for the range of
    // intended implementations (in-memory, file, and SQS).  You may include additional methods if
    // you choose.
    //
    // - push
    //   pushes a message onto a queue.
    SendMessageResult sendMessage(String queueUrl, String messageBody);

    // - pull
    //   retrieves a single message from a queue.
    ReceiveMessageResult receiveMessage(String queueUrl);

    // - delete
    //   deletes a message from the queue that was received by pull().
    void deleteMessage(String queueUrl, String receiptHandle);

    // Queue management section
    CreateQueueResult createQueue(String queueName);
    void deleteQueue(String queueUrl);
    ListQueuesResult listQueues();
    GetQueueUrlResult getQueueUrl(String queueName);

}
