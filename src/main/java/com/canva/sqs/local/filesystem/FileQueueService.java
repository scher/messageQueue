package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.*;
import com.canva.sqs.local.AbstractLocalQueue;
import com.canva.sqs.local.Queue;
import org.apache.http.annotation.ThreadSafe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.canva.sqs.local.filesystem.FileDescriptor.SEMAPHORE;
import static java.util.stream.Collectors.toList;

/**
 *
 * File based implementation of SQS
 * <p>
 * Manages queues and maps queue responses to SQS API Results;
 * This implementation is abstracted from messages management.
 * <p>
 * For some methods (According to AWS SQS Documentation) it is not clear how to react if queue does not exists.
 * I decided to do nothing (do not throw exception in this case)
 */
@ThreadSafe
public class FileQueueService extends AbstractLocalQueue {
    public static final String SQS_QUEUES_DIR_KEY = "sqs.queues.dir";

    private final String queuesBaseDirStr;
    private final Path queuesBaseDir;

    public FileQueueService(Properties props) {
        System.out.println("Initializing File System Queue Service");
        queuesBaseDirStr = props.getProperty(SQS_QUEUES_DIR_KEY);
        queuesBaseDir = Paths.get(queuesBaseDirStr);
        FileQueue.setProperties(props);
    }

    @Override
    public SendMessageResult sendMessage(String queueUrl, String messageBody) {
        SemaphoreFile.increaseCounter(SEMAPHORE.getPath(queueUrl).toString());
        try {
            return super.sendMessage(queueUrl, messageBody);
        } finally {
            SemaphoreFile.decreaseCounter(SEMAPHORE.getPath(queueUrl).toString());
        }
    }

    @Override
    public ReceiveMessageResult receiveMessage(String queueUrl) {
        SemaphoreFile.increaseCounter(SEMAPHORE.getPath(queueUrl).toString());
        try {
            return super.receiveMessage(queueUrl);
        } finally {
            SemaphoreFile.decreaseCounter(SEMAPHORE.getPath(queueUrl).toString());
        }
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        SemaphoreFile.increaseCounter(SEMAPHORE.getPath(queueUrl).toString());
        try {
            super.deleteMessage(queueUrl, receiptHandle);
        } finally {
            SemaphoreFile.decreaseCounter(SEMAPHORE.getPath(queueUrl).toString());
        }

    }

    @Override
    protected Queue getQueue(String ignored) {
        return FileQueue.getInstance();
    }

    /**
     * Creates queue specified by it queueName parameter.
     * Does nothing if queue with such name already exists.
     */
    @Override
    public CreateQueueResult createQueue(String queueName) {
        FileQueue.init(queueName);
        GetQueueUrlResult queueUrl = getQueueUrl(queueName);
        createQueueSemaphore(SEMAPHORE.getPath(queueUrl.getQueueUrl()).toString());
        return new CreateQueueResult().withQueueUrl(queueUrl.getQueueUrl());
    }

    /**
     * Deletes queue even if there are message in this queue.
     * <p>
     * Blocks if there are clients currently working with this queue.
     * Livelock is possible
     * <p>
     * All subsequent requests to this queue by its name or url will achieve visibility of this action.
     * Does nothing if queue does not exists
     *
     * @param queueUrl queueUrl
     */
    @Override
    public void deleteQueue(String queueUrl) {
        boolean isExecuted;
        do {
            isExecuted = SemaphoreFile.executeIfZero(queueUrl, () -> {
                getQueue(queueUrl).cleanup(queueUrl);
                try {
                    Files.delete(Paths.get(SEMAPHORE.getPath(queueUrl).toString()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } while (!isExecuted);
    }

    @Override
    public ListQueuesResult listQueues() {
        List<String> queues = new ArrayList<>();
        try (GlobalCloseableLock ignored = new GlobalCloseableLock(queuesBaseDirStr).lock()) {
            queues = Files.list(queuesBaseDir).map(Path::toString).collect(toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ListQueuesResult().withQueueUrls(queues);
    }

    @Override
    public GetQueueUrlResult getQueueUrl(String queueName) {
        try {
            return Files.list(queuesBaseDir)
                    .filter(p -> p.endsWith(queueName))
                    .findFirst()
                    .map(p -> new GetQueueUrlResult().withQueueUrl(p.toString()))
                    .orElseThrow(() -> new QueueDoesNotExistException("Queue: " + queueName + " does not exist"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new GetQueueUrlResult();
    }

    private void createQueueSemaphore(String queueSemaphore) {
        try {
            Files.createFile(Paths.get(queueSemaphore).toAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
