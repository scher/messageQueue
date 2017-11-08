package com.canva.sqs.local.filesystem;

import com.amazonaws.services.sqs.model.*;
import com.canva.sqs.local.AbstractLocalQueue;
import com.canva.sqs.local.Queue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.stream.Collectors.toList;

public class FileQueueService extends AbstractLocalQueue {
    public static final String SQS_QUEUES_DIR_KEY = "sqs.queues.dir";
    private static final String SEMAPHORE = "semaphore";

    private String queuesBaseDirStr;
    private Path queuesBaseDir;

    public FileQueueService(Properties props) {
        System.out.println("Initializing File System Queue Service");
        queuesBaseDirStr = props.getProperty(SQS_QUEUES_DIR_KEY);
        queuesBaseDir = Paths.get(queuesBaseDirStr);
        FileQueue.setProperties(props);
    }

    private String getQueueSemaphore(String queueUrl) {
        return Paths.get(queueUrl, SEMAPHORE).toString();
    }

    @Override
    public SendMessageResult sendMessage(String queueUrl, String messageBody) {
        SemaphoreFile.increaseCounter(getQueueSemaphore(queueUrl));
        try {
            return super.sendMessage(queueUrl, messageBody);
        } finally {
            SemaphoreFile.decreaseCounter(getQueueSemaphore(queueUrl));
        }
    }

    @Override
    public ReceiveMessageResult receiveMessage(String queueUrl) {
        SemaphoreFile.increaseCounter(getQueueSemaphore(queueUrl));
        try {
            return super.receiveMessage(queueUrl);
        } finally {
            SemaphoreFile.decreaseCounter(getQueueSemaphore(queueUrl));
        }
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        SemaphoreFile.increaseCounter(getQueueSemaphore(queueUrl));
        try {
            super.deleteMessage(queueUrl, receiptHandle);
        } finally {
            SemaphoreFile.decreaseCounter(getQueueSemaphore(queueUrl));
        }

    }

    @Override
    protected Queue getQueue(String ignored) {
        return FileQueue.getInstance();
    }

    @Override
    public CreateQueueResult createQueue(String queueName) {
        FileQueue.init(queueName);
        GetQueueUrlResult queueUrl = getQueueUrl(queueName);
        createQueueSemaphore(getQueueSemaphore(queueUrl.getQueueUrl()));
        return new CreateQueueResult().withQueueUrl(queueUrl.getQueueUrl());
    }

    private void createQueueSemaphore(String queueSemaphore) {
        try {
            Files.createFile(Paths.get(queueSemaphore).toAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteQueue(String queueUrl) {
        SemaphoreFile.executeIfZero(queueUrl, () -> {
            getQueue(queueUrl).cleanup(queueUrl);
            try {
                Files.delete(Paths.get(getQueueSemaphore(queueUrl)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
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
}
