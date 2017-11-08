package com.example;

import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.canva.sqs.local.filesystem.FileQueueService;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Properties;

import static com.canva.sqs.local.filesystem.FileQueueService.SQS_QUEUES_DIR_KEY;
import static com.canva.sqs.local.memory.InMemoryQueue.INFLIGHT_TIMEOUT_SECONDS_KEY;

public class FileQueueTest extends TestCases {

    @Before
    public void init() {
        Properties props = new Properties();
        String delay = String.valueOf(10);
        props.setProperty(INFLIGHT_TIMEOUT_SECONDS_KEY, delay);
        props.setProperty(SQS_QUEUES_DIR_KEY, "/tmp/sqs");
        try {
            Files.createDirectory(Paths.get(props.getProperty(SQS_QUEUES_DIR_KEY)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        service = new FileQueueService(props);
        String queueName = "queueName";
        CreateQueueResult queue = service.createQueue(queueName);
        queueUrl = queue.getQueueUrl();
    }

    @After
    public void cleanup() {
        try {
            Path rootPath = Paths.get("/tmp/sqs");
            Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
//                    .peek(System.out::println)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
