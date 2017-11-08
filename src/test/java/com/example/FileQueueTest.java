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
import java.util.Comparator;
import java.util.Properties;

import static com.canva.sqs.local.filesystem.FileQueueService.SQS_QUEUES_DIR_KEY;
import static com.canva.sqs.local.memory.InMemoryQueue.INFLIGHT_TIMEOUT_SECONDS_KEY;

public class FileQueueTest extends TestCases {

    private Path tempDirectory;

    @Before
    public void init() throws IOException {
        Properties props = new Properties();
        String delay = String.valueOf(10);
        tempDirectory = Files.createTempDirectory("sqs_tests");

        props.setProperty(INFLIGHT_TIMEOUT_SECONDS_KEY, delay);
        props.setProperty(SQS_QUEUES_DIR_KEY, tempDirectory.toString());
        service = new FileQueueService(props);
        String queueName = "queueName";
        CreateQueueResult queue = service.createQueue(queueName);
        queueUrl = queue.getQueueUrl();
    }

    @After
    public void cleanup() {
        try {
            Files.walk(tempDirectory, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
