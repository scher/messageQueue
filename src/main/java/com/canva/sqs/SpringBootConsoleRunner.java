package com.canva.sqs;

import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBootConsoleRunner implements CommandLineRunner {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(SpringBootConsoleRunner.class, args);
    }

    //access command line arguments
    @Override
    public void run(String... args) throws Exception {
        // some king of smoke test
        QueueService sqsService = SQSRunner.getInstance().getSQSService();

        String queueName = "queueName";
        CreateQueueResult queueResult = sqsService.createQueue(queueName);
        String queueUrl = queueResult.getQueueUrl();
        sqsService.sendMessage(queueUrl, "I'm alive Master!");
        ReceiveMessageResult receiveMessageResult = sqsService.receiveMessage(queueUrl);

        System.out.println(receiveMessageResult.getMessages().get(0));

    }
}
