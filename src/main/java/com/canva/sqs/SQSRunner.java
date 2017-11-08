package com.canva.sqs;


import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * All Dependency Injection is situated here.
 * For other components it will be transparent which implementation of SQS they are using
 *
 * @author Alexander Pronin
 * @since 02/11/2017
 */
public class SQSRunner {
    private static final String SERVICE_NAME = "sqs";
    private static final String FLAVOR_KEY = "flavor";
    private static final String SQS_IMPL_KEY = "sqs.impl";

    private QueueService service;
    private Properties properties;

    private static final SQSRunner INSTANCE = new SQSRunner();

    private SQSRunner() {
        try {
            String flavor = getFlavor();
            properties = initProperties(flavor);
            String SQSImplClass = properties.getProperty(SQS_IMPL_KEY);
            // let's make initialization more complex

            Class<?> c = Class.forName(SQSImplClass);
            Constructor<?> cons = c.getConstructor(Properties.class);
            service = (QueueService) cons.newInstance(properties);

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public QueueService getSQSService() {
        return service;
    }

    public static SQSRunner getInstance() {
        return INSTANCE;
    }

    private static Properties initProperties(String flavor) throws IOException {
        Properties properties = new Properties();
        InputStream input =
                SQSRunner.class.getClassLoader().getResourceAsStream(SERVICE_NAME + "." + flavor + ".properties");
        properties.load(input);
        return properties;
    }

    private static String getFlavor() {
        return System.getenv(FLAVOR_KEY);
    }

    public static void main(String[] args) {
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
