package com.canva.sqs;


import java.io.IOException;
import java.io.InputStream;
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
            service = (QueueService) Class.forName(SQSImplClass).newInstance();

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public QueueService getSQSService() {
        return service;
    }

    public static SQSRunner getInstance() {
        return INSTANCE;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
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
}
