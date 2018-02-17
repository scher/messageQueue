package com.canva.sqs;


import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * All Dependency Injection is situated here.
 * For other components it will be transparent which implementation of SQS they are using
 *
 * Supported flavors: local, memory, prod.
 * Set env. variable: flavor=memory
 *
 * Take a look at attached Intellij Idea Run Configuration
 *
 * @author Alexander Pronin
 * @since 02/11/2017
 */
public class SQSRunner {
    private static final String SERVICE_NAME = "sqs";
    private static final String FLAVOR_KEY = "flavor";
    private static final String SQS_IMPL_KEY = "sqs.impl";

    private QueueService service;

    private static final SQSRunner INSTANCE = new SQSRunner();

    private SQSRunner() {
        try {
            String flavor = getFlavor();
            Properties properties = initProperties(flavor);
            String SQSImplClass = properties.getProperty(SQS_IMPL_KEY);

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
}
