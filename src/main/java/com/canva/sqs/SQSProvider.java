package com.canva.sqs;


import java.io.FileInputStream;
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
public class SQSProvider {
    private static final String SERVICE_NAME = "sqs";
    private static final String FLAVOR_KEY = "flavor";
    private static final String SQS_IMPL_KEY = "sqs.impl";

    private static QueueService service;

    /**
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static synchronized QueueService getSQSService()
            throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (service == null) {
            String flavor = getFlavor();
            Properties properties = getProperties(flavor);
            String SQSImplClass = properties.getProperty(SQS_IMPL_KEY);
            service = (QueueService) Class.forName(SQSImplClass).newInstance();
        }
        return service;
    }



    private static String getFlavor() {
        return System.getenv(FLAVOR_KEY);
    }
}
