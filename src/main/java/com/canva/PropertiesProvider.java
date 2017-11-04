package com.canva;

import com.canva.sqs.SQSProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Alexander Pronin
 * @since 04/11/2017
 */
public class PropertiesProvider {

    private static Properties getProperties(String serviceName, String flavor) throws IOException {
        Properties properties = new Properties();
        InputStream input = SQSProvider.class.getClassLoader().getResourceAsStream(SERVICE_NAME + "." + flavor + ".properties");
        properties.load(input);
        return properties;
    }
}
