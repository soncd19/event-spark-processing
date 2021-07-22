package com.vnpay.ott.spark.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import java.util.Properties;

/**
 * Created by SonCD on 31/03/2021
 */
public class PropertiesSingleton {

    private static PropertiesSingleton instance;
    private final Properties properties = new Properties();

    private PropertiesSingleton() throws IOException {
        buildProperties();
    }

    public static PropertiesSingleton getInstance() throws IOException {
        if (instance == null) {
            instance = new PropertiesSingleton();
        }
        return instance;
    }

    private void buildProperties() throws IOException {
        Properties envProps = new Properties();
        String configFile = System.getProperty("user.dir") + "/conf/process.properties";
        InputStreamReader isr = new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8);
        properties.load(isr);
        isr.close();
    }

    public Properties getProperties() {
        return properties;
    }
}
