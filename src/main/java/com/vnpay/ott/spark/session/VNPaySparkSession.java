package com.vnpay.ott.spark.session;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by SonCD on 13/05/2021
 */
public class VNPaySparkSession {
    private static final Logger log = LoggerFactory.getLogger(VNPaySparkSession.class);
    private final Properties properties;
    private final SparkSession sparkSession;

    public VNPaySparkSession(Properties properties) {
        this.properties = properties;
        this.sparkSession = buildSparkSession();
        log.info("VNPay: create spark session success");
    }

    private SparkSession buildSparkSession() {
        String master = properties.getProperty("spark.master");
        String appName = properties.getProperty("spark.app.name");
        return SparkSession.builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public Properties getProperties() {
        return properties;
    }
}
