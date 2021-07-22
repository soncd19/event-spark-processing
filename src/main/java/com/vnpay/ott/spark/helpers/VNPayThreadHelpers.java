package com.vnpay.ott.spark.helpers;

import com.github.kagkarlsson.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by SonCD on 13/05/2021
 */
public class VNPayThreadHelpers {
    private static final Logger logger = LoggerFactory.getLogger(VNPayThreadHelpers.class);

    public static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void registerShutdownHook(Scheduler scheduler) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("VNPay: Received shutdown signal.");
            scheduler.stop();
        }));

    }
}
