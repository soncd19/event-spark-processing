package com.vnpay.ott.spark.schedulers;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.vnpay.ott.spark.entity.VNPayJob;
import com.vnpay.ott.spark.entity.mapper.VNPayJopMapper;
import com.vnpay.ott.spark.helpers.VNPayMainHelper;
import com.vnpay.ott.spark.helpers.VNPayThreadHelpers;
import com.vnpay.ott.spark.session.VNPaySparkSession;
import com.vnpay.ott.spark.utils.LoadDataFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Created by SonCD on 13/05/2021
 */
public class VNPayStartScheduler extends VNPayMainHelper {

    private static final Logger log = LoggerFactory.getLogger(VNPayStartScheduler.class);

    public static void main(String[] args) throws Exception {
        new VNPayStartScheduler().runWithDatasource();
    }

    @Override
    public void run(DataSource dataSource, Properties properties) {
        VNPaySparkSession vnPaySparkSession = null;
        try {

            String jobPath = System.getProperty("user.dir") + properties.getProperty("spark.path.job");
            String jobData = LoadDataFileUtils.fileToString(jobPath);

            log.info("VNPay: load spark job config path:" + jobPath);
            VNPayJob vnPayJob = VNPayJopMapper.mapper(jobData);
            vnPaySparkSession = new VNPaySparkSession(properties);
            VNPayBuilderScheduler builderScheduler = new VNPayBuilderScheduler(vnPaySparkSession);
            List<RecurringTask<Void>> recurringTasks = builderScheduler.buildTask(vnPayJob);

            final Scheduler scheduler = Scheduler
                    .create(dataSource)
                    .startTasks(recurringTasks)
                    .pollingInterval(Duration.ofSeconds(1))
                    .build();

            VNPayThreadHelpers.registerShutdownHook(scheduler);
            scheduler.start();

        } catch (Exception e) {
            log.error("VNPay start spark job processing error: " + e);
            if (vnPaySparkSession != null) {
                vnPaySparkSession.getSparkSession().close();
            }
        }
    }
}
