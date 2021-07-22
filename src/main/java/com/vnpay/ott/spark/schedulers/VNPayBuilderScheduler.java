package com.vnpay.ott.spark.schedulers;

import com.github.kagkarlsson.scheduler.task.helper.RecurringTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.github.kagkarlsson.scheduler.task.schedule.Schedules;
import com.mongodb.spark.MongoSpark;
import com.vnpay.ott.spark.entity.VNPayJob;
import com.vnpay.ott.spark.entity.VNPayScheduler;
import com.vnpay.ott.spark.session.VNPaySparkSession;
import com.vnpay.ott.spark.utils.FormattedDateMatcher;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

/**
 * Created by SonCD on 13/05/2021
 */
public class VNPayBuilderScheduler {
    private static final Logger log = LoggerFactory.getLogger(VNPayBuilderScheduler.class);
    private final Properties properties;
    private final SparkSession sparkSession;

    public VNPayBuilderScheduler(VNPaySparkSession vnPaySparkSession) {
        this.sparkSession = vnPaySparkSession.getSparkSession();
        this.properties = vnPaySparkSession.getProperties();
    }

    public List<RecurringTask<Void>> buildTask(VNPayJob vnPayJob) throws Exception {

        List<RecurringTask<Void>> recurringTasks = new ArrayList<>();
        List<VNPayScheduler> vnPaySchedulers = vnPayJob.getSchedulers();

        String startDate = vnPayJob.getStartDate();
        String endDate = vnPayJob.getEndDate();

        String sparkHdfsUri = properties.getProperty("spark.hdfs.uri");
        String sparkHdfsPath = properties.getProperty("spark.hdfs.path");
        String sparkMongoUri = properties.getProperty("spark.mongo.uri");
        String sparkMongoDb = properties.getProperty("spark.mongo.database");

        FileSystem fs = FileSystem.get(new URI(sparkHdfsUri), sparkSession.sparkContext().hadoopConfiguration());

        vnPaySchedulers.forEach(vnPayScheduler -> {

            String type = vnPayScheduler.getType();

            Schedule schedule = null;
            String expression = vnPayScheduler.getExpression();
            String source = vnPayScheduler.getSource();
            String sql = vnPayScheduler.getSql();
            String collection = vnPayScheduler.getCollection();
            String saveMode = vnPayScheduler.getSaveMode();
            String preView = vnPayScheduler.getPreview();

            switch (type) {
                case "cron":
                    schedule = Schedules.cron(expression);
                    break;
                case "daily":
                    schedule = Schedules.daily(LocalTime.parse(expression));
                    break;
                case "fixed":

                default:
                    throw new IllegalStateException("VNPay: Unexpected value: " + type);
            }

            RecurringTask<Void> cronTask = Tasks.recurring(collection + "-task", schedule)
                    .execute((taskInstance, executionContext) -> {

                        log.info("VNPay: start scheduler job spark with source: " + source + ", output collections: " + collection);

                        long starting = FormattedDateMatcher.generateStartDate(startDate);
                        long finishing = FormattedDateMatcher.generateFinishDate(endDate);

                        for (long i = starting; i < finishing; i++) {

                            LocalDate date = LocalDate.now().minusDays(i);
                            String jsonHdfsPath = String.format("%s/%s/%s/%s", sparkHdfsUri, sparkHdfsPath, source, date.toString());
                            try {
                                boolean fileExists = fs.exists(new Path(String.format("/%s/%s/%s", sparkHdfsPath, source, date.toString())));
                                if (fileExists) {
                                    Dataset<Row> ds = sparkSession.read().json(jsonHdfsPath);

                                    ds.createOrReplaceTempView(preView);
                                    Dataset<Row> dataframe = sparkSession.sqlContext().sql(sql);

                                    dataframe.write().format("com.mongodb.spark.sql.DefaultSource")
                                            .option("uri", sparkMongoUri)
                                            .option("database", sparkMongoDb)
                                            .option("collection", collection)
                                            .mode(saveMode)
                                            .save();

                                } else {
                                    log.info("VNPay path hdfs: " + jsonHdfsPath + " is not exists");
                                }

                            } catch (IOException e) {
                                log.error("VNPay Read file on hdfs with spark error: " + e);
                            }

                        }
                    });

            recurringTasks.add(cronTask);
        });

        return recurringTasks;
    }
}
