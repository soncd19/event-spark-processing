package com.vnpay.ott.spark.helpers;

import com.vnpay.ott.spark.utils.PropertiesSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by SonCD on 13/05/2021
 */
public abstract class VNPayMainHelper {
    protected Logger log = LoggerFactory.getLogger(getClass());

    public abstract void run(DataSource ds, Properties properties);

    protected void runWithDatasource() throws IOException {
        run(VNPayHsqlDatasource.initDatabase(), PropertiesSingleton.getInstance().getProperties());
    }
}
