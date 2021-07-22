package com.vnpay.ott.spark.helpers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;
/**
 * Created by SonCD on 13/05/2021
 */
public class VNPayHsqlDatasource {
    public static DataSource initDatabase() {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:schedule_testing");
        config.setUsername("sa");
        config.setPassword("");
        config.addDataSourceProperty("driver", "");
        HikariDataSource dataSource  = new HikariDataSource(config);

        doWithConnection(dataSource, c -> {
            try (Statement statement = c.createStatement()) {
                String createTables = readFile("/hsql_tables.sql");
                statement.execute(createTables);
            } catch (Exception e) {
                throw new RuntimeException("VNPay: Failed to create tables", e);
            }
        });

        return dataSource;
    }

    private static String readFile(String resource) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(VNPayHsqlDatasource.class.getResourceAsStream(resource)))) {
            String line;
            StringBuilder createTables = new StringBuilder();
            while ((line = in.readLine()) != null) {
                createTables.append(line);
            }
            return createTables.toString();
        }
    }

    private static void doWithConnection(DataSource dataSource, Consumer<Connection> consumer) {
        try (Connection connection = dataSource.getConnection()) {
            consumer.accept(connection);
        } catch (SQLException e) {
            throw new RuntimeException("VNPay: Error getting connection from datasource.", e);
        }
    }
}
