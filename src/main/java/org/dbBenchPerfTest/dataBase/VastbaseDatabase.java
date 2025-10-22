package org.dbBenchPerfTest.dataBase;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.DbManager;
import org.testController.JavaProcessExecutor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class VastbaseDatabase implements DatabaseInface {

    private static final Logger logger = LoggerFactory.getLogger(VastbaseDatabase.class);

    private final TestConfig config;

    public VastbaseDatabase(TestConfig config) {
        this.config = config;
    }


    @Override
    public void createPartitionTable() {
        logger.info("[预处理] VASTBASE 数据库开始创建分区表...");

        String partTableName = config.getPartTableName();

        String  createTableSql = "CREATE TABLE IF NOT EXISTS " + partTableName + " ("
                + "begintime text," + "usernum text," + "imei text," + "calltype text," + "netid text," + "lai text," +
                "ci text," + "imsi text," + "start_time text," + "end_time text," + "longitude text," + "latitude text," +
                "lacci text," + "timespan text," + "extra_longitude text," + "extra_latitude text," + "geospan text," +
                "anchorhash text," + "extra_geohash text," + "bd text," + "ad text," + "user_id text," + "address text," +
                "car_id text," + "mac text," + "mobile_mode text," + "usernum1 text," + "area text," + "ipv4 text," +
                "ipv6 text," + "mission_id text," + "bankcard_id text"
                + ") ";

        StringBuilder partitionSql = new StringBuilder(createTableSql);
        partitionSql.append("PARTITION BY RANGE (begintime) (");

        LocalDate current = config.getPartStartDate();
        while (current.isBefore(config.getPartEndDate())) {
            String partitionName = partTableName + "_p" + current.format(DateTimeFormatter.BASIC_ISO_DATE);
            String valueLessThan = current.plusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            partitionSql.append("PARTITION ").append(partitionName)
                    .append(" VALUES LESS THAN (TO_DATE('").append(valueLessThan).append("','YYYYMMDD')),");
            current = current.plusDays(1);
        }
        partitionSql.setLength(partitionSql.length() - 1);
        partitionSql.append(")");
        logger.info("VASTBASE 数据库开始执行创建分区表："+partTableName);
        try(Connection connec = DbManager.getConnection(config.getDbType());
            Statement stmt = connec.createStatement()) {
            stmt.execute(partitionSql.toString());
        } catch (SQLException e) {
            logger.error("VASTBASE 数据库创建分区表失败");
            throw new RuntimeException(e);
        }
        logger.info("VASTBASE 数据库创建分区表："+partTableName+"成功");
    }

    @Override
    public void copyData() throws IOException {
        logger.info("Highgo数据库 批量入库...执行 COPY STDIN ");

        String insertIntoJar = config.getInsertIntoJar();
        String l2oProperties = config.getL2oProperties();

//        批量入库执行
        try {
            long loadStart = System.currentTimeMillis();
            String logFile = "Scenario1_copy_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
            JavaProcessExecutor.executeJavaProcess(insertIntoJar, l2oProperties, 0, logFile);
            long loadEnd = System.currentTimeMillis();
            logger.info("批量入库耗时: " + ((loadEnd - loadStart) / 1000) + " 秒");
        } catch (Exception e) {
            logger.error("批量入库执行程序失败", e);
            throw e;
        }
    }

    @Override
    public void createPartIndexes() throws IOException {

    }
}
