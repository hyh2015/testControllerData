package org.dbBenchPerfTest.dataBase;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.DbManager;
import org.testController.JavaProcessExecutor;
import org.testController.PartitionIndexCreator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 原生 PostgreSQL（db.type=pgdb），面向 PG 17。
 *
 * <p>用的是声明式分区（PG 10 起支持），与 {@link IvoryDatabase} 完全一致 ——
 * IvorySQL 本身就是 PG 分支，两者共用一套 DDL，这样 ivory 与 pgdb 的压测结果才有可比性。
 *
 * <p>入库侧无需改动：入库程序的 `L2OServer` 已按 `db.type` 分发，
 * `pgdb` 走 `L2PGByCopy`（COPY，场景 1），逐条 insert（场景 4）走通用实现。
 */
public class PostgresDatabase implements DatabaseInface {

    private static final Logger logger = LoggerFactory.getLogger(PostgresDatabase.class);

    private final TestConfig config;

    private final String dataType = "POSTGRESQL";

    public PostgresDatabase(TestConfig config) {
        this.config = config;
    }


    @Override
    public void createPartitionTable() throws SQLException {

        logger.info("[预处理] " + dataType + " 数据库开始创建分区表...");

        String partTableName = config.getPartTableName();

        String createTableSql = "CREATE TABLE IF NOT EXISTS " + partTableName + " ("
                + "begintime text," + "usernum text," + "imei text," + "calltype text," + "netid text," + "lai text," +
                "ci text," + "imsi text," + "start_time text," + "end_time text," + "longitude text," + "latitude text," +
                "lacci text," + "timespan text," + "extra_longitude text," + "extra_latitude text," + "geospan text," +
                "anchorhash text," + "extra_geohash text," + "bd text," + "ad text," + "user_id text," + "address text," +
                "car_id text," + "mac text," + "mobile_mode text," + "usernum1 text," + "area text," + "ipv4 text," +
                "ipv6 text," + "mission_id text," + "bankcard_id text"
                + ") ";


        createTableSql += "PARTITION BY RANGE (begintime)";
        logger.info(dataType + " 数据库开始创建分区表基表：" + partTableName);

        try (Connection connec = DbManager.getConnection(config.getDbType());
             Statement stmt = connec.createStatement()) {
            stmt.execute(createTableSql);

            logger.info(dataType + " 数据库的分区表：" + partTableName + "基表创建成功");

            LocalDate partStartDate = config.getPartStartDate();
            LocalDate partEndDate = config.getPartEndDate();

            String doSql = String.format(
                    "DO $$\n" +
                            "DECLARE\n" +
                            "    start_date DATE := DATE '%s';\n" +
                            "    end_date DATE := DATE '%s';\n" +
                            "    partition_name TEXT;\n" +
                            "    table_name TEXT := '%s';\n" +
                            "BEGIN\n" +
                            "    WHILE start_date < end_date LOOP\n" +
                            "        partition_name := format('%%I_p%%s', table_name, to_char(start_date, 'YYYYMMDD'));\n" +
                            "        IF NOT EXISTS (\n" +
                            "            SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = partition_name\n" +
                            "        ) THEN\n" +
                            "            EXECUTE format(\n" +
                            "                'CREATE TABLE IF NOT EXISTS %%I PARTITION OF %%I FOR VALUES FROM (%%L::DATE) TO (%%L::DATE);',\n" +
                            "                partition_name,\n" +
                            "                table_name,\n" +
                            "                start_date,\n" +
                            "                start_date + INTERVAL '1 day'\n" +
                            "            );\n" +
                            "        END IF;\n" +
                            "        start_date := start_date + INTERVAL '1 day';\n" +
                            "    END LOOP;\n" +
                            "END\n" +
                            "$$;"
                    , partStartDate, partEndDate, partTableName);
            // 执行匿名块
            logger.info(dataType + " 数据库的分区表：" + partTableName + "，执行匿名块创建分区");
            stmt.execute(doSql);
            logger.info(dataType + " 数据库的分区表，匿名块执行创建分区成功");
        }

    }

    @Override
    public void copyData() {
        logger.info(dataType + " 数据库 批量入库...执行 COPY STDIN ");

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
    public void createPartIndexes() {

        String partTableName = config.getPartTableName();

        List<String> indexFields = new ArrayList<>();
        indexFields.add("usernum");
        indexFields.add("imei");
        indexFields.add("imsi");
        indexFields.add("lai,ci");
        List<String> indexNames = new ArrayList<>();
        indexNames.add(DbManager.getProperty("index.name.1") + partTableName);
        indexNames.add(DbManager.getProperty("index.name.2") + partTableName);
        indexNames.add(DbManager.getProperty("index.name.3") + partTableName);
        indexNames.add(DbManager.getProperty("index.name.4") + partTableName);

        // 走遍历 pg_inherits、给每个子分区单独建索引的方式，与 ivory 保持一致。
        // PG 11 起也可以直接对主表 CREATE INDEX（会自动下推到各分区），但那样会在整个
        // 建索引期间持有主表的 ACCESS EXCLUSIVE 锁，365 个日分区一次做完锁得太久；
        // 更重要的是，ivory 用的就是逐分区方式，保持一致 pgdb 与 ivory 的耗时才可比。
        PartitionIndexCreator.createPartitionIndexesPgIvory(config.getConn(), partTableName, indexFields, indexNames);
    }
}
