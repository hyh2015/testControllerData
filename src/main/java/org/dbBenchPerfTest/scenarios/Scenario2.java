package org.dbBenchPerfTest.scenarios;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.dbBenchPerfTest.inface.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.DbManager;
import org.testController.PrepareSecnarioEnvironment;

import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;

public class Scenario2 implements Scenario {

    private static final Logger logger = LoggerFactory.getLogger(Scenario2.class);
    String scenario2 = "Scenario2";

    TestConfig config;

    public Scenario2(TestConfig config) {
        this.config = config;
    }

    @Override
    public void run(DatabaseInface db) throws Exception {

        logger.info("----------------------------[" + scenario2 + "] 开始执行 -------------------------------");
        logger.info("[" + scenario2 + "] 执行动态创建用户列表表，并统计时间...");
        String baseUsernumTable = "tb_usernum_list";
        String sourceTable = config.getPartTableName();
        String[] dates = {"2025-01-25", "2025-02-25", "2025-03-25"};

        try (Statement stmt = config.getConn().createStatement()) {
            for (int i = 0; i < dates.length; i++) {
                String startDate = dates[i];
                // 计算结束日期（加一天）
                LocalDate start = LocalDate.parse(startDate);
                String endDate = start.plusDays(1).toString();
                String targetTable = baseUsernumTable + (i + 1);

                String owner = DbManager.getProperty(config.getDbType() + ".user");
                boolean tableExists = PrepareSecnarioEnvironment.checkTableIsExist(config.getConn(), targetTable, config.getDbType(), owner);
                if (tableExists) {
                    try (Statement stmtTruncate = config.getConn().createStatement()) {
                        stmtTruncate.execute("drop table " + targetTable);
                        logger.info("删除 " + targetTable + " 表成功");
                    }
                }

                String sql = String.format(
                        "CREATE TABLE %s AS " +
                                "SELECT DISTINCT usernum FROM %s " +
                                "WHERE begintime >= '%s' AND begintime < '%s' AND usernum IS NOT NULL",
                        targetTable, sourceTable, startDate, endDate
                );

                logger.info("开始执行SQL，创建表：" + targetTable);
                logger.info("执行SQL：" + sql);
                long startTime = System.currentTimeMillis();

                stmt.execute(sql);

                long endTime = System.currentTimeMillis();

                ResultSet rs = stmt.executeQuery("select count(*) from " + targetTable);
                if (rs.next()) {
                    int count = rs.getInt(1);
                    logger.info("表 " + targetTable + " 共插入 " + count + " 条数据");
                }
                logger.info("创建表 " + targetTable + " 完成，耗时 " + ((endTime - startTime) / 1000.0) + " 秒");
            }
        }
    }
}

