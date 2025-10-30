package org.dbBenchPerfTest.scenarios;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.dbBenchPerfTest.inface.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.*;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 场景3：并发随机读（默认100个并发）默认指定查询时间3h
 *
 */
public class Scenario3 implements Scenario {

    private static final Logger logger = LoggerFactory.getLogger(Scenario3.class);
    String scenario3 = "Scenario3";

    TestConfig config ;

    public Scenario3(TestConfig config) {
        this.config = config;
    }

    @Override
    public void run(DatabaseInface db) throws Exception {
        String scenario3mode1 = "1";

        logger.info("----------------------------[" + scenario3 + "] 开始执行 -------------------------------");
        String recordTable1 = config.getRecordTable1();
        String tableMigJar = config.getTableMigJar();
        String configProperties = config.getConfigProperties();
        String dbType = config.getDbType();

//       1. 更新 config.properties 的配置
        if (!UpdateConfProperties.updateConcurrentReadConfig(scenario3mode1)) {
            logger.error("["+scenario3+"] 更新 config.properties 配置文件失败，终止执行");
            return;
        }
        logger.info("["+scenario3+"] 更新 config.properties 文件成功");

//       2. 预处理逻辑
        logger.info("数据库开始进行相关记录表的预处理");
        PrepareSecnarioEnvironment.prepareScenario3Environment(config.getConn(),dbType,recordTable1);

//       3. 启动性能监控
        String monitorInterval60 = DbManager.getProperty("monitorInterval.60");
        logger.info("启动性能监控 iostat 和 dstat，间隔为 " + monitorInterval60 + " 秒...");

        MonitorIOUtils.MonitorProcesses monitorsRead = MonitorIOUtils.startIOstatDstatOutput(scenario3, monitorInterval60, "read");

//       4. 执行并发读程序
        int timeHour = Integer.parseInt(DbManager.getProperty("timeout.read.hour"));
        String logFile = scenario3 + "_read_out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
        JavaProcessExecutor.executeJavaProcess(tableMigJar, configProperties, timeHour, logFile);

//       5. 获取部分指标信息
        RecordTableSelector.recordTableSqlList(config.getConn(), recordTable1);

//       6. 杀掉 iostat 和 dstat（若仍在运行）
        MonitorIOUtils.stopMonitoring(monitorsRead);

    }
}

