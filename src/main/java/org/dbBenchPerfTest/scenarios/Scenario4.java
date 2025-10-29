package org.dbBenchPerfTest.scenarios;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.dbBenchPerfTest.inface.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.*;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Scenario4 implements Scenario {

    private static final Logger logger = LoggerFactory.getLogger(Scenario4.class);
    String scenario4 = "Scenario4";
    TestConfig config ;

    public Scenario4(TestConfig config) {
        this.config = config;
    }


    @Override
    public void run(DatabaseInface db) throws Exception {

        logger.info("----------------------------[" + scenario4 + "] 开始执行 -------------------------------");

//        1. 修改 l2o.properties，设置 file.num = 15,thread.num = 10,bulkload = false
        String insertFileNum = DbManager.getProperty("insert.file.num");
        String insertThreadNum = DbManager.getProperty("insert.thread.num");
        String insertTableEvt = DbManager.getProperty("insert.tablename");

        UpdateConfProperties.updateConcurrentInsertConfig(insertTableEvt, insertFileNum, insertThreadNum, false);
        logger.info("[场景4] 更新 l2o.properties 文件成功.");

//        2. 开始执行逐条入库
        String insertIntoJar = config.getInsertIntoJar();
        String l2oProperties = config.getL2oProperties();

//        3. 确保先创建表和索引
        PrepareSecnarioEnvironment.prepareScenario4Environment(config.getConn(), config.getDbType());

        String monitorInterval600 = DbManager.getProperty("monitorInterval.600");

        for (int round = 1; round <= 3

                ; round++) {
            logger.info("[Scenario4] 第 " + round + " 次逐条入库开始...");

//           4. 启动性能监控
            logger.info("[Scenario4] 启动 iostat / dstat 监控，间隔: " + monitorInterval600);

            MonitorIOUtils.MonitorProcesses monitors = MonitorIOUtils.startIOstatDstatOutput("Scenario4", monitorInterval600, "insertInto"+round);

//            5. 调用jar包执行
            long start = System.currentTimeMillis();
            String logFile = "Scenario4_insertInto" + round + "_out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
            JavaProcessExecutor.executeJavaProcess(insertIntoJar, l2oProperties, 0, logFile);
            long end = System.currentTimeMillis();

            logger.info("[Scenario4] 第 " + round + " 次入库完成，耗时：" + ((end - start) / 1000.0) + " 秒");

            MonitorIOUtils.stopMonitoring(monitors);
        }
        logger.info("[场景4] 所有逐条入库执行完成。");

    }
}

