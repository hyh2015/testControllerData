package org.dbBenchPerfTest.scenarios;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.dbBenchPerfTest.inface.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Scenario1 implements Scenario {

    private static final Logger logger = LoggerFactory.getLogger(Scenario1.class);
    String scenario1 = "Scenario1";
    TestConfig config ;

    public Scenario1(TestConfig config) {
        this.config = config;
    }

    /**
     * 场景1： 批量入库（copy...stdin...） + 创建索引（4个）
     *
     * @param db
     * @throws Exception
     */
    @Override
    public void run(DatabaseInface db) throws Exception {

        logger.info("-----------------------["+scenario1+"] 开始执行 --------------------------");

        String copyFileNum = DbManager.getProperty("copy.file.num");
        String copyThreadNum = DbManager.getProperty("copy.thread.num");

        String partTableName = config.getPartTableName();

//        1. 修改 l2o.properties，设置 file.num = 365,thread.num = 4,bulkload = true
        UpdateConfProperties.updateConcurrentInsertConfig(partTableName, copyFileNum, copyThreadNum, true);
        logger.info("["+scenario1+"] 更新 l2o.properties 文件成功.");

//        2. 调用执行iostat dstat监控
        String monitorInterval600 = DbManager.getProperty("monitorInterval.600");
        logger.info("["+scenario1+"] 启动 iostat / dstat 监控，间隔: " + monitorInterval600);
        MonitorIOUtils.MonitorProcesses monitors = MonitorIOUtils.startIOstatDstatOutput(scenario1, monitorInterval600, "copy");

//        3. 批量入库执行
        logger.info("["+scenario1+"] 开始执行批量入库程序...");
        db.copyData();

//       4. 启动创建索引的IOSTAT DSTAT
        logger.info("["+scenario1+"] 启动创建索引阶段性能监控 iostat 和 dstat...");
        MonitorIOUtils.MonitorProcesses monitorCreateIdx = MonitorIOUtils.startIOstatDstatOutput(scenario1, monitorInterval600, "createIndex");

//        5. 创建分区索引
        logger.info("["+scenario1+"] 开始创建分区索引");
        db.createPartIndexes();

//       6. 杀掉 iostat 和 dstat（若仍在运行）
        MonitorIOUtils.stopMonitoring(monitors);

        MonitorIOUtils.stopMonitoring(monitorCreateIdx);
    }
}
