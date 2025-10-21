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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Scenario5 implements Scenario {
    private static final Logger logger = LoggerFactory.getLogger(Scenario5.class);
    String scenario5 = "Scenario5";
    TestConfig config ;

    public Scenario5(TestConfig config) {
        this.config = config;
    }

    @Override
    public void run(DatabaseInface db) throws Exception {

        logger.info("-----------------------["+scenario5+"] 开始执行 --------------------------");

//       1. 启动性能监控
        String monitorInterval = DbManager.getProperty("monitorInterval.600");
        logger.info("[场景5] 启动 iostat 和 dstat 监控成功，间隔: " + monitorInterval + "s");
        MonitorIOUtils.MonitorProcesses monitors = MonitorIOUtils.startIOstatDstatOutput(scenario5,monitorInterval,"binfa");

        logger.info("[场景5] 开始执行：并发执行场景3和场景4");
        String Scenario5 = "Scenario5";
        String insertTableEvt = config.getInsertTableEvt();
        String recordTable2 = config.getRecordTable2();
        String dbType = config.getDbType();
        String tableMigJar = config.getTableMigJar();
        String configProperties = config.getConfigProperties();
        String insertIntoJar = config.getInsertIntoJar();
        String l2oProperties = config.getL2oProperties();

//        2. 准备环境，初始化
        PrepareScenarioEnvironment.prepareScenario5Environment(config.getConn(), insertTableEvt,recordTable2,dbType);



//        3. 并发执行
        ExecutorService executor = Executors.newFixedThreadPool(2); // 开两个线程

        long start = System.currentTimeMillis();
        Future<?> future1 = executor.submit(() -> {
            try {
                // 场景3：并发随机读 3h 100并发
                int timeHour = Integer.parseInt(DbManager.getProperty("timeout.binfa.hour"));
                logger.info("并发任务1（随机读：100个并发 并发随机读"+timeHour+"小时）");
                String logFile = Scenario5 + "_100read_out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
                JavaProcessExecutor.executeJavaProcess(tableMigJar,configProperties,timeHour,logFile);
            } catch (Exception e) {
                logger.error("并发任务1（随机读）执行失败", e);
                e.printStackTrace();
            }
        });

        Future<?> future2 = executor.submit(() -> {
            try {
                //场景4：逐条入库
                String fileNum = DbManager.getProperty("binfaInsert.file.num");
                logger.info("并发任务2（逐条入库：执行入库 "+fileNum+"个文件，每个file大概4.1G）");
                long startTime = System.currentTimeMillis();
                String logFile = Scenario5 + "_insert150File_out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
                JavaProcessExecutor.executeJavaProcess(insertIntoJar, l2oProperties, 0, logFile);
                long endTime = System.currentTimeMillis();
                logger.info("入库完成，耗时：" + ((endTime - startTime) / 1000.0) + " 秒");
            } catch (Exception e) {
                logger.error("并发任务2（逐条入库）执行失败", e);
                e.printStackTrace();
            }
        });

//        4.等待两个任务完成（可设置最大等待时间）
        executor.shutdown();
        try {
            if (!executor.awaitTermination(24, TimeUnit.HOURS)) {
                executor.shutdownNow();
                logger.warn("并发执行时间超过：24h");
            } else {
                logger.info("[场景5] 所有并发任务执行完成");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        long end = System.currentTimeMillis();

//        5.停止监控进程
        MonitorIOUtils.stopMonitoring(monitors);
    /*    destroyIfAlive(iostat);
        destroyIfAlive(dstat);*/
        logger.info("[场景5] 停止性能监控进程完成");

//        6.获取部分指标信息
        RecordTableSelector.recordTableSqlList(config.getConn(), recordTable2);

        config.getConn().close();


        logger.info("[场景5] 场景3和场景4均已执行完毕，场景5结束");


    }
}
