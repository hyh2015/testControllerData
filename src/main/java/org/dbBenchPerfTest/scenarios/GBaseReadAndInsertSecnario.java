package org.dbBenchPerfTest.scenarios;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.dbBenchPerfTest.inface.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class GBaseReadAndInsertSecnario implements Scenario  {

    private static final Logger logger = LoggerFactory.getLogger(GBaseReadAndInsertSecnario.class);
    String readAndInsertSecnario = "GBaseReadAndInsertSecnario";
    TestConfig config;

    public GBaseReadAndInsertSecnario(TestConfig config) {
        this.config = config;
    }

    @Override
    public void run(DatabaseInface db) throws Exception {


        logger.info("----------------------------[" + readAndInsertSecnario + "] 开始执行 -------------------------------");

        logger.info("开始执行：GBase数据库并发执行读写场景");
        String insertTableEvt = config.getInsertTableEvt();
        String recordTable1 = config.getRecordTable1();
        String dbType = config.getDbType();
        String tableMigJar = config.getTableMigJar();
        String configProperties = config.getConfigProperties();
        String insertIntoJar = config.getInsertIntoJar();
        String l2oProperties = config.getL2oProperties();
        int timeHour = Integer.parseInt(DbManager.getProperty("timeout.binfa.hour"));

        //       1.1 更新 config.properties 的配置
        if (!UpdateConfProperties.updateReadConfig("1","100")) {
            logger.error("["+readAndInsertSecnario+"] 更新 config.properties 配置文件失败，终止执行");
            return;
        }
        logger.info("["+readAndInsertSecnario+"] 更新 config.properties 文件成功");

        //        1.2 修改 l2o.properties，设置 file.num = 150,thread.num = 10,bulkload = false
        String fileNum = DbManager.getProperty("binfaInsert.file.num");
        String threadNum = DbManager.getProperty("insert.thread.num");
        UpdateConfProperties.updateConcurrentInsertConfig(insertTableEvt, fileNum, threadNum, false);
        logger.info("更新 l2o.properties 文件成功.");

        //       2. 预处理逻辑
        logger.info("数据库开始进行相关记录表的预处理");
        PrepareSecnarioEnvironment.prepareGBase8sScenario5Environment(config.getConn(), insertTableEvt,recordTable1,dbType);

//       3. 启动性能监控
        String monitorInterval600 = DbManager.getProperty("monitorInterval.600");
        logger.info("启动性能监控 iostat 和 dstat，间隔为 " + monitorInterval600 + " 秒...");

        MonitorIOUtils.MonitorProcesses monitorsReadAndInsert = MonitorIOUtils.startIOstatDstatOutput(readAndInsertSecnario, monitorInterval600, "Read100AndInsert");

//       4. 执行并发读程序
        ExecutorService executor = Executors.newFixedThreadPool(2); // 开两个线程

        Future<?> future1 = executor.submit(() -> {
            try {
                // 场景3：并发随机读 3h 100并发
                logger.info("并发任务1（随机读：100个并发 并发随机读"+timeHour+"小时）");
                String logFile = readAndInsertSecnario + ".Read100.out." + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
                JavaProcessExecutor.executeJavaProcess(tableMigJar, configProperties, timeHour, logFile);

            } catch (Exception e) {
                logger.error("并发任务1（随机读）执行失败", e);
                e.printStackTrace();
            }
        });

        Future<?> future2 = executor.submit(() -> {
            try {
                //场景4：逐条入库
                logger.info("并发任务2（逐条入库：执行入库 "+fileNum+"个文件，每个file大概4.1G）");
                long startTime = System.currentTimeMillis();
                String logFile = readAndInsertSecnario + ".insert"+fileNum+"File.out." + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
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

//        5.停止监控进程
        MonitorIOUtils.stopMonitoring(monitorsReadAndInsert);

        logger.info("[场景5] 停止性能监控进程完成");

//        6.获取部分指标信息
        RecordTableSelector.recordTableSqlList(config.getConn(), recordTable1);

        config.getConn().close();


        logger.info("[场景5] 场景3和场景4均已执行完毕，场景5结束");

    }
}

