package org.dbBenchPerfTest.scenarios;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.dbBenchPerfTest.inface.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.*;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GBaseRandomReadSecnario implements Scenario {


    private static final Logger logger = LoggerFactory.getLogger(GBaseRandomReadSecnario.class);
    static String scenarioGBase = "GBaseSecnario";
    TestConfig config;

    String recordTableNo1 = "1";
    String recordTableNo2 = "2";
    String recordTableNo3 = "3";
    String recordTableNo4 = "4";

    String Thread50 = "50";
    String Thread100 = "100";
    String Thread200 = "200";
    int time2Hour = 2;

    public GBaseRandomReadSecnario(TestConfig config) {
        this.config = config;
    }

    @Override
    public void run(DatabaseInface db) throws Exception {

        /**
         * 精确查询场景  50 100 200 并发 多次测试
         */
        logger.info("----------------------------[" + scenarioGBase + "] 开始执行 -------------------------------");
        String recordTable1 = config.getRecordTable1();
        String recordTable2 = config.getRecordTable2();
        String recordTable3 = config.getRecordTable3();
        String recordTable4 = config.getRecordTable4();

        String owner = DbManager.getProperty(config.getDbType() + ".user");

        /**
         * 处理record表信息 再次执行下一轮场景的并发测试
         *  1. 判断record表是否存在 且不为空
         *  2. 进行 ctas备份表  再turncate原record表
         *  【精确查询】场景
         */

        String newTable50 = recordTable1+"_new50";
        String newTable100 = recordTable1+"_new100";
        String newTable200 = recordTable1+"_new200";

        /*********** 50并发【精确查询】场景 *****************/
        binfaLogic(recordTableNo1, recordTable1, Thread50, time2Hour, config, "singleRandomRead");
        handleRecordTable(newTable50, recordTable1, owner);
        Thread.sleep(10 * 60 * 1000);  // 暂停10min进行下一个并发程序

        /*********** 100并发【精确查询】场景 *****************/
        binfaLogic(recordTableNo1, recordTable1, Thread100, time2Hour, config, "singleRandomRead");
        handleRecordTable(newTable100, recordTable1, owner);
        Thread.sleep(10 * 60 * 1000);

        /*********** 200并发【精确查询】场景 *****************/
        binfaLogic(recordTableNo1, recordTable1, Thread200, time2Hour, config,"singleRandomRead");
        handleRecordTable(newTable200, recordTable1, owner);

        Thread.sleep(10 * 60 * 1000);
        /*********** 100并发【范围查询】场景 *****************/

        binfaLogic(recordTableNo2, recordTable2, Thread100, time2Hour, config, "rangeReadRandom");

        Thread.sleep(10 * 60 * 1000);
        /*********** 100并发【范围查询+排序】场景 *****************/

        binfaLogic(recordTableNo3, recordTable3, Thread100, time2Hour, config, "rangeReadOrderBy");

        Thread.sleep(10 * 60 * 1000);
        /*********** 100并发【范围查询+开窗】场景 *****************/

        binfaLogic(recordTableNo4, recordTable4, Thread100, time2Hour, config, "rangeReadRowNumberOver");

    }




    private void handleRecordTable(String newTable, String sourceTable, String owner) throws SQLException {

        boolean tableExists = PrepareSecnarioEnvironment.checkTableIsExist(config.getConn(), sourceTable, config.getDbType(), owner);
        if (tableExists) {

            try (Statement stmt = config.getConn().createStatement()) {
//            备份表到新表
                stmt.execute("create table "+newTable+" as select * from "+sourceTable);
                logger.info("创建 " + newTable + "表成功");

//            清空原record表
                stmt.execute("truncate table " + sourceTable);
                logger.info("清空 " + sourceTable + " 表成功");
            }
        }
    }




    public static void binfaLogic(String recordNameNum,String recordTableName,String threads,int time2Hour, TestConfig config, String logName ) throws SQLException, IOException {
        //       1. 更新 config.properties 的配置
        if (!UpdateConfProperties.updateReadConfig(recordNameNum,threads)) {
            logger.error("["+scenarioGBase+"] 更新 config.properties 配置文件失败，终止执行");
            return;
        }
        logger.info("["+scenarioGBase+"] 更新 config.properties 文件成功");

        //       2. 预处理逻辑
        logger.info("数据库开始进行相关记录表的预处理");
        PrepareSecnarioEnvironment.prepareScenario3Environment(config.getConn(),config.getDbType(),recordTableName);

//       3. 启动性能监控
        String monitorInterval600 = DbManager.getProperty("monitorInterval.600");
        logger.info("启动性能监控 iostat 和 dstat，间隔为 " + monitorInterval600 + " 秒...");

        MonitorIOUtils.MonitorProcesses monitorsRead = MonitorIOUtils.startIOstatDstatOutput(scenarioGBase, monitorInterval600, logName+threads);

//       4. 执行并发读程序
        String logFile = scenarioGBase + "."+logName + threads +".out." + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
        JavaProcessExecutor.executeJavaProcess(config.getTableMigJar(), config.getConfigProperties(), time2Hour, logFile);


//       5. 获取部分指标信息
        RecordTableSelector.recordTableSqlList(config.getConn(), recordTableName);

//       6. 杀掉 iostat 和 dstat（若仍在运行）
        MonitorIOUtils.stopMonitoring(monitorsRead);

    }

}

