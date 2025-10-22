package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SceneExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SceneExecutor.class);

    private final String dbType;
    private final Connection conn;
    private final String dataPath;
    private final String partTableName;
    private final LocalDate partStartDate;
    private final LocalDate partEndDate;

    private final String dbDriverClass;
    private final String dbURL;
    private final String dbUser;
    private final String dbPassword;

    // ivory/pg不支持sysdate函数 。所以默认record表1和表2。表名不能改变。表1记录场景3，表2记录场景5.
    private final String recordTable1= "tb_test_record_sql1";
    private final String recordTable2= "tb_test_record_sql2";

    private final String insertTableEvt;

    private final String tableMigJar= "tableMigration.jar";
    private final String insertIntoJar="InsertIntoOracle.jar";
    private final String mockdataJar = "mockdata.jar";

    private final String configProperties = "config.properties";
    private final String l2oProperties = "l2o.properties";

    static final String dirPath = DbManager.getProperty("data.path");
    static final int checkFileNum = Integer.parseInt(DbManager.getProperty("mockdata.file.num"));

    static final int fileSizeMb = 4102;

    public SceneExecutor(String dbType) {
        this.dbType = dbType;
        this.conn = DbManager.getConnection(dbType);
        this.dataPath = DbManager.getProperty("data.path");
        this.partTableName = DbManager.getProperty("partition.table_name");
        this.partStartDate = LocalDate.parse(DbManager.getProperty("partition.start_date"));
        this.partEndDate = LocalDate.parse(DbManager.getProperty("partition.end_date"));
        this.dbDriverClass = DbManager.getProperty(dbType + ".driver");
        this.dbURL = DbManager.getProperty(dbType + ".url")
                .replace("{host}", DbManager.getProperty(dbType + ".host"))
                .replace("{port}", DbManager.getProperty(dbType + ".port"))
                .replace("{database}", DbManager.getProperty(dbType + ".database"));
        this.dbUser = DbManager.getProperty(dbType + ".user");
        this.dbPassword = DbManager.getProperty(dbType + ".password");
        this.insertTableEvt = DbManager.getProperty("insert.tablename");
    }

    public void generateMockTestData() {
        logger.info("[预处理] 生成测试数据...");
        Mockdata.generateMockTestData(mockdataJar, dataPath);
        if (Mockdata.runMockScript() && Mockdata.waitForVaildFiles(checkFileNum, dirPath)) {
            logger.info("测试数据生成成功");
        } else {
            logger.error("测试数据生成失败");
        }
    }

    public void createPartitionTable() throws Exception {
        logger.info("[预处理] 创建分区表...");
        PatitionTableCreator.createPartitionTable(conn, partTableName, dbType, partStartDate, partEndDate);
    }

    /**
     * 场景1： 批量入库（copy...stdin...） + 创建索引（4个）
     *
     * @throws Exception
     */
    public void runScenario1() throws Exception {
        logger.info("[场景1] 批量入库...执行 COPY STDIN ");
        String Scenario1 = "Scenario1";

        String copyFileNum = DbManager.getProperty("copy.file.num");
        String copyThreadNum = DbManager.getProperty("copy.thread.num");

//        1.修改 l2o.properties，设置 file.num = 365,thread.num = 4,bulkload = true
        UpdateConfProperties.updateConcurrentInsertConfig(partTableName, copyFileNum, copyThreadNum, true);
        logger.info("[场景1] 更新 l2o.properties 文件成功.");

        String monitorInterval600 = DbManager.getProperty("monitorInterval.600");
        logger.info("[场景1] 启动 iostat / dstat 监控，间隔: " + monitorInterval600);

//        调用执行 iostat dstat 监控
        MonitorIOUtils.MonitorProcesses monitors = MonitorIOUtils.startIOstatDstatOutput(Scenario1,monitorInterval600,"copy");

        logger.info("[场景1] 开始执行批量入库程序...");
        try {
            long loadStart = System.currentTimeMillis();
//          2. 批量入库执行
            String logFile = Scenario1 + "_copy_out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
            JavaProcessExecutor.executeJavaProcess(insertIntoJar, l2oProperties, 0, logFile);
            long loadEnd = System.currentTimeMillis();
            logger.info("[场景1] 批量入库耗时: " + ((loadEnd - loadStart) / 1000) + " 秒");
        } catch (Exception e) {
            logger.error("[场景1] 批量入库执行程序失败", e);
            throw e;
        } finally {
            // 杀掉 iostat 和 dstat（若仍在运行）
            MonitorIOUtils.stopMonitoring(monitors);
        }

        logger.info("[场景1] 启动创建索引阶段性能监控 iostat 和 dstat...");
        MonitorIOUtils.MonitorProcesses monitorsCreateIdx = MonitorIOUtils.startIOstatDstatOutput(Scenario1,monitorInterval600,"createIndex");


//        分别创建四个索引
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

        if (dbType.equalsIgnoreCase("pgdb") || dbType.equalsIgnoreCase("ivory")) {
            PartitionIndexCreator.createPartitionIndexesPgIvory(conn, partTableName, indexFields, indexNames);
        } else if (dbType.equalsIgnoreCase("highgo") || dbType.equalsIgnoreCase("vastdata")
                || dbType.equalsIgnoreCase("Oracle")) {
            PartitionIndexCreator.createPartitionIndexesHgVb(conn, partTableName, indexFields, indexNames, dbType);
        }

        // 杀掉 iostat 和 dstat（若仍在运行）
        MonitorIOUtils.stopMonitoring(monitorsCreateIdx);

    }


    /**
     * 场景2：distinct计算
     *
     * @throws Exception
     */
    public void runScenario2() throws Exception {
        logger.info("[场景2] 执行动态创建用户列表表，并统计时间...");

        String baseUsernumTable = "tb_usernum_list";
        String sourceTable = partTableName; // 假设是 tb_part_2025
        String[] dates = {"2025-01-01", "2025-02-01", "2025-04-01"};

        try (Statement stmt = conn.createStatement()) {
            for (int i = 0; i < dates.length; i++) {
                String startDate = dates[i];
                // 计算结束日期（加一天）
                LocalDate start = LocalDate.parse(startDate);
                String endDate = start.plusDays(1).toString();

                String targetTable = baseUsernumTable + (i + 1);
                String sql = "";
                if (dbType.equalsIgnoreCase("Oracle")) {
                    sql = String.format(
                            "CREATE TABLE %s AS " +
                                    "SELECT DISTINCT usernum FROM %s " +
                                    "WHERE begintime >= to_date('%s','yyyymmdd') AND begintime < to_date('%s','yyyymmdd') AND usernum IS NOT NULL",
                            targetTable, sourceTable, startDate, endDate
                    );
                } else {
                    sql = String.format(
                            "CREATE TABLE %s AS " +
                                    "SELECT DISTINCT usernum FROM %s " +
                                    "WHERE begintime >= '%s' AND begintime < '%s' AND usernum IS NOT NULL",
                            targetTable, sourceTable, startDate, endDate
                    );
                }
                logger.info("[场景2] 开始执行SQL，创建表：" + targetTable);
                logger.info("[场景2] 执行SQL：" + sql);
                long startTime = System.currentTimeMillis();
                stmt.execute(sql);
                long endTime = System.currentTimeMillis();

                ResultSet rs = stmt.executeQuery("select count(*) from " + targetTable);
                if (rs.next()) {
                    int count = rs.getInt(1);
                    logger.info("[场景2] 表" + targetTable + " 共插入 " + count + " 条记录");
                }

                logger.info("[场景2] 创建表 " + targetTable + " 完成，耗时 " + ((endTime - startTime) / 1000.0) + " 秒");
            }
        }
    }

    /**
     * 场景3：并发随机读（默认100个并发）默认指定查询时间3h
     */
    public void runScenario3() throws Exception {

        String Scenario3 = "Scenario3";
        String mode1 = "1";

//       1. 更新 config.properties 的配置
        if (!UpdateConfProperties.updateConcurrentReadConfig(mode1)) {
            logger.error("[场景3] 更新 config.properties 配置文件失败，终止执行");
            return;
        }
        logger.info("[场景3] 更新 config.properties 文件成功");

//       1：预处理逻辑
        PrepareSecnarioEnvironment.prepareScenario3Environment(conn, dbType, recordTable1);

        /**
         //        1.1 固定选择 tb_usernum_list1

         String selectedTable = "TB_USERNUM_LIST1";
         String owner = DbManager.getProperty(dbType + ".user");

         //        1.2 根据数据库类型选择验证语句
         String checkSql;
         if ("pgdb".equalsIgnoreCase(dbType) || "ivory".equalsIgnoreCase(dbType)) {
         checkSql = "SELECT to_regclass('" + selectedTable + "')";
         } else if ("vastdata".equalsIgnoreCase(dbType)) {
         checkSql = "SELECT * from pg_catalog.all_tables where owner=upper('" + owner + "') and table_name=upper('" + selectedTable + "')";
         } else if ("highgo".equalsIgnoreCase(dbType)) {
         checkSql = "select * from pg_catalog.pg_tables pt where tableowner=lower('" + owner + "') and tablename=lower('" + selectedTable + "')";
         } else if ("Oracle".equalsIgnoreCase(dbType)){
         checkSql = "SELECT * from dba_tables where owner=upper('" + owner + "') and table_name=upper('" + selectedTable + "')";
         } else {
         throw new IllegalArgumentException("不支持的数据库类型: " + dbType);
         }

         boolean tableExists = false;
         Statement stmt = null;
         ResultSet rs = null;
         try {
         stmt = conn.createStatement();
         rs = stmt.executeQuery(checkSql);
         if (rs.next()) {
         if ("pgdb".equalsIgnoreCase(dbType) || "ivory".equalsIgnoreCase(dbType)) {
         tableExists = rs.getString(1) != null;
         } else {
         tableExists = true; // 有结果表示存在
         }
         }
         } finally {
         if (rs != null) rs.close();
         if (stmt != null) stmt.close();
         }

         if (!tableExists) {
         logger.warn("未找到 tb_usernum_list1 表，查看是否存在 tb_usernum_list 表已rename操作");
         //            throw new RuntimeException("未找到 tb_usernum_list1 表，无法继续执行场景3");

         }

         //        1.3 将选中的表重命名为 tb_usernum_list
         String renameSql = "ALTER TABLE " + selectedTable + " RENAME TO tb_usernum_list";
         stmt = null;
         try {
         stmt = conn.createStatement();
         stmt.execute(renameSql);
         logger.info("[场景3] 成功将号码池表 " + selectedTable + " 重命名为 tb_usernum_list");
         } finally {
         if (stmt != null) stmt.close();
         }

         //        1.4 创建 tb_table_list
         String createTableListSql = "";
         if (dbType.equalsIgnoreCase("Oracle")) {
         createTableListSql = "CREATE TABLE tb_table_list (tablename varchar2(128))";
         } else {
         createTableListSql = "CREATE TABLE IF NOT EXISTS tb_table_list (tablename TEXT)";
         }
         stmt = null;
         try {
         stmt = conn.createStatement();
         stmt.execute(createTableListSql);
         logger.info("[场景3] 创建 tb_table_list 表成功");
         } finally {
         if (stmt != null) stmt.close();
         }

         //        1.5 注册被测分区表名
         String partitionTable = DbManager.getProperty("partition.table_name"); // 可从配置读取
         String insertTableSql = "INSERT INTO tb_table_list VALUES('" + partitionTable + "')";
         stmt = null;
         try {
         stmt = conn.createStatement();
         stmt.execute(insertTableSql);
         logger.info("[场景3] 注册分区表： " + partitionTable + "成功");
         } finally {
         if (stmt != null) stmt.close();
         }

         //        1.6 创建测试记录表1
         String createRecordSql = "";
         if (dbType.equalsIgnoreCase("Oracle")) {
         createRecordSql = "CREATE TABLE  " + recordTable1 + " ( \n" +
         "thread_name varchar2(128), \n" +
         "missionid varchar2(128), \n" +
         "query_num varchar2(128),\n" +
         "return_time number,\n" +
         "count_finish_time number,\n" +
         "count number,\n" +
         "record_time TIMESTAMP)";
         } else {
         createRecordSql = "CREATE TABLE IF NOT EXISTS " + recordTable1 + " (" +
         "thread_name TEXT," +
         "missionid TEXT," +
         "query_num TEXT," +
         "return_time NUMERIC," +
         "count_finish_time NUMERIC," +
         "count NUMERIC," +
         "record_time TIMESTAMP)";
         }

         stmt = null;
         try {
         stmt = conn.createStatement();
         stmt.execute(createRecordSql);
         logger.info("[场景3] 创建record table " + recordTable1 + " 表成功");
         } finally {
         if (stmt != null) stmt.close();
         }

         */

        logger.info("数据库初始化完成，即将启动并发读测试...");

//        2. 启动性能监控
        String monitorInterval60 = DbManager.getProperty("monitorInterval.60");
        logger.info("启动性能监控 iostat 和 dstat，间隔为 " + monitorInterval60 + " 秒...");
        MonitorIOUtils.MonitorProcesses monitors = MonitorIOUtils.startIOstatDstatOutput(Scenario3,monitorInterval60,"read");


//        3. 执行并发随机读程序
        int timeHour = Integer.parseInt(DbManager.getProperty("timeout.read.hour"));
        String logFile = Scenario3 + "_read_out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
        JavaProcessExecutor.executeJavaProcess(tableMigJar, configProperties, timeHour, logFile);

        MonitorIOUtils.stopMonitoring(monitors);


        // 获取部分指标信息
        RecordTableSelector.recordTableSqlList(conn, recordTable1);

    }


    /**
     * 场景4： 逐条入库（insert values） 执行三次 每次入库15个文件
     */
    public void runScenario4() throws Exception {
        String Scenario4 = "Scenario4";
//        1. 确保先创建表和索引
        PrepareSecnarioEnvironment.prepareScenario4Environment(conn, dbType);

//        2. 修改 l2o.properties，设置 file.num = 15,thread.num = 10,bulkload = false
        String insertFileNum = DbManager.getProperty("insert.file.num");
        String insertThreadNum = DbManager.getProperty("insert.thread.num");
        UpdateConfProperties.updateConcurrentInsertConfig(insertTableEvt, insertFileNum, insertThreadNum, false);
        logger.info("[场景4] 更新 l2o.properties 文件成功.");

        String monitorInterval600 = DbManager.getProperty("monitorInterval.600");

        for (int round = 1; round <= 3; round++) {
            logger.info("[场景4] 第 " + round + " 次逐条入库开始...");

//            3. 启动性能监控
            logger.info("[场景4] 启动 iostat / dstat 监控，间隔: " + monitorInterval600);
            String logRound = "insertInto"+round;
            MonitorIOUtils.MonitorProcesses monitors = MonitorIOUtils.startIOstatDstatOutput(Scenario4,monitorInterval600,logRound);


//            4. 调用jar包执行
            long start = System.currentTimeMillis();
            String logFile = Scenario4 + "_insertInto" + round + "_out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
            JavaProcessExecutor.executeJavaProcess(insertIntoJar, l2oProperties, 0, logFile);
            long end = System.currentTimeMillis();

            double finishTimeSec = (end - start) / 1000.0;
            //        一个文件大小为4102Mb
            double rMB_sec = (Integer.parseInt(insertFileNum) * fileSizeMb)/finishTimeSec;

            logger.info("[场景4] 第 " + round + " 次入库完成，耗时：" + finishTimeSec + " 秒");
            logger.info("[场景4] 第 " + round + " 次入库完成，获取到的指标数据 rMB/sec：" + rMB_sec);


            MonitorIOUtils.stopMonitoring(monitors);
        }

        logger.info("[场景4] 所有逐条入库执行完成。");
    }

    /**
     *  场景5 并发随机读+逐条入库
     *
     * @throws IOException
     */
    public void runScenario5() throws IOException, SQLException {
        logger.info("[场景5] 开始执行：并发执行场景3和场景4");
        String Scenario5 = "Scenario5";
        int fileInsertNum = Integer.parseInt(DbManager.getProperty("binfaInsert.file.num"));
        int insert150FileRowNum = 1099500150;
        int timeHour = Integer.parseInt(DbManager.getProperty("timeout.binfa.hour"));
        int timeSec = timeHour * 3600;

//        准备环境，初始化
        PrepareSecnarioEnvironment.prepareScenario5Environment(conn, insertTableEvt, recordTable2, dbType);


//         1. 启动性能监控
        String monitorInterval600 = DbManager.getProperty("monitorInterval.600");
        MonitorIOUtils.MonitorProcesses monitors = MonitorIOUtils.startIOstatDstatOutput(Scenario5,monitorInterval600,"binfa");
        logger.info("[场景5] 启动 iostat 和 dstat 监控成功，间隔: " + monitorInterval600 + "s");


//        2. 并发执行
        ExecutorService executor = Executors.newFixedThreadPool(2); // 开两个线程

        long start = System.currentTimeMillis();
        Future<?> future1 = executor.submit(() -> {
            try {
                // 场景3：并发随机读 3h 100并发

                logger.info("并发任务1（随机读：100个并发 并发随机读"+timeHour+"小时）");
                String logFile = Scenario5 + "_100read_out_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
                JavaProcessExecutor.executeJavaProcess(tableMigJar, configProperties, timeHour, logFile);
            } catch (Exception e) {
                logger.error("并发任务1（随机读）执行失败", e);
                e.printStackTrace();
            }
        });

        Future<?> future2 = executor.submit(() -> {
            try {
                //场景4：逐条入库

                logger.info("并发任务2 （逐条入库：执行入库"+fileInsertNum+"个file，每个file大概4.1G）");
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

        logger.info("[场景5] 停止性能监控进程完成");

//        6.获取部分指标信息
        RecordTableSelector.recordTableSqlList(conn, recordTable2);

//
        double row_sec = insert150FileRowNum/timeSec;
        logger.info("[场景5] 获取到的指标数据 row/sec：" + row_sec);


        conn.close();

        logger.info("[场景5] 场景3和场景4均已执行完毕，场景5结束");
    }


    private void createIndex(Statement stmt, String indexName, String tableName, String columnName) throws SQLException {
        long start = System.currentTimeMillis();
        String sql = "";
        if (dbType.equalsIgnoreCase("Oracle")) {
            sql = String.format("CREATE INDEX %s ON %s (%s) LOCAL", indexName, tableName, columnName);
        } else {
            sql = String.format("CREATE INDEX %s ON %s (%s)", indexName, tableName, columnName);
        }
        logger.info("[场景1] 开始创建分区索引: {} on {}({})", indexName, tableName, columnName);
        stmt.execute(sql);
        long end = System.currentTimeMillis();
        logger.info(dbType + "数据库：索引 {} 创建耗时: {} 秒", indexName, (end - start) / 1000);
    }

    private static void destroyIfAlive(Process process) {
        if (process != null && process.isAlive()) {
            process.destroy();
            logger.info("已终止 " + process + " 进程");
        }
    }

    public String getDbDriverClass() {
        return dbDriverClass;
    }

    public String getDbURL() {
        return dbURL;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getDbType() {
        return dbType;
    }

    public String getDataPath() {
        return dataPath;
    }

}



