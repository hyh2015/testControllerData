package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PrepareSecnarioEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(PrepareSecnarioEnvironment.class);

    public static boolean checkTableIsExist(Connection conn, String tableName, String dbType, String owner) throws SQLException {

        String checkSql;
        if ("pgdb".equalsIgnoreCase(dbType) || "ivory".equalsIgnoreCase(dbType)) {
            checkSql = "SELECT to_regclass('" + tableName + "')";
        } else if ("vastdata".equalsIgnoreCase(dbType)) {
            checkSql = "SELECT * from pg_catalog.all_tables where owner=upper('" + owner + "') and table_name=upper('" + tableName + "')";
        } else if ("highgo".equalsIgnoreCase(dbType)) {
            checkSql = "select * from pg_catalog.pg_tables pt where tableowner=lower('" + owner + "') and tablename=lower('" + tableName + "')";
        } else if ("Oracle".equalsIgnoreCase(dbType)){
            checkSql = "SELECT * from dba_tables where owner=upper('" + owner + "') and table_name=upper('" + tableName + "')";
        } else if ("gbasedbt".equalsIgnoreCase(dbType)){
            checkSql = "select * from systables where tabid > 999 and tabname= lower('"+tableName+"') and owner=lower('"+owner+"');";
        } else {
            throw new IllegalArgumentException("不支持的数据库类型: " + dbType);
        }


        boolean tableExists = false;
        try(Statement stmtCheck = conn.createStatement();ResultSet rs = stmtCheck.executeQuery(checkSql)) {
            if (rs.next()) {
                if ("pgdb".equalsIgnoreCase(dbType) || "ivory".equalsIgnoreCase(dbType)) {
                    tableExists = rs.getString(1) != null;
                } else {
                    tableExists = true; // 有结果表示存在
                }
            }
        }

        logger.info("数据库 "+dbType + ": 表 "+tableName+" 存在返回true,否则返回false：" + tableExists);
        return tableExists;
    }

    /**
     * 为场景3准备环境 做一些table_list/record表的处理
     * @param conn
     * @param dbType
     * @param recordTable1 创建record 表1
     * @return
     * @throws SQLException
     */
    public static boolean prepareScenario3Environment(Connection conn, String dbType, String recordTable1) throws SQLException {


        boolean prepareScenario3 = true;

        /**
         * 1.1 固定选择 tb_usernum_list1 进行rename为 tb_usernum_list 使用
         *  存在tb_usernum_list1且不存在tb_usernum_list  进行正常开始rename 操作
         */
        String selectedTable = "TB_USERNUM_LIST1";
        String renamedTable = "TB_USERNUM_LIST";
        String owner = DbManager.getProperty(dbType + ".user");

//        1.2 根据数据库类型选择验证语句
        boolean selectedTableExists = checkTableIsExist(conn, selectedTable, dbType, owner);
        boolean renamedTableExists = checkTableIsExist(conn,renamedTable,dbType,owner);
        if (!selectedTableExists) {
            logger.warn("未找到 "+selectedTable+" 表，查看是否存在 "+renamedTable+" 表已rename操作");
            if(!renamedTableExists){
                throw new RuntimeException("未找到 "+renamedTable+" 表 无法继续执行场景3");
            }

            logger.info("已存在 "+renamedTable+" 表");

            // 非第一次执行场景3 只需要清空record table1
            try (Statement stmtTruncate = conn.createStatement()) {
                stmtTruncate.execute("TRUNCATE TABLE " + recordTable1);
                logger.info("[场景3] 清空 " + recordTable1 + " 表成功");
            }
            return prepareScenario3;
        } else if(renamedTableExists){
            try (Statement stmtTruncate = conn.createStatement()) {
                stmtTruncate.execute("DROP TABLE " + renamedTable);
                logger.info("[场景3] 删除 " + renamedTable + " 表成功");
            }
        }

//        1.3 将选中的表重命名为 tb_usernum_list
        String renameSql = "ALTER TABLE " + selectedTable + " RENAME TO " + renamedTable;

        try(Statement stmtRename = conn.createStatement()) {
            stmtRename.execute(renameSql);
            logger.info("[场景3] 成功将号码池表 " + selectedTable + " 重命名为 "+ renamedTable);
        }

//        1.4 创建 tb_table_list
        String createTableListSql = "";
        if (dbType.equalsIgnoreCase("Oracle")) {
            createTableListSql = "CREATE TABLE tb_table_list (tablename varchar2(128))";
        } else {
            createTableListSql = "CREATE TABLE IF NOT EXISTS tb_table_list (tablename TEXT)";
        }

        try(Statement stmtCreate = conn.createStatement()) {
            stmtCreate.execute(createTableListSql);
            logger.info("[场景3] 创建 tb_table_list 表成功");
        }

//        1.5 注册被测分区表名
        String partitionTable = DbManager.getProperty("partition.table_name"); // 可从配置读取
        String insertTableSql = "INSERT INTO tb_table_list VALUES('" + partitionTable + "')";

        try(Statement stmtInsert = conn.createStatement()) {
            stmtInsert.execute(insertTableSql);
            logger.info("[场景3] 注册分区表： " + partitionTable + "成功");
        }

//        1.6 创建测试记录表1
/*        boolean recordTable1Exists = checkTableIsExist(conn, recordTable1, dbType, owner);
        if(recordTable1Exists){
            try (Statement stmtTruncate = conn.createStatement()) {
                stmtTruncate.execute("TRUNCATE TABLE " + recordTable1);
                logger.info("[场景3] 清空 " + recordTable1 + " 表成功");
            }
        }*/

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


        try(Statement stmt = conn.createStatement()) {
            stmt.execute(createRecordSql);
            logger.info("[场景3] 创建record table： " + recordTable1 + " 表成功");
        }

        return prepareScenario3;

    }



    /**
     *  准备场景4环境 创建入库表 tb_evt_i 并创建该表索引
     * @param conn
     * @param dbType
     * @throws Exception
     */
    public static void prepareScenario4Environment(Connection conn, String dbType) throws Exception {

        String tableName = DbManager.getProperty("insert.tablename");
        String indexName = DbManager.getProperty("insert.indexname") + tableName;

//         1. 检查是否存在入库表 tb_evt_i 存在即清空
        String owner = DbManager.getProperty(dbType + ".user");
        boolean recordTable2Exists = checkTableIsExist(conn, tableName, dbType, owner);
        if(recordTable2Exists){
            try (Statement stmtTruncate = conn.createStatement()) {
                stmtTruncate.execute("TRUNCATE TABLE " + tableName);
                logger.info("[场景4] 清空 " + tableName + " 表成功");
                return;
            }
        } else {

            String createTableSQL = "";

            if (dbType.equalsIgnoreCase("Oracle")){
                createTableSQL = String.format("create table %s(\n" +
                        "begintime timestamp,\n" +
                        "usernum varchar(128),\n" +
                        "imei varchar(128),\n" +
                        "calltype varchar(128),\n" +
                        "netid varchar(128),\n" +
                        "lai varchar(128),\n" +
                        "ci varchar(128),\n" +
                        "imsi varchar(128),\n" +
                        "start_time varchar(128),\n" +
                        "end_time varchar(128),\n" +
                        "longitude varchar(128),\n" +
                        "latitude varchar(128),\n" +
                        "lacci varchar(128),\n" +
                        "timespan varchar(128),\n" +
                        "extra_longitude varchar(128),\n" +
                        "extra_latitude varchar(128),\n" +
                        "geospan varchar(128),\n" +
                        "anchorhash varchar(128),\n" +
                        "extra_geohash varchar(128),\n" +
                        "bd varchar(128),\n" +
                        "ad varchar(128),\n" +
                        "user_id varchar(128),\n" +
                        "address varchar(256),\n" +
                        "car_id varchar(128),\n" +
                        "mac varchar(128),\n" +
                        "mobile_mode varchar(128),\n" +
                        "usernum1 varchar(128),\n" +
                        "area varchar(128),\n" +
                        "ipv4 varchar(128),\n" +
                        "ipv6 varchar(128),\n" +
                        "mission_id varchar(256),\n" +
                        "bankcard_id varchar(128)\n" +
                        ")", tableName);
            } else {
                createTableSQL = String.format("create table if not exists %s(\n" +
                        "begintime timestamp,\n" +
                        "usernum varchar(128),\n" +
                        "imei varchar(128),\n" +
                        "calltype varchar(128),\n" +
                        "netid varchar(128),\n" +
                        "lai varchar(128),\n" +
                        "ci varchar(128),\n" +
                        "imsi varchar(128),\n" +
                        "start_time varchar(128),\n" +
                        "end_time varchar(128),\n" +
                        "longitude varchar(128),\n" +
                        "latitude varchar(128),\n" +
                        "lacci varchar(128),\n" +
                        "timespan varchar(128),\n" +
                        "extra_longitude varchar(128),\n" +
                        "extra_latitude varchar(128),\n" +
                        "geospan varchar(128),\n" +
                        "anchorhash varchar(128),\n" +
                        "extra_geohash varchar(128),\n" +
                        "bd varchar(128),\n" +
                        "ad varchar(128),\n" +
                        "user_id varchar(128),\n" +
                        "address varchar(128),\n" +
                        "car_id varchar(128),\n" +
                        "mac varchar(128),\n" +
                        "mobile_mode varchar(128),\n" +
                        "usernum1 varchar(128),\n" +
                        "area varchar(128),\n" +
                        "ipv4 varchar(128),\n" +
                        "ipv6 varchar(128),\n" +
                        "mission_id varchar(128),\n" +
                        "bankcard_id varchar(128)\n" +
                        ")", tableName);
            }

            Statement stmt = conn.createStatement();
            stmt.execute(createTableSQL);
            logger.info("[场景4] 创建入库表 " + tableName + " 成功");

//         2. 创建本地索引
            String createIndexSQL = "";
            if(dbType.equalsIgnoreCase("Oracle")){
                createIndexSQL = "CREATE INDEX " + indexName + " ON " + tableName + " (usernum)";
            }else {
                createIndexSQL = "CREATE INDEX " + indexName + " ON " + tableName + " USING BTREE(usernum)";
            }
            stmt.execute(createIndexSQL);
            logger.info("[场景4] 创建本地索引 " + indexName + " 成功");

            stmt.close();
        }

    }

    /**
     *  为场景5 做环境初始化
     * @param connect
     * @param insertTableEvt  清空入库表 tb_evt_i
     * @param recordTable1    并发读记录表 tb_test_record_sql1
     * @param dbType          数据库类型
     */
    public static void prepareScenario5Environment(Connection connect, String insertTableEvt, String recordTable1, String dbType) throws SQLException {
        String owner = DbManager.getProperty(dbType + ".user");
        // 5.1 继续使用recordtable1表进行并发读 保持前后场景读取类型一致
        handleRecordTable(connect,recordTable1,owner,dbType);

/*
//        5.1 创建测试记录表2
        boolean recordTable2Exists = checkTableIsExist(connect, recordTable2, dbType, owner);
        if(recordTable2Exists){
            try (Statement stmtTruncate = connect.createStatement()) {
                stmtTruncate.execute("TRUNCATE TABLE " + recordTable2);
                logger.info("[场景5] 清空 " + recordTable2 + " 表成功");
            }
        }

        String createRecordSql = "";
        if (dbType.equalsIgnoreCase("Oracle")) {
            createRecordSql = "CREATE TABLE  " + recordTable2 + " ( \n" +
                    "thread_name varchar2(128), \n" +
                    "missionid varchar2(128), \n" +
                    "query_num varchar2(128),\n" +
                    "return_time number,\n" +
                    "count_finish_time number,\n" +
                    "count number,\n" +
                    "record_time TIMESTAMP)";
        } else {
            createRecordSql = "CREATE TABLE IF NOT EXISTS " + recordTable2 + " (" +
                    "thread_name TEXT," +
                    "missionid TEXT," +
                    "query_num TEXT," +
                    "return_time NUMERIC," +
                    "count_finish_time NUMERIC," +
                    "count NUMERIC," +
                    "record_time TIMESTAMP)";
        }
        try (Statement stmtCreateTable = connect.createStatement()) {
            stmtCreateTable.execute(createRecordSql);
            logger.info("[场景5] 5.1 创建record table " + recordTable2 + " 表成功");
        } catch (SQLException throwables) {
            logger.error("[场景5] 5.1 创建record table " + recordTable2 + " 表失败");
            throwables.printStackTrace();
        }*/


//        5.2 更新config.properties配置文件
        String scenario5mode1 = "1";
        if (!UpdateConfProperties.updateConcurrentReadConfig(scenario5mode1)) {
            logger.error("[场景5] 5.2 更新 config.properties 配置文件失败，终止执行");
            return;
        }
        logger.info("[场景5] 5.2 更新 config.properties 文件成功");


//        5.3 清空tb_evt_i表
        try (Statement stmtTruncate = connect.createStatement()) {
            stmtTruncate.execute("TRUNCATE TABLE " + insertTableEvt);
            logger.info("[场景5] 5.3 清空 " + insertTableEvt + " 表成功");

        } catch (SQLException e) {
            logger.error("[场景5] 5.3 清空表：" + insertTableEvt + "失败");
            e.printStackTrace();
        }

//        5.4 修改 l2o.properties，设置 file.num = 150,thread.num = 10,bulkload = false
        String fileNum = DbManager.getProperty("binfaInsert.file.num");
        String threadNum = DbManager.getProperty("insert.thread.num");
        UpdateConfProperties.updateConcurrentInsertConfig(insertTableEvt, fileNum, threadNum, false);
        logger.info("[场景5] 5.4 更新 l2o.properties 文件成功.");
    }

    // 对record表1进行处理
    private static void handleRecordTable(Connection connect, String recordTable1, String owner, String dbType) throws SQLException {

        String newTable = recordTable1 + "_Scenario5_" + System.currentTimeMillis();

        boolean tableExists = PrepareSecnarioEnvironment.checkTableIsExist(connect, recordTable1, dbType, owner);
        if (tableExists) {
            try (Statement stmt = connect.createStatement()) {
//            备份表到新表
                stmt.execute("create table "+newTable+" as select * from "+recordTable1);
                logger.info("将 " + recordTable1 + "表成功备份到表："+newTable +".");
//            清空原record表
                stmt.execute("truncate table " + recordTable1);
                logger.info("清空 " + recordTable1 + " 表成功");
            }
        }
    }

    /**
     * 单为GBase并发读写环境使用
     * @param connect
     * @param insertTableEvt
     * @param recordTable1      精确查询使用record表 1
     * @param dbType
     */
    public static void prepareGBase8sScenario5Environment(Connection connect, String insertTableEvt, String recordTable1, String dbType) throws SQLException {
        //        5.1 创建测试记录表2
        String owner = DbManager.getProperty(dbType + ".user");
        boolean recordTable2Exists = checkTableIsExist(connect, recordTable1, dbType, owner);
        if(recordTable2Exists){
            try (Statement stmtTruncate = connect.createStatement()) {
                stmtTruncate.execute("TRUNCATE TABLE " + recordTable1);
                logger.info("[场景5] 清空 " + recordTable1 + " 表成功");
            }
        }

        String createRecordSql = "";

        createRecordSql = "CREATE TABLE IF NOT EXISTS " + recordTable1 + " (" +
                "thread_name TEXT," +
                "missionid TEXT," +
                "query_num TEXT," +
                "return_time NUMERIC," +
                "count_finish_time NUMERIC," +
                "count NUMERIC," +
                "record_time TIMESTAMP)";

        try (Statement stmtCreateTable = connect.createStatement()) {
            stmtCreateTable.execute(createRecordSql);
            logger.info(" 创建record table " + recordTable1 + " 表成功");
        } catch (SQLException throwables) {
            logger.error(" 创建record table " + recordTable1 + " 表失败");
            throwables.printStackTrace();
        }



        // 清空tb_evt_i表
        try (Statement stmtTruncate = connect.createStatement()) {
            stmtTruncate.execute("TRUNCATE TABLE " + insertTableEvt);
            logger.info("[场景5] 5.3 清空 " + insertTableEvt + " 表成功");

        } catch (SQLException e) {
            logger.error("[场景5] 5.3 清空表：" + insertTableEvt + "失败");
            e.printStackTrace();
        }
    }
}

