package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PrepareScenarioEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(PrepareScenarioEnvironment.class);


    /**
     * 为场景4环境 创建入库表 tb_evt_i 并创建该表索引
     *
     * @param conn
     * @param dbType
     * @throws SQLException
     */
    public static void prepareScenario4Environment(Connection conn, String dbType) throws SQLException {

        String tableName = DbManager.getProperty("insert.tablename");
        String indexName = DbManager.getProperty("insert.indexname");

        String createTableSQL = "";

//         1. 创建入库表 tb_evt_i
        if (dbType.equalsIgnoreCase("Oracle")) {
            createTableSQL = "create table " + tableName + "(\n" +
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
                    ") ";
        } else {
            createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                    "begintime timestamp," +
                    "usernum text," +
                    "imei text," +
                    "calltype text," +
                    "netid text," +
                    "lai text," +
                    "ci text," +
                    "imsi text," +
                    "start_time text," +
                    "end_time text," +
                    "longitude text," +
                    "latitude text," +
                    "lacci text," +
                    "timespan text," +
                    "extra_longitude text," +
                    "extra_latitude text," +
                    "geospan text," +
                    "anchorhash text," +
                    "extra_geohash text," +
                    "bd text," +
                    "ad text," +
                    "user_id text," +
                    "address text," +
                    "car_id text," +
                    "mac text," +
                    "mobile_mode text," +
                    "usernum1 text," +
                    "area text," +
                    "ipv4 text," +
                    "ipv6 text," +
                    "mission_id text," +
                    "bankcard_id text" +
                    ")";
        }


        Statement stmt = conn.createStatement();
        stmt.execute(createTableSQL);
        logger.info("[场景4] 创建入库表 " + tableName + " 成功");

//         2. 创建本地索引
        String createIndexSQL = "";
        if (dbType.equalsIgnoreCase("Oracle")) {
            createIndexSQL = "CREATE INDEX " + indexName + " ON " + tableName + " (usernum)";
        } else {
            createIndexSQL = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + tableName + " USING BTREE(usernum)";
        }
        stmt.execute(createIndexSQL);
        logger.info("[场景4] 创建本地索引 " + indexName + " 成功");

        stmt.close();
    }


    public static void prepareScenario5Environment(Connection conn, String insertTableEvt, String recordTable2, String dbType) {

//        5.1 创建测试记录表2
        String createRecordSql = "";
        if (dbType.equalsIgnoreCase("Oracle")) {
            createRecordSql = "CREATE TABLE " + recordTable2 + "(\n" +
                    "thread_name varchar2(128),\n" +
                    "missionid varchar2(128),\n" +
                    "query_num varchar2(128),\n" +
                    "return_time number,\n" +
                    "count_finish_time number,\n" +
                    "count number,\n" +
                    "record_time timestamp\n" +
                    ")";
        } else {
            createRecordSql = "CREATE TABLE IF NOT exists "+recordTable2+"(\n" +
                    "thread_name text,\n" +
                    "missionid text,\n" +
                    "query_num text,\n" +
                    "return_time numeric,\n" +
                    "count_finish_time numeric,\n" +
                    "count numeric,\n" +
                    "record_time timestamp\n" +
                    ");";
        }

        try(Statement statCreateTable = conn.createStatement()){
            statCreateTable.execute(createRecordSql);
            logger.info("[场景5] 5.1 创建record table "+ recordTable2 + "表成功");
        } catch (SQLException e) {
            logger.error("[场景5] 5.1 创建record table "+ recordTable2 + "表失败");
            throw new RuntimeException(e);
        }

//        5.2 更新config.properties配置文件
        String mode2 = "2";
        if (!UpdateConfProperties.updateConcurrentReadConfig(mode2)){
            logger.error("[场景5] 5.2 更新config.properties配置文件,终止执行");
            return;
        }
        logger.info("[场景5] 5.2 更新config.properties配置文件 成功");

//        5.3 清空tb_evt_i 表
        try(Statement statTruncate = conn.createStatement()){
            statTruncate.execute("truncate table "+insertTableEvt);
            logger.info("[场景5] 5.3 清空表 " + insertTableEvt + "成功");

            conn.close();
        } catch (SQLException e) {
            logger.error("[场景5] 5.3 清空表 " + insertTableEvt + "失败");
            throw new RuntimeException(e);
        }


//        5.4 修改l2o.properties文件参数
        String fileNum = DbManager.getProperty("binfaInsert.file.num");
        String threadNum = DbManager.getProperty("insert.thread.num");
        UpdateConfProperties.updateConcurrentInsertConfig(insertTableEvt, fileNum, threadNum, false);
        logger.info("[场景5] 5.4 更新l2o.properties文件成功");


    }

}
