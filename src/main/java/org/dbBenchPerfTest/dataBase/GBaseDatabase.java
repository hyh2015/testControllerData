package org.dbBenchPerfTest.dataBase;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.DbManager;
import org.testController.PartitionIndexCreator;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class GBaseDatabase implements DatabaseInface {

    private static final Logger logger = LoggerFactory.getLogger(GBaseDatabase.class);

    private final TestConfig config;
    String tableNamePrex = "tab_storeone_";

    public GBaseDatabase(TestConfig config) {
        this.config = config;
    }



    @Override
    public void createPartitionTable() throws SQLException {


        String extTableName = DbManager.getProperty("external.table");
        List<String> listTableName = getTableNameList(tableNamePrex);

        /**
         *  todo 1. 创建分区表
         *       2. 创建外部表
         *       3. 使用外部表入库（insert into 分区表... select 外部表；）
         */


        logger.info("[预处理] GBase 数据库开始创建分区表...");

        for (int i = 1; i < 8; i++) {
            String partTableName = listTableName.get(i);
            String createPartTable = createPartitionTable(partTableName);

            try (Statement stmtTruncate = config.getConn().createStatement()) {
                stmtTruncate.execute(createPartTable);
                logger.info("创建分区表："+partTableName + "成功" );
            }

        }

//        try (Statement stmtTruncate = config.getConn().createStatement()) {
//            stmtTruncate.execute("drop table if exists " + extTableName);
//            logger.info("GBase数据库清理外部表： " + extTableName + " 表成功");
//        }

        logger.info("[预处理] GBase 数据库开始创建外部表...");
        String createSql = createExternalTable(extTableName);
        try (Statement stmtTruncate = config.getConn().createStatement()) {
            stmtTruncate.execute(createSql);
            logger.info("GBase数据库创建外部表： " + extTableName + " 表成功");
        }

        long startTime = System.currentTimeMillis();
        // 10t数据量大概需要7张入库表
        for (int i = 1; i < 8; i++) {
            String partTableName = tableNamePrex+i;
            String insertSql = "insert into "+partTableName+" select * from "+extTableName+"";

            try (Statement stmtTruncate = config.getConn().createStatement()) {
                logger.info("GBase数据库 开始执行批量入库 (使用insert外部表进行入库) " );
                stmtTruncate.execute(insertSql);
            }

        }

        long endTime = System.currentTimeMillis();
        logger.info("入库完成，耗时：" + ((endTime - startTime) / 1000.0) + " 秒");

    }

    @Override
    public void copyData()   {

    }

    @Override
    public void createPartIndexes() {

        String partTableName = config.getPartTableName();

        List<String> indexFields = new ArrayList<>();
        indexFields.add("usernum");
//        indexFields.add("usernum1");
        indexFields.add("imei");
        indexFields.add("imsi");
        indexFields.add("lai,ci");
        List<String> indexNames = new ArrayList<>();
        indexNames.add(DbManager.getProperty("index.name.1") + partTableName);
        indexNames.add(DbManager.getProperty("index.name.2") + partTableName);
        indexNames.add(DbManager.getProperty("index.name.3") + partTableName);
        indexNames.add(DbManager.getProperty("index.name.4") + partTableName);

        PartitionIndexCreator.createPartitionIndexesGBase8s(config.getConn(), partTableName, indexFields, indexNames, config.getDbType());

    }

    private static List<String> getTableNameList(String tableNamePrex){
        List<String> lists = new ArrayList<>();

        for (int i = 1; i < 8; i++) {
            lists.add(tableNamePrex+i);
        }

        return lists;
    }



    private String createExternalTable(String extTableName){

        String createSql = "create external table "+extTableName+" sameas "+extTableName+"1 \n" +
                "using (\n" +
                "  datafiles('disk:/storeone/GBase/loaddata/g2025-01-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-01-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-01-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-01-3%r(0..1).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-02-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-02-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-02-2%r(0..8).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-03-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-03-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-03-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-03-3%r(0..1).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-04-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-04-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-04-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-04-30.unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-05-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-05-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-05-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-05-3%r(0..1).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-06-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-06-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-06-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-06-30.unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-07-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-07-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-07-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-07-3%r(0..1).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-08-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-08-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-08-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-08-3%r(0..1).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-09-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-09-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-09-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-09-30.unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-10-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-10-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-10-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-10-3%r(0..1).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-11-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-11-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-11-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-11-30.unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-12-0%r(1..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-12-1%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-12-2%r(0..9).unl',\n" +
                "            'disk:/storeone/GBase/loaddata/g2025-12-3%r(0..1).unl'\n" +
                "  ),\n" +
                "  format 'delimited',\n" +
                "  delimiter ',',\n" +
                "  rejectfile '/storeone/GBase/0901/err_ext_tab.log',\n" +
                "  maxerrors 99999999\n" +
                ")";

        return createSql;
    }



    private String createPartitionTable(String tableName){


        String sql = "create raw table "+tableName+" \n" +
                "(\n" +
                "begintime datetime year to second,\n" +
                "usernum varchar(255),\n" +
                "imei varchar(255),\n" +
                "calltype varchar(255),\n" +
                "netid varchar(255),\n" +
                "lai varchar(255),\n" +
                "ci varchar(255),\n" +
                "imsi varchar(255),\n" +
                "start_time varchar(255),\n" +
                "end_time varchar(255),\n" +
                "longitude varchar(255),\n" +
                "latitude varchar(255),\n" +
                "lacci varchar(255),\n" +
                "timespan varchar(255),\n" +
                "extra_longitude varchar(255),\n" +
                "extra_latitude varchar(255),\n" +
                "geospan varchar(255),\n" +
                "anchorhash varchar(255),\n" +
                "extra_geohash varchar(255),\n" +
                "bd varchar(255),\n" +
                "ad varchar(255),\n" +
                "user_id varchar(255),\n" +
                "address varchar(255),\n" +
                "car_id varchar(255),\n" +
                "mac varchar(255),\n" +
                "mobile_mode varchar(255),\n" +
                "usernum1 varchar(255),\n" +
                "area varchar(255),\n" +
                "ipv4 varchar(255),\n" +
                "ipv6 varchar(255),\n" +
                "mission_id varchar(255),\n" +
                "bankcard_id varchar(255)\n" +
                ")\n" +
                "fragment by range(begintime) interval(1 units day)\n" +
                "partition p0        values < datetime(2025-01-01 00:00:00) year to second in datadbs00,\n" +
                "partition p20250101 values < datetime(2025-01-02 00:00:00) year to second in datadbs01,\n" +
                "partition p20250102 values < datetime(2025-01-03 00:00:00) year to second in datadbs01,\n" +
                "partition p20250103 values < datetime(2025-01-04 00:00:00) year to second in datadbs01,\n" +
                "partition p20250104 values < datetime(2025-01-05 00:00:00) year to second in datadbs01,\n" +
                "partition p20250105 values < datetime(2025-01-06 00:00:00) year to second in datadbs01,\n" +
                "partition p20250106 values < datetime(2025-01-07 00:00:00) year to second in datadbs01,\n" +
                "partition p20250107 values < datetime(2025-01-08 00:00:00) year to second in datadbs01,\n" +
                "partition p20250108 values < datetime(2025-01-09 00:00:00) year to second in datadbs01,\n" +
                "partition p20250109 values < datetime(2025-01-10 00:00:00) year to second in datadbs01,\n" +
                "partition p20250110 values < datetime(2025-01-11 00:00:00) year to second in datadbs01,\n" +
                "partition p20250111 values < datetime(2025-01-12 00:00:00) year to second in datadbs01,\n" +
                "partition p20250112 values < datetime(2025-01-13 00:00:00) year to second in datadbs01,\n" +
                "partition p20250113 values < datetime(2025-01-14 00:00:00) year to second in datadbs01,\n" +
                "partition p20250114 values < datetime(2025-01-15 00:00:00) year to second in datadbs01,\n" +
                "partition p20250115 values < datetime(2025-01-16 00:00:00) year to second in datadbs01,\n" +
                "partition p20250116 values < datetime(2025-01-17 00:00:00) year to second in datadbs01,\n" +
                "partition p20250117 values < datetime(2025-01-18 00:00:00) year to second in datadbs01,\n" +
                "partition p20250118 values < datetime(2025-01-19 00:00:00) year to second in datadbs01,\n" +
                "partition p20250119 values < datetime(2025-01-20 00:00:00) year to second in datadbs01,\n" +
                "partition p20250120 values < datetime(2025-01-21 00:00:00) year to second in datadbs01,\n" +
                "partition p20250121 values < datetime(2025-01-22 00:00:00) year to second in datadbs01,\n" +
                "partition p20250122 values < datetime(2025-01-23 00:00:00) year to second in datadbs01,\n" +
                "partition p20250123 values < datetime(2025-01-24 00:00:00) year to second in datadbs01,\n" +
                "partition p20250124 values < datetime(2025-01-25 00:00:00) year to second in datadbs01,\n" +
                "partition p20250125 values < datetime(2025-01-26 00:00:00) year to second in datadbs01,\n" +
                "partition p20250126 values < datetime(2025-01-27 00:00:00) year to second in datadbs01,\n" +
                "partition p20250127 values < datetime(2025-01-28 00:00:00) year to second in datadbs01,\n" +
                "partition p20250128 values < datetime(2025-01-29 00:00:00) year to second in datadbs01,\n" +
                "partition p20250129 values < datetime(2025-01-30 00:00:00) year to second in datadbs01,\n" +
                "partition p20250130 values < datetime(2025-01-31 00:00:00) year to second in datadbs01,\n" +
                "partition p20250131 values < datetime(2025-02-01 00:00:00) year to second in datadbs02,\n" +
                "partition p20250201 values < datetime(2025-02-02 00:00:00) year to second in datadbs02,\n" +
                "partition p20250202 values < datetime(2025-02-03 00:00:00) year to second in datadbs02,\n" +
                "partition p20250203 values < datetime(2025-02-04 00:00:00) year to second in datadbs02,\n" +
                "partition p20250204 values < datetime(2025-02-05 00:00:00) year to second in datadbs02,\n" +
                "partition p20250205 values < datetime(2025-02-06 00:00:00) year to second in datadbs02,\n" +
                "partition p20250206 values < datetime(2025-02-07 00:00:00) year to second in datadbs02,\n" +
                "partition p20250207 values < datetime(2025-02-08 00:00:00) year to second in datadbs02,\n" +
                "partition p20250208 values < datetime(2025-02-09 00:00:00) year to second in datadbs02,\n" +
                "partition p20250209 values < datetime(2025-02-10 00:00:00) year to second in datadbs02,\n" +
                "partition p20250210 values < datetime(2025-02-11 00:00:00) year to second in datadbs02,\n" +
                "partition p20250211 values < datetime(2025-02-12 00:00:00) year to second in datadbs02,\n" +
                "partition p20250212 values < datetime(2025-02-13 00:00:00) year to second in datadbs02,\n" +
                "partition p20250213 values < datetime(2025-02-14 00:00:00) year to second in datadbs02,\n" +
                "partition p20250214 values < datetime(2025-02-15 00:00:00) year to second in datadbs02,\n" +
                "partition p20250215 values < datetime(2025-02-16 00:00:00) year to second in datadbs02,\n" +
                "partition p20250216 values < datetime(2025-02-17 00:00:00) year to second in datadbs02,\n" +
                "partition p20250217 values < datetime(2025-02-18 00:00:00) year to second in datadbs02,\n" +
                "partition p20250218 values < datetime(2025-02-19 00:00:00) year to second in datadbs02,\n" +
                "partition p20250219 values < datetime(2025-02-20 00:00:00) year to second in datadbs02,\n" +
                "partition p20250220 values < datetime(2025-02-21 00:00:00) year to second in datadbs02,\n" +
                "partition p20250221 values < datetime(2025-02-22 00:00:00) year to second in datadbs02,\n" +
                "partition p20250222 values < datetime(2025-02-23 00:00:00) year to second in datadbs02,\n" +
                "partition p20250223 values < datetime(2025-02-24 00:00:00) year to second in datadbs02,\n" +
                "partition p20250224 values < datetime(2025-02-25 00:00:00) year to second in datadbs02,\n" +
                "partition p20250225 values < datetime(2025-02-26 00:00:00) year to second in datadbs02,\n" +
                "partition p20250226 values < datetime(2025-02-27 00:00:00) year to second in datadbs02,\n" +
                "partition p20250227 values < datetime(2025-02-28 00:00:00) year to second in datadbs02,\n" +
                "partition p20250228 values < datetime(2025-03-01 00:00:00) year to second in datadbs03,\n" +
                "partition p20250301 values < datetime(2025-03-02 00:00:00) year to second in datadbs03,\n" +
                "partition p20250302 values < datetime(2025-03-03 00:00:00) year to second in datadbs03,\n" +
                "partition p20250303 values < datetime(2025-03-04 00:00:00) year to second in datadbs03,\n" +
                "partition p20250304 values < datetime(2025-03-05 00:00:00) year to second in datadbs03,\n" +
                "partition p20250305 values < datetime(2025-03-06 00:00:00) year to second in datadbs03,\n" +
                "partition p20250306 values < datetime(2025-03-07 00:00:00) year to second in datadbs03,\n" +
                "partition p20250307 values < datetime(2025-03-08 00:00:00) year to second in datadbs03,\n" +
                "partition p20250308 values < datetime(2025-03-09 00:00:00) year to second in datadbs03,\n" +
                "partition p20250309 values < datetime(2025-03-10 00:00:00) year to second in datadbs03,\n" +
                "partition p20250310 values < datetime(2025-03-11 00:00:00) year to second in datadbs03,\n" +
                "partition p20250311 values < datetime(2025-03-12 00:00:00) year to second in datadbs03,\n" +
                "partition p20250312 values < datetime(2025-03-13 00:00:00) year to second in datadbs03,\n" +
                "partition p20250313 values < datetime(2025-03-14 00:00:00) year to second in datadbs03,\n" +
                "partition p20250314 values < datetime(2025-03-15 00:00:00) year to second in datadbs03,\n" +
                "partition p20250315 values < datetime(2025-03-16 00:00:00) year to second in datadbs03,\n" +
                "partition p20250316 values < datetime(2025-03-17 00:00:00) year to second in datadbs03,\n" +
                "partition p20250317 values < datetime(2025-03-18 00:00:00) year to second in datadbs03,\n" +
                "partition p20250318 values < datetime(2025-03-19 00:00:00) year to second in datadbs03,\n" +
                "partition p20250319 values < datetime(2025-03-20 00:00:00) year to second in datadbs03,\n" +
                "partition p20250320 values < datetime(2025-03-21 00:00:00) year to second in datadbs03,\n" +
                "partition p20250321 values < datetime(2025-03-22 00:00:00) year to second in datadbs03,\n" +
                "partition p20250322 values < datetime(2025-03-23 00:00:00) year to second in datadbs03,\n" +
                "partition p20250323 values < datetime(2025-03-24 00:00:00) year to second in datadbs03,\n" +
                "partition p20250324 values < datetime(2025-03-25 00:00:00) year to second in datadbs03,\n" +
                "partition p20250325 values < datetime(2025-03-26 00:00:00) year to second in datadbs03,\n" +
                "partition p20250326 values < datetime(2025-03-27 00:00:00) year to second in datadbs03,\n" +
                "partition p20250327 values < datetime(2025-03-28 00:00:00) year to second in datadbs03,\n" +
                "partition p20250328 values < datetime(2025-03-29 00:00:00) year to second in datadbs03,\n" +
                "partition p20250329 values < datetime(2025-03-30 00:00:00) year to second in datadbs03,\n" +
                "partition p20250330 values < datetime(2025-03-31 00:00:00) year to second in datadbs03,\n" +
                "partition p20250331 values < datetime(2025-04-01 00:00:00) year to second in datadbs04,\n" +
                "partition p20250401 values < datetime(2025-04-02 00:00:00) year to second in datadbs04,\n" +
                "partition p20250402 values < datetime(2025-04-03 00:00:00) year to second in datadbs04,\n" +
                "partition p20250403 values < datetime(2025-04-04 00:00:00) year to second in datadbs04,\n" +
                "partition p20250404 values < datetime(2025-04-05 00:00:00) year to second in datadbs04,\n" +
                "partition p20250405 values < datetime(2025-04-06 00:00:00) year to second in datadbs04,\n" +
                "partition p20250406 values < datetime(2025-04-07 00:00:00) year to second in datadbs04,\n" +
                "partition p20250407 values < datetime(2025-04-08 00:00:00) year to second in datadbs04,\n" +
                "partition p20250408 values < datetime(2025-04-09 00:00:00) year to second in datadbs04,\n" +
                "partition p20250409 values < datetime(2025-04-10 00:00:00) year to second in datadbs04,\n" +
                "partition p20250410 values < datetime(2025-04-11 00:00:00) year to second in datadbs04,\n" +
                "partition p20250411 values < datetime(2025-04-12 00:00:00) year to second in datadbs04,\n" +
                "partition p20250412 values < datetime(2025-04-13 00:00:00) year to second in datadbs04,\n" +
                "partition p20250413 values < datetime(2025-04-14 00:00:00) year to second in datadbs04,\n" +
                "partition p20250414 values < datetime(2025-04-15 00:00:00) year to second in datadbs04,\n" +
                "partition p20250415 values < datetime(2025-04-16 00:00:00) year to second in datadbs04,\n" +
                "partition p20250416 values < datetime(2025-04-17 00:00:00) year to second in datadbs04,\n" +
                "partition p20250417 values < datetime(2025-04-18 00:00:00) year to second in datadbs04,\n" +
                "partition p20250418 values < datetime(2025-04-19 00:00:00) year to second in datadbs04,\n" +
                "partition p20250419 values < datetime(2025-04-20 00:00:00) year to second in datadbs04,\n" +
                "partition p20250420 values < datetime(2025-04-21 00:00:00) year to second in datadbs04,\n" +
                "partition p20250421 values < datetime(2025-04-22 00:00:00) year to second in datadbs04,\n" +
                "partition p20250422 values < datetime(2025-04-23 00:00:00) year to second in datadbs04,\n" +
                "partition p20250423 values < datetime(2025-04-24 00:00:00) year to second in datadbs04,\n" +
                "partition p20250424 values < datetime(2025-04-25 00:00:00) year to second in datadbs04,\n" +
                "partition p20250425 values < datetime(2025-04-26 00:00:00) year to second in datadbs04,\n" +
                "partition p20250426 values < datetime(2025-04-27 00:00:00) year to second in datadbs04,\n" +
                "partition p20250427 values < datetime(2025-04-28 00:00:00) year to second in datadbs04,\n" +
                "partition p20250428 values < datetime(2025-04-29 00:00:00) year to second in datadbs04,\n" +
                "partition p20250429 values < datetime(2025-04-30 00:00:00) year to second in datadbs04,\n" +
                "partition p20250430 values < datetime(2025-05-01 00:00:00) year to second in datadbs05,\n" +
                "partition p20250501 values < datetime(2025-05-02 00:00:00) year to second in datadbs05,\n" +
                "partition p20250502 values < datetime(2025-05-03 00:00:00) year to second in datadbs05,\n" +
                "partition p20250503 values < datetime(2025-05-04 00:00:00) year to second in datadbs05,\n" +
                "partition p20250504 values < datetime(2025-05-05 00:00:00) year to second in datadbs05,\n" +
                "partition p20250505 values < datetime(2025-05-06 00:00:00) year to second in datadbs05,\n" +
                "partition p20250506 values < datetime(2025-05-07 00:00:00) year to second in datadbs05,\n" +
                "partition p20250507 values < datetime(2025-05-08 00:00:00) year to second in datadbs05,\n" +
                "partition p20250508 values < datetime(2025-05-09 00:00:00) year to second in datadbs05,\n" +
                "partition p20250509 values < datetime(2025-05-10 00:00:00) year to second in datadbs05,\n" +
                "partition p20250510 values < datetime(2025-05-11 00:00:00) year to second in datadbs05,\n" +
                "partition p20250511 values < datetime(2025-05-12 00:00:00) year to second in datadbs05,\n" +
                "partition p20250512 values < datetime(2025-05-13 00:00:00) year to second in datadbs05,\n" +
                "partition p20250513 values < datetime(2025-05-14 00:00:00) year to second in datadbs05,\n" +
                "partition p20250514 values < datetime(2025-05-15 00:00:00) year to second in datadbs05,\n" +
                "partition p20250515 values < datetime(2025-05-16 00:00:00) year to second in datadbs05,\n" +
                "partition p20250516 values < datetime(2025-05-17 00:00:00) year to second in datadbs05,\n" +
                "partition p20250517 values < datetime(2025-05-18 00:00:00) year to second in datadbs05,\n" +
                "partition p20250518 values < datetime(2025-05-19 00:00:00) year to second in datadbs05,\n" +
                "partition p20250519 values < datetime(2025-05-20 00:00:00) year to second in datadbs05,\n" +
                "partition p20250520 values < datetime(2025-05-21 00:00:00) year to second in datadbs05,\n" +
                "partition p20250521 values < datetime(2025-05-22 00:00:00) year to second in datadbs05,\n" +
                "partition p20250522 values < datetime(2025-05-23 00:00:00) year to second in datadbs05,\n" +
                "partition p20250523 values < datetime(2025-05-24 00:00:00) year to second in datadbs05,\n" +
                "partition p20250524 values < datetime(2025-05-25 00:00:00) year to second in datadbs05,\n" +
                "partition p20250525 values < datetime(2025-05-26 00:00:00) year to second in datadbs05,\n" +
                "partition p20250526 values < datetime(2025-05-27 00:00:00) year to second in datadbs05,\n" +
                "partition p20250527 values < datetime(2025-05-28 00:00:00) year to second in datadbs05,\n" +
                "partition p20250528 values < datetime(2025-05-29 00:00:00) year to second in datadbs05,\n" +
                "partition p20250529 values < datetime(2025-05-30 00:00:00) year to second in datadbs05,\n" +
                "partition p20250530 values < datetime(2025-05-31 00:00:00) year to second in datadbs05,\n" +
                "partition p20250531 values < datetime(2025-06-01 00:00:00) year to second in datadbs06,\n" +
                "partition p20250601 values < datetime(2025-06-02 00:00:00) year to second in datadbs06,\n" +
                "partition p20250602 values < datetime(2025-06-03 00:00:00) year to second in datadbs06,\n" +
                "partition p20250603 values < datetime(2025-06-04 00:00:00) year to second in datadbs06,\n" +
                "partition p20250604 values < datetime(2025-06-05 00:00:00) year to second in datadbs06,\n" +
                "partition p20250605 values < datetime(2025-06-06 00:00:00) year to second in datadbs06,\n" +
                "partition p20250606 values < datetime(2025-06-07 00:00:00) year to second in datadbs06,\n" +
                "partition p20250607 values < datetime(2025-06-08 00:00:00) year to second in datadbs06,\n" +
                "partition p20250608 values < datetime(2025-06-09 00:00:00) year to second in datadbs06,\n" +
                "partition p20250609 values < datetime(2025-06-10 00:00:00) year to second in datadbs06,\n" +
                "partition p20250610 values < datetime(2025-06-11 00:00:00) year to second in datadbs06,\n" +
                "partition p20250611 values < datetime(2025-06-12 00:00:00) year to second in datadbs06,\n" +
                "partition p20250612 values < datetime(2025-06-13 00:00:00) year to second in datadbs06,\n" +
                "partition p20250613 values < datetime(2025-06-14 00:00:00) year to second in datadbs06,\n" +
                "partition p20250614 values < datetime(2025-06-15 00:00:00) year to second in datadbs06,\n" +
                "partition p20250615 values < datetime(2025-06-16 00:00:00) year to second in datadbs06,\n" +
                "partition p20250616 values < datetime(2025-06-17 00:00:00) year to second in datadbs06,\n" +
                "partition p20250617 values < datetime(2025-06-18 00:00:00) year to second in datadbs06,\n" +
                "partition p20250618 values < datetime(2025-06-19 00:00:00) year to second in datadbs06,\n" +
                "partition p20250619 values < datetime(2025-06-20 00:00:00) year to second in datadbs06,\n" +
                "partition p20250620 values < datetime(2025-06-21 00:00:00) year to second in datadbs06,\n" +
                "partition p20250621 values < datetime(2025-06-22 00:00:00) year to second in datadbs06,\n" +
                "partition p20250622 values < datetime(2025-06-23 00:00:00) year to second in datadbs06,\n" +
                "partition p20250623 values < datetime(2025-06-24 00:00:00) year to second in datadbs06,\n" +
                "partition p20250624 values < datetime(2025-06-25 00:00:00) year to second in datadbs06,\n" +
                "partition p20250625 values < datetime(2025-06-26 00:00:00) year to second in datadbs06,\n" +
                "partition p20250626 values < datetime(2025-06-27 00:00:00) year to second in datadbs06,\n" +
                "partition p20250627 values < datetime(2025-06-28 00:00:00) year to second in datadbs06,\n" +
                "partition p20250628 values < datetime(2025-06-29 00:00:00) year to second in datadbs06,\n" +
                "partition p20250629 values < datetime(2025-06-30 00:00:00) year to second in datadbs06,\n" +
                "partition p20250630 values < datetime(2025-07-01 00:00:00) year to second in datadbs07,\n" +
                "partition p20250701 values < datetime(2025-07-02 00:00:00) year to second in datadbs07,\n" +
                "partition p20250702 values < datetime(2025-07-03 00:00:00) year to second in datadbs07,\n" +
                "partition p20250703 values < datetime(2025-07-04 00:00:00) year to second in datadbs07,\n" +
                "partition p20250704 values < datetime(2025-07-05 00:00:00) year to second in datadbs07,\n" +
                "partition p20250705 values < datetime(2025-07-06 00:00:00) year to second in datadbs07,\n" +
                "partition p20250706 values < datetime(2025-07-07 00:00:00) year to second in datadbs07,\n" +
                "partition p20250707 values < datetime(2025-07-08 00:00:00) year to second in datadbs07,\n" +
                "partition p20250708 values < datetime(2025-07-09 00:00:00) year to second in datadbs07,\n" +
                "partition p20250709 values < datetime(2025-07-10 00:00:00) year to second in datadbs07,\n" +
                "partition p20250710 values < datetime(2025-07-11 00:00:00) year to second in datadbs07,\n" +
                "partition p20250711 values < datetime(2025-07-12 00:00:00) year to second in datadbs07,\n" +
                "partition p20250712 values < datetime(2025-07-13 00:00:00) year to second in datadbs07,\n" +
                "partition p20250713 values < datetime(2025-07-14 00:00:00) year to second in datadbs07,\n" +
                "partition p20250714 values < datetime(2025-07-15 00:00:00) year to second in datadbs07,\n" +
                "partition p20250715 values < datetime(2025-07-16 00:00:00) year to second in datadbs07,\n" +
                "partition p20250716 values < datetime(2025-07-17 00:00:00) year to second in datadbs07,\n" +
                "partition p20250717 values < datetime(2025-07-18 00:00:00) year to second in datadbs07,\n" +
                "partition p20250718 values < datetime(2025-07-19 00:00:00) year to second in datadbs07,\n" +
                "partition p20250719 values < datetime(2025-07-20 00:00:00) year to second in datadbs07,\n" +
                "partition p20250720 values < datetime(2025-07-21 00:00:00) year to second in datadbs07,\n" +
                "partition p20250721 values < datetime(2025-07-22 00:00:00) year to second in datadbs07,\n" +
                "partition p20250722 values < datetime(2025-07-23 00:00:00) year to second in datadbs07,\n" +
                "partition p20250723 values < datetime(2025-07-24 00:00:00) year to second in datadbs07,\n" +
                "partition p20250724 values < datetime(2025-07-25 00:00:00) year to second in datadbs07,\n" +
                "partition p20250725 values < datetime(2025-07-26 00:00:00) year to second in datadbs07,\n" +
                "partition p20250726 values < datetime(2025-07-27 00:00:00) year to second in datadbs07,\n" +
                "partition p20250727 values < datetime(2025-07-28 00:00:00) year to second in datadbs07,\n" +
                "partition p20250728 values < datetime(2025-07-29 00:00:00) year to second in datadbs07,\n" +
                "partition p20250729 values < datetime(2025-07-30 00:00:00) year to second in datadbs07,\n" +
                "partition p20250730 values < datetime(2025-07-31 00:00:00) year to second in datadbs07,\n" +
                "partition p20250731 values < datetime(2025-08-01 00:00:00) year to second in datadbs08,\n" +
                "partition p20250801 values < datetime(2025-08-02 00:00:00) year to second in datadbs08,\n" +
                "partition p20250802 values < datetime(2025-08-03 00:00:00) year to second in datadbs08,\n" +
                "partition p20250803 values < datetime(2025-08-04 00:00:00) year to second in datadbs08,\n" +
                "partition p20250804 values < datetime(2025-08-05 00:00:00) year to second in datadbs08,\n" +
                "partition p20250805 values < datetime(2025-08-06 00:00:00) year to second in datadbs08,\n" +
                "partition p20250806 values < datetime(2025-08-07 00:00:00) year to second in datadbs08,\n" +
                "partition p20250807 values < datetime(2025-08-08 00:00:00) year to second in datadbs08,\n" +
                "partition p20250808 values < datetime(2025-08-09 00:00:00) year to second in datadbs08,\n" +
                "partition p20250809 values < datetime(2025-08-10 00:00:00) year to second in datadbs08,\n" +
                "partition p20250810 values < datetime(2025-08-11 00:00:00) year to second in datadbs08,\n" +
                "partition p20250811 values < datetime(2025-08-12 00:00:00) year to second in datadbs08,\n" +
                "partition p20250812 values < datetime(2025-08-13 00:00:00) year to second in datadbs08,\n" +
                "partition p20250813 values < datetime(2025-08-14 00:00:00) year to second in datadbs08,\n" +
                "partition p20250814 values < datetime(2025-08-15 00:00:00) year to second in datadbs08,\n" +
                "partition p20250815 values < datetime(2025-08-16 00:00:00) year to second in datadbs08,\n" +
                "partition p20250816 values < datetime(2025-08-17 00:00:00) year to second in datadbs08,\n" +
                "partition p20250817 values < datetime(2025-08-18 00:00:00) year to second in datadbs08,\n" +
                "partition p20250818 values < datetime(2025-08-19 00:00:00) year to second in datadbs08,\n" +
                "partition p20250819 values < datetime(2025-08-20 00:00:00) year to second in datadbs08,\n" +
                "partition p20250820 values < datetime(2025-08-21 00:00:00) year to second in datadbs08,\n" +
                "partition p20250821 values < datetime(2025-08-22 00:00:00) year to second in datadbs08,\n" +
                "partition p20250822 values < datetime(2025-08-23 00:00:00) year to second in datadbs08,\n" +
                "partition p20250823 values < datetime(2025-08-24 00:00:00) year to second in datadbs08,\n" +
                "partition p20250824 values < datetime(2025-08-25 00:00:00) year to second in datadbs08,\n" +
                "partition p20250825 values < datetime(2025-08-26 00:00:00) year to second in datadbs08,\n" +
                "partition p20250826 values < datetime(2025-08-27 00:00:00) year to second in datadbs08,\n" +
                "partition p20250827 values < datetime(2025-08-28 00:00:00) year to second in datadbs08,\n" +
                "partition p20250828 values < datetime(2025-08-29 00:00:00) year to second in datadbs08,\n" +
                "partition p20250829 values < datetime(2025-08-30 00:00:00) year to second in datadbs08,\n" +
                "partition p20250830 values < datetime(2025-08-31 00:00:00) year to second in datadbs08,\n" +
                "partition p20250831 values < datetime(2025-09-01 00:00:00) year to second in datadbs09,\n" +
                "partition p20250901 values < datetime(2025-09-02 00:00:00) year to second in datadbs09,\n" +
                "partition p20250902 values < datetime(2025-09-03 00:00:00) year to second in datadbs09,\n" +
                "partition p20250903 values < datetime(2025-09-04 00:00:00) year to second in datadbs09,\n" +
                "partition p20250904 values < datetime(2025-09-05 00:00:00) year to second in datadbs09,\n" +
                "partition p20250905 values < datetime(2025-09-06 00:00:00) year to second in datadbs09,\n" +
                "partition p20250906 values < datetime(2025-09-07 00:00:00) year to second in datadbs09,\n" +
                "partition p20250907 values < datetime(2025-09-08 00:00:00) year to second in datadbs09,\n" +
                "partition p20250908 values < datetime(2025-09-09 00:00:00) year to second in datadbs09,\n" +
                "partition p20250909 values < datetime(2025-09-10 00:00:00) year to second in datadbs09,\n" +
                "partition p20250910 values < datetime(2025-09-11 00:00:00) year to second in datadbs09,\n" +
                "partition p20250911 values < datetime(2025-09-12 00:00:00) year to second in datadbs09,\n" +
                "partition p20250912 values < datetime(2025-09-13 00:00:00) year to second in datadbs09,\n" +
                "partition p20250913 values < datetime(2025-09-14 00:00:00) year to second in datadbs09,\n" +
                "partition p20250914 values < datetime(2025-09-15 00:00:00) year to second in datadbs09,\n" +
                "partition p20250915 values < datetime(2025-09-16 00:00:00) year to second in datadbs09,\n" +
                "partition p20250916 values < datetime(2025-09-17 00:00:00) year to second in datadbs09,\n" +
                "partition p20250917 values < datetime(2025-09-18 00:00:00) year to second in datadbs09,\n" +
                "partition p20250918 values < datetime(2025-09-19 00:00:00) year to second in datadbs09,\n" +
                "partition p20250919 values < datetime(2025-09-20 00:00:00) year to second in datadbs09,\n" +
                "partition p20250920 values < datetime(2025-09-21 00:00:00) year to second in datadbs09,\n" +
                "partition p20250921 values < datetime(2025-09-22 00:00:00) year to second in datadbs09,\n" +
                "partition p20250922 values < datetime(2025-09-23 00:00:00) year to second in datadbs09,\n" +
                "partition p20250923 values < datetime(2025-09-24 00:00:00) year to second in datadbs09,\n" +
                "partition p20250924 values < datetime(2025-09-25 00:00:00) year to second in datadbs09,\n" +
                "partition p20250925 values < datetime(2025-09-26 00:00:00) year to second in datadbs09,\n" +
                "partition p20250926 values < datetime(2025-09-27 00:00:00) year to second in datadbs09,\n" +
                "partition p20250927 values < datetime(2025-09-28 00:00:00) year to second in datadbs09,\n" +
                "partition p20250928 values < datetime(2025-09-29 00:00:00) year to second in datadbs09,\n" +
                "partition p20250929 values < datetime(2025-09-30 00:00:00) year to second in datadbs09,\n" +
                "partition p20250930 values < datetime(2025-10-01 00:00:00) year to second in datadbs10,\n" +
                "partition p20251001 values < datetime(2025-10-02 00:00:00) year to second in datadbs10,\n" +
                "partition p20251002 values < datetime(2025-10-03 00:00:00) year to second in datadbs10,\n" +
                "partition p20251003 values < datetime(2025-10-04 00:00:00) year to second in datadbs10,\n" +
                "partition p20251004 values < datetime(2025-10-05 00:00:00) year to second in datadbs10,\n" +
                "partition p20251005 values < datetime(2025-10-06 00:00:00) year to second in datadbs10,\n" +
                "partition p20251006 values < datetime(2025-10-07 00:00:00) year to second in datadbs10,\n" +
                "partition p20251007 values < datetime(2025-10-08 00:00:00) year to second in datadbs10,\n" +
                "partition p20251008 values < datetime(2025-10-09 00:00:00) year to second in datadbs10,\n" +
                "partition p20251009 values < datetime(2025-10-10 00:00:00) year to second in datadbs10,\n" +
                "partition p20251010 values < datetime(2025-10-11 00:00:00) year to second in datadbs10,\n" +
                "partition p20251011 values < datetime(2025-10-12 00:00:00) year to second in datadbs10,\n" +
                "partition p20251012 values < datetime(2025-10-13 00:00:00) year to second in datadbs10,\n" +
                "partition p20251013 values < datetime(2025-10-14 00:00:00) year to second in datadbs10,\n" +
                "partition p20251014 values < datetime(2025-10-15 00:00:00) year to second in datadbs10,\n" +
                "partition p20251015 values < datetime(2025-10-16 00:00:00) year to second in datadbs10,\n" +
                "partition p20251016 values < datetime(2025-10-17 00:00:00) year to second in datadbs10,\n" +
                "partition p20251017 values < datetime(2025-10-18 00:00:00) year to second in datadbs10,\n" +
                "partition p20251018 values < datetime(2025-10-19 00:00:00) year to second in datadbs10,\n" +
                "partition p20251019 values < datetime(2025-10-20 00:00:00) year to second in datadbs10,\n" +
                "partition p20251020 values < datetime(2025-10-21 00:00:00) year to second in datadbs10,\n" +
                "partition p20251021 values < datetime(2025-10-22 00:00:00) year to second in datadbs10,\n" +
                "partition p20251022 values < datetime(2025-10-23 00:00:00) year to second in datadbs10,\n" +
                "partition p20251023 values < datetime(2025-10-24 00:00:00) year to second in datadbs10,\n" +
                "partition p20251024 values < datetime(2025-10-25 00:00:00) year to second in datadbs10,\n" +
                "partition p20251025 values < datetime(2025-10-26 00:00:00) year to second in datadbs10,\n" +
                "partition p20251026 values < datetime(2025-10-27 00:00:00) year to second in datadbs10,\n" +
                "partition p20251027 values < datetime(2025-10-28 00:00:00) year to second in datadbs10,\n" +
                "partition p20251028 values < datetime(2025-10-29 00:00:00) year to second in datadbs10,\n" +
                "partition p20251029 values < datetime(2025-10-30 00:00:00) year to second in datadbs10,\n" +
                "partition p20251030 values < datetime(2025-10-31 00:00:00) year to second in datadbs10,\n" +
                "partition p20251031 values < datetime(2025-11-01 00:00:00) year to second in datadbs11,\n" +
                "partition p20251101 values < datetime(2025-11-02 00:00:00) year to second in datadbs11,\n" +
                "partition p20251102 values < datetime(2025-11-03 00:00:00) year to second in datadbs11,\n" +
                "partition p20251103 values < datetime(2025-11-04 00:00:00) year to second in datadbs11,\n" +
                "partition p20251104 values < datetime(2025-11-05 00:00:00) year to second in datadbs11,\n" +
                "partition p20251105 values < datetime(2025-11-06 00:00:00) year to second in datadbs11,\n" +
                "partition p20251106 values < datetime(2025-11-07 00:00:00) year to second in datadbs11,\n" +
                "partition p20251107 values < datetime(2025-11-08 00:00:00) year to second in datadbs11,\n" +
                "partition p20251108 values < datetime(2025-11-09 00:00:00) year to second in datadbs11,\n" +
                "partition p20251109 values < datetime(2025-11-10 00:00:00) year to second in datadbs11,\n" +
                "partition p20251110 values < datetime(2025-11-11 00:00:00) year to second in datadbs11,\n" +
                "partition p20251111 values < datetime(2025-11-12 00:00:00) year to second in datadbs11,\n" +
                "partition p20251112 values < datetime(2025-11-13 00:00:00) year to second in datadbs11,\n" +
                "partition p20251113 values < datetime(2025-11-14 00:00:00) year to second in datadbs11,\n" +
                "partition p20251114 values < datetime(2025-11-15 00:00:00) year to second in datadbs11,\n" +
                "partition p20251115 values < datetime(2025-11-16 00:00:00) year to second in datadbs11,\n" +
                "partition p20251116 values < datetime(2025-11-17 00:00:00) year to second in datadbs11,\n" +
                "partition p20251117 values < datetime(2025-11-18 00:00:00) year to second in datadbs11,\n" +
                "partition p20251118 values < datetime(2025-11-19 00:00:00) year to second in datadbs11,\n" +
                "partition p20251119 values < datetime(2025-11-20 00:00:00) year to second in datadbs11,\n" +
                "partition p20251120 values < datetime(2025-11-21 00:00:00) year to second in datadbs11,\n" +
                "partition p20251121 values < datetime(2025-11-22 00:00:00) year to second in datadbs11,\n" +
                "partition p20251122 values < datetime(2025-11-23 00:00:00) year to second in datadbs11,\n" +
                "partition p20251123 values < datetime(2025-11-24 00:00:00) year to second in datadbs11,\n" +
                "partition p20251124 values < datetime(2025-11-25 00:00:00) year to second in datadbs11,\n" +
                "partition p20251125 values < datetime(2025-11-26 00:00:00) year to second in datadbs11,\n" +
                "partition p20251126 values < datetime(2025-11-27 00:00:00) year to second in datadbs11,\n" +
                "partition p20251127 values < datetime(2025-11-28 00:00:00) year to second in datadbs11,\n" +
                "partition p20251128 values < datetime(2025-11-29 00:00:00) year to second in datadbs11,\n" +
                "partition p20251129 values < datetime(2025-11-30 00:00:00) year to second in datadbs11,\n" +
                "partition p20251130 values < datetime(2025-12-01 00:00:00) year to second in datadbs12,\n" +
                "partition p20251201 values < datetime(2025-12-02 00:00:00) year to second in datadbs12,\n" +
                "partition p20251202 values < datetime(2025-12-03 00:00:00) year to second in datadbs12,\n" +
                "partition p20251203 values < datetime(2025-12-04 00:00:00) year to second in datadbs12,\n" +
                "partition p20251204 values < datetime(2025-12-05 00:00:00) year to second in datadbs12,\n" +
                "partition p20251205 values < datetime(2025-12-06 00:00:00) year to second in datadbs12,\n" +
                "partition p20251206 values < datetime(2025-12-07 00:00:00) year to second in datadbs12,\n" +
                "partition p20251207 values < datetime(2025-12-08 00:00:00) year to second in datadbs12,\n" +
                "partition p20251208 values < datetime(2025-12-09 00:00:00) year to second in datadbs12,\n" +
                "partition p20251209 values < datetime(2025-12-10 00:00:00) year to second in datadbs12,\n" +
                "partition p20251210 values < datetime(2025-12-11 00:00:00) year to second in datadbs12,\n" +
                "partition p20251211 values < datetime(2025-12-12 00:00:00) year to second in datadbs12,\n" +
                "partition p20251212 values < datetime(2025-12-13 00:00:00) year to second in datadbs12,\n" +
                "partition p20251213 values < datetime(2025-12-14 00:00:00) year to second in datadbs12,\n" +
                "partition p20251214 values < datetime(2025-12-15 00:00:00) year to second in datadbs12,\n" +
                "partition p20251215 values < datetime(2025-12-16 00:00:00) year to second in datadbs12,\n" +
                "partition p20251216 values < datetime(2025-12-17 00:00:00) year to second in datadbs12,\n" +
                "partition p20251217 values < datetime(2025-12-18 00:00:00) year to second in datadbs12,\n" +
                "partition p20251218 values < datetime(2025-12-19 00:00:00) year to second in datadbs12,\n" +
                "partition p20251219 values < datetime(2025-12-20 00:00:00) year to second in datadbs12,\n" +
                "partition p20251220 values < datetime(2025-12-21 00:00:00) year to second in datadbs12,\n" +
                "partition p20251221 values < datetime(2025-12-22 00:00:00) year to second in datadbs12,\n" +
                "partition p20251222 values < datetime(2025-12-23 00:00:00) year to second in datadbs12,\n" +
                "partition p20251223 values < datetime(2025-12-24 00:00:00) year to second in datadbs12,\n" +
                "partition p20251224 values < datetime(2025-12-25 00:00:00) year to second in datadbs12,\n" +
                "partition p20251225 values < datetime(2025-12-26 00:00:00) year to second in datadbs12,\n" +
                "partition p20251226 values < datetime(2025-12-27 00:00:00) year to second in datadbs12,\n" +
                "partition p20251227 values < datetime(2025-12-28 00:00:00) year to second in datadbs12,\n" +
                "partition p20251228 values < datetime(2025-12-29 00:00:00) year to second in datadbs12,\n" +
                "partition p20251229 values < datetime(2025-12-30 00:00:00) year to second in datadbs12,\n" +
                "partition p20251230 values < datetime(2025-12-31 00:00:00) year to second in datadbs12,\n" +
                "partition p20251231 values < datetime(2026-01-01 00:00:00) year to second in datadbs12\n" +
                "extent size 4800000 next size 1024000 ";

        return sql;

    }
}

