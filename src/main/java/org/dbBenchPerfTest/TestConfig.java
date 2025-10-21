package org.dbBenchPerfTest;

import org.testController.DbManager;

import java.sql.Connection;
import java.time.LocalDate;

public class TestConfig {
    private final String dataPath;
    private final String partTableName;
    private final LocalDate partStartDate;
    private final LocalDate partEndDate;
    private final String insertTableEvt;

    protected final String dbType;
    protected final Connection conn;
    protected final String dbUser;
    protected final String dbPassword;
    protected final String dbURL;
    protected final String dbDriverClass;

    // 固定表名
    private final String recordTable1 = "tb_test_record_sql1";
    private final String recordTable2 = "tb_test_record_sql2";

    // 依赖的 jar
    private final String tableMigJar = "tableMigration.jar";
    private final String insertIntoJar = "InsertIntoOracle.jar";
    private final String mockdataJar = "mockdata.jar";

    // 配置文件
    private final String configProperties = "config.properties";
    private final String l2oProperties = "l2o.properties";

    static String directoryPath = DbManager.getProperty("data.path");
    static int fileNum = Integer.parseInt(DbManager.getProperty("mockdata.file.num"));

    public TestConfig(String dbType) {
        this.dataPath = DbManager.getProperty("data.path");
        this.partTableName = DbManager.getProperty("partition.table_name");
        this.partStartDate = LocalDate.parse(DbManager.getProperty("partition.start_date"));
        this.partEndDate = LocalDate.parse(DbManager.getProperty("partition.end_date"));
        this.insertTableEvt = DbManager.getProperty("insert.tablename");

        this.dbType = dbType;
        this.conn = DbManager.getConnection(dbType);
        this.dbDriverClass = DbManager.getProperty(dbType + ".driver");
        this.dbURL = DbManager.getProperty(dbType + ".url")
                .replace("{host}", DbManager.getProperty(dbType + ".host"))
                .replace("{port}", DbManager.getProperty(dbType + ".port"))
                .replace("{database}", DbManager.getProperty(dbType + ".database"));
        this.dbUser = DbManager.getProperty(dbType + ".user");
        this.dbPassword = DbManager.getProperty(dbType + ".password");
    }

    // getter 方法
    public String getDataPath() { return dataPath; }
    public String getPartTableName() { return partTableName; }
    public LocalDate getPartStartDate() { return partStartDate; }
    public LocalDate getPartEndDate() { return partEndDate; }
    public String getInsertTableEvt() { return insertTableEvt; }

    public String getRecordTable1() { return recordTable1; }
    public String getRecordTable2() { return recordTable2; }

    public String getTableMigJar() {
        return tableMigJar;
    }

    public String getInsertIntoJar() {
        return insertIntoJar;
    }

    public String getMockdataJar() {
        return mockdataJar;
    }

    public String getConfigProperties() {
        return configProperties;
    }

    public String getL2oProperties() {
        return l2oProperties;
    }

    public static int getFileNum() {
        return fileNum;
    }

    public static String getDirectoryPath() {
        return directoryPath;
    }


    public String getDbType() {
        return dbType;
    }

    public Connection getConn() {
        return conn;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getDbURL() {
        return dbURL;
    }

    public String getDbDriverClass() {
        return dbDriverClass;
    }
}

