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

    private final String recordTable3 = "tb_test_record_sql3";
    private final String recordTable4 = "tb_test_record_sql4";

    // 被调度的两个程序。它们已随本项目打进同一个 fat jar，不再是独立 jar 文件，
    // 因此这里存的是主类名，由 JavaProcessExecutor 以 java -cp <自身jar> <主类> 拉起。
    private final String tableMigJar = "com.test.Test";        // 随机读负载
    private final String insertIntoJar = "com.s1.l2o.Start";   // 逐条/批量入库负载
    // mockdata 是自包含 fat jar（内置 logback，与本项目的 log4j2 绑定冲突），
    // 保持独立交付，仍由生成的 mock.sh 用 java -jar 调用。
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

    public String getRecordTable3() {
        return recordTable3;
    }

    public String getRecordTable4() {
        return recordTable4;
    }

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


