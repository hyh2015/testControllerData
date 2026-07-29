package org.dbBenchPerfTest.scenarios;

import org.testController.DbManager;

public class SceneExecutorNew {


    private final String dbType;
    private final String dataPath;
    private final String dbDriverClass;
    private final String dbURL;
    private final String dbUser;
    private final String dbPassword;


    public SceneExecutorNew(String dbType) {
        this.dbType = dbType;
        this.dataPath = DbManager.getProperty("data.path");
        this.dbDriverClass = DbManager.getProperty(dbType + ".driver");
        this.dbURL = DbManager.getProperty(dbType + ".url")
                .replace("{host}", DbManager.getProperty(dbType + ".host"))
                .replace("{port}", DbManager.getProperty(dbType + ".port"))
                .replace("{database}", DbManager.getProperty(dbType + ".database"));
        this.dbUser = DbManager.getProperty(dbType + ".user");
        this.dbPassword = DbManager.getProperty(dbType + ".password");
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

