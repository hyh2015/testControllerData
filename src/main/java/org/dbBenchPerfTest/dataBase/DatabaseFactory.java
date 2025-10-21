package org.dbBenchPerfTest.dataBase;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.DatabaseInface;

public class DatabaseFactory {


    public static DatabaseInface getDatabase(TestConfig config) {
        String dbType = config.getDbType();

        switch (dbType.toLowerCase()) {
            case "pgdb":
                return new PostgresDatabase(config);
            case "highgo":
                return new HighgoDatabase(config);
            case "vastdata":
                return new VastbaseDatabase(config);
            case "ivory":
                return new IvoryDatabase(config);
            default:
                throw new IllegalArgumentException("不支持的数据库类型: " + dbType);
        }
    }
}

