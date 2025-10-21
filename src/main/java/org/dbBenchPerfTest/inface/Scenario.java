package org.dbBenchPerfTest.inface;

public interface Scenario {

//    Database 是一个数据库抽象接口
    void run(DatabaseInface db) throws Exception;
}
