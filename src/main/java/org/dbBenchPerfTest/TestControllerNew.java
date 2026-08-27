package org.dbBenchPerfTest;

import org.dbBenchPerfTest.checkDatabase.CheckDatabaseInstall;
import org.dbBenchPerfTest.dataBase.DatabaseFactory;
import org.dbBenchPerfTest.inface.DatabaseInface;
import org.dbBenchPerfTest.scenarios.*;
import org.testController.DbManager;

public class TestControllerNew {

    private final DatabaseInface db;
    private final TestConfig config;

    public TestControllerNew(TestConfig testConfig) {
        this.db = DatabaseFactory.getDatabase(testConfig);
        this.config = new TestConfig(testConfig.getDbType());
    }

    public void runAllTests() throws Exception {
        // 场景开关缺失时一律按不执行处理：漏配置不该意外多跑一个耗时几小时、
        // 还会 DROP/TRUNCATE 表的场景。
        if (DbManager.isEnabled("scene.mock.enabled", false)) {
            new ScenarioMockData(config).run(db);
        }
        if (DbManager.isEnabled("scene.createPartition.enabled", false)) {
            db.createPartitionTable();
        }
        if (DbManager.isEnabled("scene1.enabled", false)) {
            new Scenario1(config).run(db);
        }
        if (DbManager.isEnabled("scene2.enabled", false)) {
            new Scenario2(config).run(db);
        }
        if (DbManager.isEnabled("scene3.enabled", false)) {
            new Scenario3(config).run(db);
        }
        if (DbManager.isEnabled("scene4.enabled", false)) {
            new Scenario4(config).run(db);
        }
        if (DbManager.isEnabled("scene5.enabled", false)) {
            new Scenario5(config).run(db);
        }
        if (DbManager.isEnabled("GBbase8s.read.enabled", false)){
            new GBaseRandomReadSecnario(config).run(db);
        }
        if (DbManager.isEnabled("GBbase8s.readAndinsert.enabled", false)){
            new GBaseReadAndInsertSecnario(config).run(db);
        }
    }
}


