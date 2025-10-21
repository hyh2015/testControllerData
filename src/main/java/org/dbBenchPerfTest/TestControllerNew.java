package org.dbBenchPerfTest;

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
        if (DbManager.isEnabled("scene.mock.enabled")) {
            new ScenarioMockData(config).run(db);
        }
        if (DbManager.isEnabled("scene.createPartition.enabled")) {
            db.createPartitionTable();
        }
        if (DbManager.isEnabled("scene1.enabled")) {
            new Scenario1(config).run(db);
        }
        if (DbManager.isEnabled("scene2.enabled")) {
            new Scenario2(config).run(db);
        }
        if (DbManager.isEnabled("scene3.enabled")) {
            new Scenario3(config).run(db);
        }
        if (DbManager.isEnabled("scene4.enabled")) {
            new Scenario4(config).run(db);
        }
        if (DbManager.isEnabled("iscene5.enabled")) {
            new Scenario5(config).run(db);
        }
    }
}
