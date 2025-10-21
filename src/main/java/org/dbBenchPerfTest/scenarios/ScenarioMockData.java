package org.dbBenchPerfTest.scenarios;

import org.dbBenchPerfTest.inface.DatabaseInface;
import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.inface.Scenario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.Mockdata;

public class ScenarioMockData implements Scenario {
    private static final Logger logger = LoggerFactory.getLogger(ScenarioMockData.class);


    private final TestConfig config;

    public ScenarioMockData(TestConfig config) {
        this.config = config;
    }

    @Override
    public void run(DatabaseInface db) throws Exception {
        logger.info("[预处理] 生成测试数据...");
        Mockdata.generateMockTestData(config.getMockdataJar(), config.getDataPath());
        if (Mockdata.runMockScript() && Mockdata.waitForValidFiles(config.getFileNum(), config.getDirectoryPath())) {
            System.out.println("测试数据生成成功");
        } else {
            System.err.println("测试数据生成失败");
        }
    }
}
