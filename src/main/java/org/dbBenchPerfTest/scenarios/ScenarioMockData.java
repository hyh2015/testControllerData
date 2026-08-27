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

    /**
     * 生成压测用的数据文件。
     *
     * <p>任一步失败都直接抛出终止整轮压测：后续场景（建分区表、场景 1 批量入库）
     * 会直接读 data.path 下的文件，数据缺失或不完整时它们不会报错，只会把
     * 「入库了 0 个文件」当成正常结果跑完 —— 拿到的指标毫无意义且不易察觉。
     *
     * @throws IllegalStateException mock.sh 启动失败，或数据文件最终未通过校验
     */
    @Override
    public void run(DatabaseInface db) throws Exception {
        logger.info("[预处理] 生成测试数据...");
        Mockdata.generateMockTestData(config.getMockdataJar(), config.getDataPath());

        // mock.sh 里每条命令都以 & 结尾，脚本把 mockdata.months 个 java 进程拉起来就立即退出。
        // 所以这里为 false 只意味着「连启动都没成功」，真正的产物校验在下面那步。
        if (!Mockdata.runMockScript()) {
            throw new IllegalStateException("测试数据生成失败：mock.sh 未能正常执行。"
                    + "请检查 mockdata.jar 是否与主程序在同一目录、data.path 是否存在且可写。");
        }

        // 每 30s 轮询一次，直到数据文件齐备为止，不设超时上限（造数据本身就很慢）。
        // 返回 false 说明校验命令本身出错了，不是「还没造完」。
        if (!Mockdata.waitForVaildFiles(config.getFileNum(), config.getDirectoryPath())) {
            throw new IllegalStateException("测试数据校验失败：目录 " + config.getDirectoryPath()
                    + " 下未能确认存在 " + config.getFileNum() + " 个大于 4080MB 的数据文件，"
                    + "校验命令执行异常，请检查该目录是否可访问。");
        }

        logger.info("测试数据生成成功");
    }
}

