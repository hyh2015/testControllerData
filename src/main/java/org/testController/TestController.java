package org.testController;

public class TestController {

    private final SceneExecutor sceneExecutor;

    public TestController(String dbType) throws Exception {
        this.sceneExecutor = new SceneExecutor(dbType);
    }

    public void runAllTests() throws Exception {
//        String mode = "1";
/*        // 生成测试数据
        sceneExecutor.generateMockTestData();
        sceneExecutor.createPartitionTable();
        // copy 批量入库+创建索引
        sceneExecutor.runScenario1();
        // distinc计算
        sceneExecutor.runScenario2();
        // 并发随机读
        sceneExecutor.runScenario3(mode);
        // 逐条入库
        sceneExecutor.runScenario4();
        // 并发随机读+逐条入库
        sceneExecutor.runScenario5();*/

//        生成测试数据
        if (DbManager.isEnabled("scene.mock.enabled")) {
            sceneExecutor.generateMockTestData();
        }
//        创建分区表
        if (DbManager.isEnabled("scene.createPartition.enabled")) {
            sceneExecutor.createPartitionTable();
        }

//        场景1：copy 批量入库+创建索引
        if (DbManager.isEnabled("scene1.enabled")) {
            sceneExecutor.runScenario1();
        }

//        场景2： distinc计算
        if (DbManager.isEnabled("scene2.enabled")) {
            sceneExecutor.runScenario2();
        }

//        场景3： 并发随机读
        if (DbManager.isEnabled("scene3.enabled")) {
            sceneExecutor.runScenario3();
        }

//        场景4： 逐条入库
        if (DbManager.isEnabled("scene4.enabled")) {
            sceneExecutor.runScenario4();
        }

//        场景5： 并发随机读+逐条入库
        if (DbManager.isEnabled("scene5.enabled")) {
            sceneExecutor.runScenario5();
        }
    }
}
