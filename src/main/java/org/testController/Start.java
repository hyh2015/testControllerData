package org.testController;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.TestControllerNew;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Start {

    public static void main(String[] args) throws Exception {

        final Logger logger = LoggerFactory.getLogger(Start.class);

        String dbType = DbManager.getProperty("db.type");
        logger.info("==== 开始测试数据库: " + dbType + " ====\n");
//        TestController controller = new TestController(dbType);
        TestControllerNew controller = new TestControllerNew(new TestConfig(dbType));
        controller.runAllTests();
        logger.info("==== 完成数据库: " + dbType + " ====\n");
    }

}
