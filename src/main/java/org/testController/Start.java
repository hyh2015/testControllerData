package org.testController;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.TestControllerNew;
import org.dbBenchPerfTest.checkDatabase.CheckDatabaseInstall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Start {

    public static void main(String[] args) throws Exception {

        final Logger logger = LoggerFactory.getLogger(Start.class);

        String dbType = DbManager.getProperty("db.type");

        if (dbType.equalsIgnoreCase("ivory")
                && DbManager.isEnabled("is.install.ivory")) {
            new CheckDatabaseInstall().checkAndInstallDatabase();
        } else if(DbManager.isEnabled("is.install.ivory") &&
                !dbType.equalsIgnoreCase("ivory")){
            logger.error("只支持"+dbType+"数据库,暂不支持安装其他类型数据库");
        }

        logger.info("==== 开始测试数据库: " + dbType + " ====\n");
//        TestController controller = new TestController(dbType);
        TestControllerNew controller = new TestControllerNew(new TestConfig(dbType));
        controller.runAllTests();
        logger.info("==== 完成数据库: " + dbType + " ====\n");
    }

}
