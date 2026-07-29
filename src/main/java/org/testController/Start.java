package org.testController;

import org.dbBenchPerfTest.TestConfig;
import org.dbBenchPerfTest.TestControllerNew;
import org.dbBenchPerfTest.checkDatabase.CheckDatabaseInstall;
import org.dbBenchPerfTest.checkDatabase.CheckHardware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Start {

    private static final Logger logger = LoggerFactory.getLogger(Start.class);

    public static void main(String[] args) throws Exception {

        final Logger logger = LoggerFactory.getLogger(Start.class);
        String dbType = DbManager.getProperty("db.type");


        /**
         * 判断是否安装ivorysql数据库
         */
        if(dbType.equalsIgnoreCase("ivory") && DbManager.isEnabled("is.install.ivory")){
            new CheckDatabaseInstall().checkAndInstallDatabase();
        } else if (!dbType.equalsIgnoreCase("ivory") && DbManager.isEnabled("is.install.ivory")) {
            logger.error("只支持安装 ivory 数据库，暂不支持安装其他类型数据库");
        }

        /**
         * 判断服务器硬盘是否正常
         */
        if (!new CheckHardware().checkStorageHealth()) {
            logger.error("由于服务器硬盘状态异常，为保证压测数据准确性，程序已经自动终止。");
            System.exit(1); //强制退出
        } else {

            logger.info("==== 开始测试数据库: " + dbType + " ====\n");
//        TestController controller = new TestController(dbType);
            TestControllerNew controller = new TestControllerNew(new TestConfig(dbType));
            controller.runAllTests();
            logger.info("==== 完成数据库: " + dbType + " ====\n");
        }

    }

}

