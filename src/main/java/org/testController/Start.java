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
        // 缺失时按不安装处理：漏配置不该意外触发一次没人要求的装库。
        if (DbManager.isEnabled("is.install.ivory", false)) {
            if (!dbType.equalsIgnoreCase("ivory")) {
                throw new IllegalStateException(
                        "只支持安装 ivory 数据库，暂不支持安装其他类型数据库。当前 db.type=" + dbType);
            }
            new CheckDatabaseInstall().checkAndInstallDatabase();
        }

        /**
         * 判断服务器硬盘是否正常
         * 需要 MegaRAID 机器（有 storcli）才能跑，显式将 hardware.check.enabled 置为 true 开启；
         * 缺省不检查 —— 实际部署的测试服务器大多没有 storcli，显式开关比强制人人都要关一遍更省事。
         */
        if (DbManager.isEnabled("hardware.check.enabled", false)) {
            if (!new CheckHardware().checkStorageHealth()) {
                logger.error("由于服务器硬盘状态异常，为保证压测数据准确性，程序已经自动终止。");
                System.exit(1); //强制退出
            }
        } else {
            logger.warn("未开启 hardware.check.enabled（缺省即不检查，需要检查请显式置为 true）。"
                    + "磁盘若存在坏盘或 Raid 降级将无法被发现，压测数据可能失真。");
        }

        logger.info("==== 开始测试数据库: " + dbType + " ====\n");
//        TestController controller = new TestController(dbType);
        TestControllerNew controller = new TestControllerNew(new TestConfig(dbType));
        controller.runAllTests();
        logger.info("==== 完成数据库: " + dbType + " ====\n");

    }

}

