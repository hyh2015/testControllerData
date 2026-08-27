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
         * 非 MegaRAID 机器（无 storcli）可将 hardware.check.enabled 置为 false 跳过
         */
        // 缺失时按需要检查处理：漏配置默认去做硬盘检查，比默默跳过更安全。
        if (DbManager.isEnabled("hardware.check.enabled", true)) {
            if (!new CheckHardware().checkStorageHealth()) {
                logger.error("由于服务器硬盘状态异常，为保证压测数据准确性，程序已经自动终止。");
                System.exit(1); //强制退出
            }
        } else {
            logger.warn("已配置 hardware.check.enabled=false，跳过硬盘预检。"
                    + "磁盘若存在坏盘或 Raid 降级将无法被发现，压测数据可能失真。");
        }

        logger.info("==== 开始测试数据库: " + dbType + " ====\n");
//        TestController controller = new TestController(dbType);
        TestControllerNew controller = new TestControllerNew(new TestConfig(dbType));
        controller.runAllTests();
        logger.info("==== 完成数据库: " + dbType + " ====\n");

    }

}

