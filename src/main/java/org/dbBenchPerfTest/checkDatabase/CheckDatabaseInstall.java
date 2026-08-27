package org.dbBenchPerfTest.checkDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CheckDatabaseInstall {

    private static final Logger logger = LoggerFactory.getLogger(CheckDatabaseInstall.class);

    // 目前只支持ivory数据库安装
    private final static String ivorysql="ivorysql";

    /**
     * 对外提供的公共方法
     * 检查数据库是否已安装，如未安装则自动执行安装。
     *
     * <p>安装失败会直接抛出异常终止整个程序 —— 带着没装成的库继续跑，
     * 最终只会在建立数据库连接时报「无法获取数据库连接」，与真正的原因相距很远。
     *
     * @throws IllegalStateException 安装检测失败，或安装过程未成功
     */
    public void checkAndInstallDatabase() {
        boolean installed = checkDatabaseInstalled(ivorysql);

        if (!installed) {
            logger.info("==============未检测到数据库 [{}]，开始执行安装流程...==============", ivorysql);
            DatabaseInstaller installer = new DatabaseInstaller();
            installer.installDatabase(ivorysql);
        } else {
            logger.info("==============数据库 [{}] 已安装，无需重复安装==============", ivorysql);
        }
    }


    /**
     * 检查数据库是否已安装。
     *
     * <p>注意判断依据是包名前缀匹配，只要装过任意版本的该包就算已安装 ——
     * 要换版本必须先手工 rpm -e 卸载干净，否则会被直接跳过。
     *
     * @param dbName 数据库包名，如 "ivorysql"
     * @return true 表示已安装
     * @throws IllegalStateException 检测命令本身执行失败（此时无法判定是否已安装，
     *                               不能当作「未安装」继续往下装）
     */
    public static boolean checkDatabaseInstalled(String dbName) {
        String command = String.format("rpm -qa | grep %s", dbName);
        ProcessBuilder builder = new ProcessBuilder("bash", "-c", command);
        builder.redirectErrorStream(true);

        try {
            Process process = builder.start();
            boolean found = false;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        logger.info("检测到已安装的包: {}", line);
                        found = true;
                    }
                }
            }

            process.waitFor();
            return found;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("检测数据库 [" + dbName + "] 是否已安装时被中断。", e);
        } catch (IOException e) {
            throw new IllegalStateException("检测数据库 [" + dbName + "] 是否已安装失败，"
                    + "无法确认安装状态：" + e.getMessage(), e);
        }
    }
}
