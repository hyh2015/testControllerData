package org.dbBenchPerfTest.checkDatabase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CheckDatabaseInstall {

    // 目前只支持ivory数据库安装
    private final static String dbName="ivorysql";

    /**
     * 对外提供的公共方法
     * 检查数据库是否已安装，如未安装则自动执行安装
     * @return true 表示数据库已存在或安装成功；false 表示安装失败
     */
    public void checkAndInstallDatabase() {
        boolean installed = checkDatabaseInstalled(dbName);

        if (!installed) {
            System.out.println("未检测到数据库 [" + dbName + "]，开始执行安装流程...");
            DatabaseInstaller installer = new DatabaseInstaller();
            installer.installDatabase(dbName);
        } else {
            System.out.println("数据库 [" + dbName + "] 已安装，无需重复安装。");
        }
    }

/*    public static void main(String[] args) {
        // 如果没传参数，默认检测 ivorysql
        String dbName = args.length > 0 ? args[0] : "ivorysql";
        boolean installed = checkDatabaseInstalled(dbName);

        if (!installed) {
            System.out.println("未检测到数据库 [" + dbName + "]，开始执行安装流程...");
            DatabaseInstaller installer = new DatabaseInstaller();
            installer.installDatabase(dbName);
        } else {
            System.out.println("数据库 [" + dbName + "] 已安装，无需重复安装。");
        }
    }*/

    /**
     * 检查数据库是否已安装
     * @param dbName 数据库包名，如 "ivorysql"
     */
    public static boolean checkDatabaseInstalled(String dbName) {
        String command = String.format("rpm -qa | grep %s", dbName);
        ProcessBuilder builder = new ProcessBuilder("bash", "-c", command);
        builder.redirectErrorStream(true);

        try {
            Process process = builder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            boolean found = false;

            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    System.out.println("检测到安装包: " + line);
                    found = true;
                }
            }

            process.waitFor();
            return found;

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }
}
