package org.dbBenchPerfTest.checkDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.DbManager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class DatabaseInstaller {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseInstaller.class);

    private static final String X86_PACKAGE = "IvorySQL-4.5-0ffca11-20250527.x86_64.rpm";
    private static final String ARM_PACKAGE = "IvorySQL-4.5-0ffca11-20250527.aarch64.rpm";

    String scriptPath = DbManager.getProperty("scriptPath");


    public void installDatabase(String dbName){

        try{
            String archName = checkSystemArch();
            String rpmPackageName = "";
            logger.info("检测到当前操作系统架构为：" + archName);

            if (archName.contains("x86_64")){
                rpmPackageName = X86_PACKAGE;
            }else if (archName.contains("aarch64")){
                rpmPackageName = ARM_PACKAGE;
            }else {
                logger.error("未知架构，无法确定安装包！");
            }

            // 修改安装脚本中对应架构的数据库名后返回脚本名
            File scriptFile = updateSetupScript(rpmPackageName);

            String installCommand = scriptFile.getAbsolutePath() + " -o install -t common -u mycat";

            logger.info("---------------["+installCommand+"]------------------");

            boolean successed = execInstallCommand(installCommand);

            boolean checkIsRunning = checkDatabaseStatus();

            if (successed && checkIsRunning) {
                System.out.println("============================数据库：["+dbName+"]安装成功，状态正常。==========================");
            } else {
                System.out.println("安装失败，请检查脚本输出或日志。");
            }



        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("安装过程出现异常。");
        }


    }


    /**
     * 检查 IvorySQL 数据库是否正常运行
     * @return true 表示数据库运行正常，false 表示异常或未启动
     */
    public boolean checkDatabaseStatus() {
        try {
            logger.info("开始检查 IvorySQL 数据库状态...");

            //检查进程是否存在
            if (!execShellCheck("ps -ef | grep ivorysql | grep -v grep")) {
                logger.warn("未检测到 ivorysql 进程，数据库可能未启动！");
                return false;
            }

            // 检查端口监听
            if (!execShellCheck("ss -lntp | grep 5966")) {
                logger.warn("未检测到端口 5966 监听，数据库可能未启动！");
                return false;
            }

            logger.info("IvorySQL 数据库运行正常！");
            return true;

        } catch (Exception e) {
            logger.error("检查数据库状态时发生异常: ", e);
            return false;
        }
    }

    /**
     * 执行命令并检查是否有输出
     */
    private boolean execShellCheck(String command) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder("bash", "-c", command);
        builder.redirectErrorStream(true);
        Process process = builder.start();

        boolean hasOutput = false;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logger.info("[状态检测输出] " + line);
                if (!line.trim().isEmpty()) {
                    hasOutput = true;
                }
            }
        }

        int exitCode = process.waitFor();
        logger.info("命令退出码: " + exitCode);
        return hasOutput && exitCode == 0;
    }


    /**
     * 直接修改 setupivory.sh 文件中 g_database_rpm_file 的值
     * 不再创建临时文件。
     */
    private File updateSetupScript(String rpmFileName) throws IOException {
        File scriptFile = new File(scriptPath, "setupivory.sh");

        if (!scriptFile.exists()) {
            throw new FileNotFoundException("找不到 setupivory.sh 脚本！路径: " + scriptFile.getAbsolutePath());
        }

        logger.info("准备修改脚本 [{}] 中的 rpm 包名为: {}", scriptFile.getAbsolutePath(), rpmFileName);

        // 读取整个文件
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(scriptFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("g_database_rpm_file=")) {
                    // 替换为新的包名
                    line = "g_database_rpm_file=\"" + rpmFileName + "\"";
                }
                content.append(line).append(System.lineSeparator());
            }
        }

        // 写回文件（覆盖原内容）
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(scriptFile, false))) {
            writer.write(content.toString());
        }

        // 确保脚本可执行
        scriptFile.setExecutable(true);

        logger.info("setupivory.sh 文件已更新完毕！");
        return scriptFile;
    }


    /**
     * 检查系统架构
     * @return 架构类型
     * @throws IOException
     * @throws InterruptedException
     */
    private String checkSystemArch() throws IOException, InterruptedException {

        ProcessBuilder pbuilder = new ProcessBuilder("bash","-c","uname -m");
        pbuilder.redirectErrorStream(true);
        Process process = pbuilder.start();


        try(BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))){
            String arch = reader.readLine();
            process.waitFor();
            return arch == null ? "未知架构" : arch.trim();
        }



    }


    /**
     * 执行安装脚本并判断是否成功
     * @param command  安装数据库命令
     * @return
     */
    private boolean execInstallCommand(String command) throws IOException, InterruptedException {
        logger.info("执行安装命令: {}", command);

        ProcessBuilder builder = new ProcessBuilder("bash", "-c", command);
        builder.directory(new File(scriptPath));
        builder.redirectErrorStream(true);

        Process process = builder.start();

        boolean success = false;
        StringBuilder output = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logger.info("[安装输出] {}", line);
                output.append(line).append("\n");
                if (line.contains("ivory database: common install complete")) {
                    success = true;
                }
            }
        }

        // 设置超时等待
        boolean finished = process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS);
        if (!finished) {
            logger.warn("安装脚本可能未正常结束，强制终止进程。");
            process.destroyForcibly();
        }

        int exitCode = process.exitValue();
        logger.info("命令退出码: {}", exitCode);
        return success;
    }




}
