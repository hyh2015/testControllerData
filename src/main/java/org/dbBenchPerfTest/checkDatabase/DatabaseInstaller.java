package org.dbBenchPerfTest.checkDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testController.DbManager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class DatabaseInstaller {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseInstaller.class);

/*
    private static final String X86_PACKAGE = "IvorySQL-4.5-0ffca11-20250527.x86_64.rpm";
    private static final String ARM_PACKAGE = "IvorySQL-4.5-0ffca11-20250527.aarch64.rpm";
*/

    String scriptPath = DbManager.getProperty("scriptPath");
    static final String X86_PACKAGE = DbManager.getProperty("X86_PACKAGE");
    static final String ARM_PACKAGE = DbManager.getProperty("ARM_PACKAGE");

    /**
     * 安装数据库。任何一步不成立都直接抛出，不允许带着「没装成的库」继续跑压测 ——
     * 否则真正的失败原因会被后续「无法获取数据库连接」掩盖，排查时容易误判成网络或配置问题。
     *
     * @param dbName 数据库名，仅用于日志
     * @throws IllegalStateException 架构无法识别、包名未配置、脚本改写失败、
     *                               安装未成功或安装后状态检查不通过
     */
    public void installDatabase(String dbName) {

        String archName;
        try {
            archName = checkSystemArch();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("检测系统架构时被中断。", e);
        } catch (IOException e) {
            throw new IllegalStateException("检测系统架构失败，无法确定安装包。", e);
        }
        logger.info("检测到当前操作系统架构为：{}", archName);

        String rpmPackageName;
        if (archName.contains("x86_64")) {
            rpmPackageName = X86_PACKAGE;
        } else if (archName.contains("aarch64")) {
            rpmPackageName = ARM_PACKAGE;
        } else {
            throw new IllegalStateException(
                    "未知的系统架构 [" + archName + "]，无法确定安装包，目前仅支持 x86_64 与 aarch64。");
        }

        // 包名为空时会被原样写进 setupivory.sh，既改坏脚本又必然安装失败，
        // 所以要在改写脚本之前拦住。
        if (rpmPackageName == null || rpmPackageName.trim().isEmpty()) {
            throw new IllegalStateException("架构 [" + archName + "] 对应的安装包名未配置，"
                    + "请检查 allconf.properties 中的 X86_PACKAGE / ARM_PACKAGE。");
        }

        // 修改安装脚本中对应架构的数据库名后返回脚本名
        File scriptFile;
        try {
            scriptFile = updateSetupScript(rpmPackageName);
        } catch (IOException e) {
            throw new IllegalStateException("改写安装脚本 setupivory.sh 失败：" + e.getMessage(), e);
        }

        String installCommand = scriptFile.getAbsolutePath() + " -o install -t common -u mycat";

        logger.info("---------------[{}]------------------", installCommand);

        boolean successed;
        try {
            successed = execInstallCommand(installCommand);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("执行安装脚本时被中断。", e);
        } catch (IOException e) {
            throw new IllegalStateException("执行安装脚本失败：" + e.getMessage(), e);
        }

        if (!successed) {
            throw new IllegalStateException("数据库 [" + dbName + "] 安装失败："
                    + "安装脚本输出中未出现成功标记，请检查上方 [安装输出] 日志。");
        }

        if (!checkDatabaseStatus()) {
            throw new IllegalStateException("数据库 [" + dbName + "] 安装脚本已执行完成，"
                    + "但状态检查未通过（ivorysql 进程或端口 " + instancePort() + " 未就绪），"
                    + "请确认数据库是否正常启动，以及 allconf.properties 里的端口与安装脚本是否一致。");
        }

        logger.info("============================数据库：[{}]安装成功，状态正常。==========================", dbName);
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

            // 检查端口监听。端口取自 allconf.properties 的 {db.type}.port，
            // 不能写死 —— 不同版本的 setupivory.sh 默认端口不一样（5.3 是 5966，
            // 5.4 的脚本里已经变了），写死会在装成功之后误报「数据库未启动」。
            String port = instancePort();
            if (!execShellCheck("ss -lntp | grep " + port)) {
                logger.warn("未检测到端口 {} 监听，数据库可能未启动！"
                        + "若数据库实际在其它端口上，请把 allconf.properties 里的 {}.port 改成与安装脚本一致。",
                        port, DbManager.getProperty("db.type"));
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
     * 取当前数据库实例的端口，用于安装后的监听检查。
     *
     * <p>取的是 allconf.properties 里的 {db.type}.port —— 装库只在 db.type=ivory 时执行
     * （见 Start），所以实际读的是 ivory.port，与 JDBC 连接用的是同一个值，不会两处打架。
     */
    private String instancePort() {
        String dbType = DbManager.getProperty("db.type");
        String port = DbManager.getProperty(dbType + ".port");
        if (port == null || port.trim().isEmpty()) {
            throw new IllegalStateException("未配置 " + dbType + ".port，无法检查数据库监听端口。");
        }
        return port.trim();
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
    /** 安装脚本里存放安装包文件名的变量。不同版本的 setupivory.sh 用的名字不一样：
     *  rpm 版脚本是 g_database_rpm_file，deb 版（5.4 起）改成了 g_database_deb_file。 */
    private static final String[] PACKAGE_VARS = {"g_database_rpm_file=", "g_database_deb_file="};

    /** 存放数据目录的变量，目前各版本一致。 */
    private static final String DATA_VAR = "g_database_data=";

    /**
     * 把 allconf.properties 里配置的安装包名和数据目录写进 setupivory.sh。
     *
     * <p>脚本里找不到对应变量时会直接抛错，而不是「改了个寂寞」——
     * 曾经因为 5.4 版脚本把 g_database_rpm_file 改名成 g_database_deb_file，
     * 这里静默地什么都没改，脚本用了自己硬编码的默认包名，排查时极难发现。
     *
     * @param packageFileName 按当前架构选出的安装包文件名
     * @throws IllegalStateException 脚本中找不到存放包名或数据目录的变量
     */
    private File updateSetupScript(String packageFileName) throws IOException {
        File scriptFile = new File(scriptPath, "setupivory.sh");
        String mountPath = DbManager.getProperty("mount.path");

        if (!scriptFile.exists()) {
            throw new FileNotFoundException("找不到 setupivory.sh 脚本！路径: " + scriptFile.getAbsolutePath());
        }

        logger.info("准备修改脚本 [{}] 中的安装包名为: {}，ivory数据库的数据目录为：{}",
                scriptFile.getAbsolutePath(), packageFileName, mountPath);

        String matchedPackageVar = null;
        boolean dataVarMatched = false;

        // 读取整个文件
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(scriptFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String packageVar = matchPackageVar(line);
                if (packageVar != null) {
                    // 替换为新的包名
                    line = packageVar + "\"" + packageFileName + "\"";
                    matchedPackageVar = packageVar;
                } else if (line.startsWith(DATA_VAR)) {
                    // 更新数据库的数据目录
                    line = DATA_VAR + "\"" + mountPath + "\"";
                    dataVarMatched = true;
                }
                content.append(line).append(System.lineSeparator());
            }
        }

        if (matchedPackageVar == null) {
            throw new IllegalStateException("安装脚本 " + scriptFile.getAbsolutePath()
                    + " 中找不到存放安装包名的变量（试过 " + String.join(" / ", PACKAGE_VARS) + "）。"
                    + "allconf.properties 里配置的包名不会生效，请确认脚本版本与本程序是否匹配。");
        }
        if (!dataVarMatched) {
            throw new IllegalStateException("安装脚本 " + scriptFile.getAbsolutePath()
                    + " 中找不到变量 " + DATA_VAR + "，mount.path 配置不会生效，"
                    + "数据目录可能落到脚本默认路径（未必在挂载盘上），请确认脚本版本。");
        }

        // 写回文件（覆盖原内容）
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(scriptFile, false))) {
            writer.write(content.toString());
        }

        // 确保脚本可执行
        scriptFile.setExecutable(true);

        logger.info("setupivory.sh 文件已更新完毕！包名写入变量 {}", matchedPackageVar);
        return scriptFile;
    }

    /** 返回该行匹配上的包名变量前缀，没匹配上返回 null。 */
    private static String matchPackageVar(String line) {
        for (String var : PACKAGE_VARS) {
            if (line.startsWith(var)) {
                return var;
            }
        }
        return null;
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

