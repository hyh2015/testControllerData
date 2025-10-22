package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class JavaProcessExecutor {

    private static final Logger logger = LoggerFactory.getLogger(JavaProcessExecutor.class);

    /**
     * 通用执行java -jar命令方法
     *
     * @param jarFileName    jar执行包文件
     * @param configFileName 配置文件
     * @param timeoutHours   超时时间（单位小时，<=0 表示不设置超时）
     * @param logFileName    输出日志文件名（可为 null 表示不写日志）
     * @return
     */
    public static void executeJavaProcess(String jarFileName, String configFileName, int timeoutHours, String logFileName) {
        List<String> commandList = buildJavaCommand(jarFileName, configFileName, timeoutHours);
        ProcessBuilder builder = new ProcessBuilder(commandList);
        builder.redirectErrorStream(true);

        if (logFileName != null && !logFileName.trim().isEmpty()) {
            File logFile = new File(logFileName);
            builder.redirectOutput(logFile);
        }
/*        if (!jarFileName.equalsIgnoreCase("tableMigration.jar")){
            if (logFileName != null && !logFileName.trim().isEmpty()) {
                File logFile = new File(logFileName);
                builder.redirectOutput(logFile);
            }
        }*/

        Process process = null;
        int exitCode = -1;

        try {
            process = builder.start();
            exitCode = process.waitFor();
            if (exitCode == 124) {
                logger.warn("jar 包因超时被 timeout 强制终止");
            } else {
                logger.info("java程序：" + jarFileName + "正常退出，退出码：" + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            logger.error("启动或执行java程序失败：" + jarFileName + "", e);
        }
    }


    /**
     * 构建 Java 命令，可选支持 timeout
     *
     * @param jarFileName    要执行的 jar 包名
     * @param configFileName 配置文件名（可为 null）
     * @param timeoutHours   超时时间（单位：小时，<=0 表示不使用 timeout）
     * @return 构建好的命令 List
     */
    public static List<String> buildJavaCommand(String jarFileName, String configFileName, int timeoutHours) {
        List<String> command = new ArrayList<>();

        if (timeoutHours > 0) {
            int timeoutSeconds = timeoutHours * 3600;
            command.add("timeout");
            command.add(String.valueOf(timeoutSeconds));
        }

        command.add("java");
        command.add("-jar");
        command.add(jarFileName);

        if (configFileName != null && !configFileName.trim().isEmpty()) {
            command.add(configFileName);
        }

        logger.info("执行命令：" + String.join(" ",command));
        return command;
    }


 /*   public static void main(String[] args) {

        String logFile = "insert_log_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";

    }*/
}

