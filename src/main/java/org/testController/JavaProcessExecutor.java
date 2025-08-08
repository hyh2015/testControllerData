package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;

public class JavaProcessExecutor {

    private static final Logger logger = LoggerFactory.getLogger(JavaProcessExecutor.class);

    /**
     * 通用执行java -jar命令方法
     * @param jarFileName    jar执行包文件
     * @param configFileName 配置文件
     * @param timeoutHours  超时时间（单位小时，<=0 表示不设置超时）
     * @param logFileName  输出日志文件名（可为 null 表示不写日志）
     * @return
     */
    public static void executeJavaProcess(String jarFileName, String configFileName, int timeoutHours,String logFileName) {
        List<String> commandList = buildJavaCommand(jarFileName, configFileName, timeoutHours);
        ProcessBuilder builder = new ProcessBuilder(commandList);
        builder.redirectErrorStream(true);

        if (logFileName != null && !logFileName.trim().isEmpty()) {
            File logFile = new File(logFileName);
            builder.redirectOutput(logFile);
        }

        Process process = null;
        int exitCode = -1;

        try {
            process = builder.start();
            exitCode = process.waitFor();
            if (exitCode == 124) {
                logger.warn("jar 包因超时被 timeout 强制终止");
            } else {
                logger.info("java程序："+jarFileName+"正常退出，退出码：" + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            logger.error("启动或执行java程序失败：" + jarFileName + "", e);
        }
    }



    /**
     * 构建 Java 命令，可选支持 timeout
     * @param jarFileName 要执行的 jar 包名
     * @param configFileName 配置文件名（可为 null）
     * @param timeoutHours 超时时间（单位：小时，<=0 表示不使用 timeout）
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

        return command;
    }




    public static void generateMockTestData(String mockdataJar, String dataPath) throws Exception {
        LocalDate startDate = LocalDate.parse(DbManager.getProperty("mockdata.start_date"));
        int monthCount = Integer.parseInt(DbManager.getProperty("mockdata.months"));

        if (mockdataJar == null || mockdataJar.trim().isEmpty()) {
            throw new IllegalArgumentException("mockdataJarPath 不能为空");
        }

        if (dataPath == null || dataPath.trim().isEmpty()) {
            throw new IllegalArgumentException("dataPath 不能为空");
        }

        File scriptFile = new File("mock.sh");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(scriptFile))) {

            writer.write("#!/bin/bash\n\n");

            for (int i = 0; i < monthCount; i++) {
                LocalDate current = startDate.plusMonths(i);
                int days = current.lengthOfMonth();
                String dateStr = current.toString();

                List<String> command = new ArrayList<>();
                command.add("java");
                command.add("-jar");
                command.add(mockdataJar);
                command.add("-T" + dateStr);
                command.add("-D" + dataPath);
                command.add("-N" + days);

                writer.write(String.join(" ", command));
                writer.write("\n");
            }

           logger.info("mock.sh 脚本生成成功: " + scriptFile.getAbsolutePath());
        }
    }


    public static void main(String[] args) {

//        String logFile = "insert_log_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";

        try {
//            generateMockTestData("mockdata.jar","/storeone/mockdata/datafile1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
