package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class Mockdata {
    private static final Logger logger = LoggerFactory.getLogger(Mockdata.class);

    static String mockStr = "mock.sh";

    // 直接调用mock.sh脚本 其中会包含执行jar包命令来执行生成数据
    public static boolean runMockScript() {
        List<String> command = new ArrayList<>();
        command.add("/bin/bash");
        command.add("mock.sh"); // 脚本需与 jar 同级，或者写绝对路径

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true); // 合并 stderr 和 stdout
        processBuilder.inheritIO(); // 让脚本的输出显示在控制台

        logger.info("开始执行生成测试数据脚本，执行命令：" + String.join(" ", command));

        try {
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            return exitCode == 0;
        } catch (IOException | InterruptedException e) {
            logger.error("执行 " + mockStr + "失败");
            e.printStackTrace();
            return false;
        }

    }


    // 校验生成测试文件是否成功
    public static boolean waitForValidFiles(int fileNum, String dirPath) {

        // 每次重试间隔（毫秒）
        final int CHECK_INTERVAL_S = 30 * 1000; // 30秒
        String cmd = String.format("expected=%d; \n" +
                        "count=$(find %s -type f | wc -l); \n" +
                        "valid=$(find %s -type f -size +4100M | wc -l); \n" +
                        "[ \"$count\" -eq \"$expected\" ] && [ \"$valid\" -eq \"$expected\" ] && echo OK || echo NOT_OK\n",
                fileNum, dirPath, dirPath
        );

        System.out.println("开始校验目录：" + dirPath);

        while (true) {
            try {
                ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
                pb.redirectErrorStream(true);
                Process process = pb.start();

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream()));
                String result = reader.readLine();
                process.waitFor();

                if ("OK".equals(result)) {
                    logger.info("校验通过，完成测试数据文件的生成");
                    return true;
                } else {
//                    logger.info("校验未通过，等待 " + (CHECK_INTERVAL_MS / 1000) + " 秒后重试...");
                    Thread.sleep(CHECK_INTERVAL_S);
                }

            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

    }

   /* // 调用mockdata.jar生成数据
    public static void generateMockTestData(String mockdataJar, String dataPath) throws Exception {
        LocalDate startDate = LocalDate.parse(DbManager.getProperty("mockdata.start_date"));
        int monthCount = Integer.parseInt(DbManager.getProperty("mockdata.months"));

        // 检查参数是否为空
        if (mockdataJar == null || mockdataJar.trim().isEmpty()) {
            throw new IllegalArgumentException("mockdataJarPath 不能为空");
        }

        if (dataPath == null || dataPath.trim().isEmpty()) {
            throw new IllegalArgumentException("dataPath 不能为空");
        }

        // 打印当前工作目录，帮助调试
        System.out.println("当前工作目录: " + new File(".").getAbsolutePath());

        for (int i = 0; i < monthCount; i++) {
            LocalDate current = startDate.plusMonths(i);
            int days = current.lengthOfMonth();
            String dateStr = current.toString();

            // 构造命令
            List<String> command = new ArrayList<>();
            command.add("java");
            command.add("-jar");
            command.add(mockdataJar);
            command.add("-T" + dateStr);
            command.add("-D" + dataPath);
            command.add("-N" + days);

            System.out.println("执行命令：" + String.join(" ", command));

*//*            try {
                ProcessBuilder processBuilder = new ProcessBuilder(command);
                processBuilder.redirectErrorStream(true); // 合并标准错误输出和标准输出

                Process process = processBuilder.start();

                // 读取子进程的输出
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[mockdata] " + line);
                }

                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    System.err.println("mockdata.jar 执行失败，退出码：" + exitCode);
                } else {
                    System.out.println("mockdata.jar 执行成功");
                }

            } catch (Exception e) {
                System.err.println("启动 mockdata.jar 失败: " + e.getMessage());
                e.printStackTrace();
            }

            Thread.sleep(1000); // 间隔 1 秒*//*
        }

        System.out.println("测试数据预处理生成完成");
    }*/


    // 生成shell脚本：mock.sh文件 其中包含执行mockdata.jar程序
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

}
