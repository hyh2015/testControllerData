package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
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

        // 每次重试间隔30s,文件执行超过时间 6h
        final int CHECK_INTERVAL_S = 30 * 1000; // 30秒
        final int timeOut = 6 * 3600;
        long countTimes = 0;
        String cmd = String.format("filecount=%d;count=$(find %s -type f |wc -l);" +
                        " valid=$(find %s -type f -size +4050M |wc -l); " +
                        "[ \"$count\" -eq \"$filecount\" ] && [ \"$valid\" -eq \"$filecount\" ] && echo OK || echo NOT_OK",
                fileNum, dirPath, dirPath
        );

        System.out.println("开始校验目录：" + dirPath);

        if (countTimes > timeOut) {
            logger.error("生产文件超时，程序退出！！！");
            return false;
        } else {
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
                    } else if ("NOT_OK".equalsIgnoreCase(result)) {
                        Thread.sleep(CHECK_INTERVAL_S);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
    }


    // 生成shell脚本：mock.sh文件 并赋权限 执行
    public static void generateMockTestData(String mockdataJar, String dataPath) throws Exception {
        LocalDate startDate = LocalDate.parse(DbManager.getProperty("mockdata.start_date"));
        int monthCount = Integer.parseInt(DbManager.getProperty("mockdata.months"));

        if (mockdataJar == null || mockdataJar.trim().isEmpty()) {
            throw new IllegalArgumentException("mockdataJarPath 不能为空");
        }

        if (dataPath == null || dataPath.trim().isEmpty()) {
            throw new IllegalArgumentException("dataPath 不能为空");
        }

        // 打印当前工作目录 帮助调试
        logger.info("当前工作目录："+new File(".").getAbsolutePath());

        File scriptFile = new File(mockStr);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(scriptFile))) {

            writer.write("#!/bin/bash\n\n");

            for (int i = 0; i < monthCount; i++) {
                LocalDate current = startDate.plusMonths(i);
                int days = current.lengthOfMonth();
                String dateStr = current.toString();

                List<String> command = Arrays.asList("java","-jar",mockdataJar,
                        " -T"+ dateStr,
                        " -D"+ dataPath,
                        " -N"+days,
                        " &");

                logger.info("准备写入命令：" + String.join(" ",command));

                writer.write(String.join(" ", command));
                writer.write("\n");
            }

        } catch (IOException e){
            logger.error("写入可执行文件：" + mockStr + "失败");
            e.printStackTrace();
        }
        logger.info("mock.sh 脚本生成成功: " + scriptFile.getAbsolutePath());

        // 赋权限给脚本本身
        try{
            new ProcessBuilder("chmod","+x", mockStr).start().waitFor();
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

    }

}
