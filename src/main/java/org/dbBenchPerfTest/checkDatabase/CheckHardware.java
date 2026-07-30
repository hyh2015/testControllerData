package org.dbBenchPerfTest.checkDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class CheckHardware {

    private static final Logger logger = LoggerFactory.getLogger(CheckHardware.class);

    private static final String STORCLI_PATH = "/opt/MegaRAID/storcli/storcli64";


    /**
     * 压测前的磁盘/RAID 健康预检。
     *
     * 注意：storcli 无法执行（未安装、路径不对、无权限、退出码非 0）时一律返回 false。
     * 「没检成」不等于「检查通过」—— 无法确认磁盘状态时不允许开始压测，
     * 否则压测数据不具备参考价值。
     *
     * @return true 磁盘状态正常，可以开始压测；false 检出异常或无法完成检查
     */
    public boolean checkStorageHealth() {
        logger.info(">>> [硬盘预检] 开始获取服务器存储环境信息...");

        if (!new File(STORCLI_PATH).canExecute()) {
            logger.error("!!! [硬盘预检] 未找到可执行的 storcli：" + STORCLI_PATH
                    + "，无法确认磁盘状态，预检不通过。请安装 storcli 或确认该路径有执行权限。");
            return false;
        }

        String controllerInfo;
        String vdInfo;
        String pdInfo;
        try {
            // 1. 控制器基本信息
            controllerInfo = execute(STORCLI_PATH, "/c0", "show");
            // 2. 逻辑盘（Raid）详细信息-检查WB/WT 和降级状态
            vdInfo = execute(STORCLI_PATH, "/c0", "/vall", "show");
            // 3. 物理盘详细信息
            pdInfo = execute(STORCLI_PATH, "/c0", "/eall", "/sall", "show");
        } catch (Exception e) {
            logger.error("!!! [硬盘预检] storcli 执行失败，无法确认磁盘状态，预检不通过：" + e.getMessage(), e);
            return false;
        }

        logHardwareDetail("控制器摘要：", controllerInfo);
        logHardwareDetail("逻辑卷（VD）状态", vdInfo);
        logHardwareDetail("物理硬盘（PD）状态", pdInfo);

        // 4.自动化判断逻辑：截断输出末尾的状态码图例，避免图例中的关键字造成误判
        String outputVD = vdInfo.toLowerCase();
        String outputPD = pdInfo.toLowerCase();
        if (outputVD.contains("vd=virtual drive")) {
            outputVD = outputVD.split("vd=virtual drive")[0];
        }
        if (outputPD.contains("eid=enclosure")) {
            outputPD = outputPD.split("eid=enclosure")[0];
        }

        boolean hasVdIssue = outputVD.contains(" dgrd ") || outputVD.contains(" offln ") || outputVD.contains(" pdgd ");
        boolean hasPdIssue = outputPD.contains(" failed ") || outputPD.contains(" miss ") || outputPD.contains(" offln ") || outputPD.contains(" rbld ");

        if (hasPdIssue || hasVdIssue) {
            logger.error("!!! [告警] 检测到硬盘硬件异常（坏盘或Raid降级），请检查日志中硬盘信息报告");
            return false;
        }

        logger.info(">>>[硬件预检] 磁盘状态正常，可以开始压测<<<");
        return true;
    }

    private static void logHardwareDetail(String title, String content) {
        logger.info("报告项：" + title);
        logger.info(content);
    }

    /**
     * 执行 storcli 命令并返回其输出。
     * 与原实现的区别：命令失败时抛出异常而不是把错误信息当成正常输出返回，
     * 否则调用方无法区分「命令没跑成」和「跑成了且磁盘正常」。
     *
     * @throws IOException 进程启动失败或退出码非 0
     */
    private static String execute(String... command) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(command);
        // 合并 stderr，storcli 把错误写到 stderr 时不会被漏读
        builder.redirectErrorStream(true);

        Process process = builder.start();

        String output;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            output = reader.lines().collect(Collectors.joining("\n"));
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("命令 [" + String.join(" ", command) + "] 退出码 " + exitCode + "，输出：" + output);
        }

        return output;
    }
}
