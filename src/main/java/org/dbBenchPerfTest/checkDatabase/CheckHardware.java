package org.dbBenchPerfTest.checkDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class CheckHardware {

    private static final Logger logger = LoggerFactory.getLogger(CheckHardware.class);

    private static final String STORCLI_PATH = "/opt/MegaRAID/storcli/storcli64";


    public static boolean checkStorageHealth(){
        logger.info(">>> [硬盘预检] 开始获取服务器存储环境信息...");

        //1. 获取控制器基本信息
        String controllerInfo = execute(STORCLI_PATH + " /c0 show");
        logHardwareDetail("控制器摘要：", controllerInfo);

        //2. 获取逻辑盘（Raid）详细信息-检查WB/WT 和降级状态
        String vdInfo = execute(STORCLI_PATH + " /c0 /vall show");
        logHardwareDetail("逻辑卷（VD）状态", vdInfo);

        //3.获取物理盘详细信息
        String pdInfo = execute(STORCLI_PATH + " /c0 /eall /sall show");
        logHardwareDetail("物理硬盘（PD）状态", pdInfo);

        //4.自动化判断逻辑
        String outputVD = vdInfo.toLowerCase();
        String outputPD = pdInfo.toLowerCase();
        if (outputVD.contains("vd=virtual drive")){
            outputVD = outputVD.split("vd=virtual drive")[0];
        }
        if (outputPD.contains("eid=enclosure")){
            outputPD = outputPD.split("eid=enclosure")[0];
        }

        boolean hasVdIssue =  outputVD.contains(" dgrd ") || outputVD.contains(" offln ") || outputVD.contains(" pdgd ");
        boolean hasPdIssue = outputPD.contains(" failed ") || outputPD.contains(" miss ") || outputPD.contains(" offln ") || outputPD.contains(" rbld ");

        if (hasPdIssue || hasVdIssue){
            logger.error("!!! [告警] 检测到硬盘硬件异常（坏盘或Raid降级），请检查日志中硬盘信息报告");
            return false;
        }

        logger.info(">>>[硬件预检] 磁盘状态正常，可以开始压测<<<");
        return true;
    }

    private static void logHardwareDetail(String title, String content){
        logger.info("报告项：" + title);
        logger.info(content);
    }

    private static String execute(String command){
        try{
            Process process = Runtime.getRuntime().exec(command);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))){
                return reader.lines().collect(Collectors.joining("\n"));
            }
        } catch (Exception e){
            return "执行命令失败：" + e.getMessage() + "（请检查storcli工具是否安装）" ;
        }
    }
}

