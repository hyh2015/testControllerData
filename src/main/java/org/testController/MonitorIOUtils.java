package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class MonitorIOUtils {

    private static final Logger logger = LoggerFactory.getLogger(MonitorIOUtils.class);

    public static class MonitorProcesses{
        public final Process iostatProcess;
        public final Process dstatProcess;

        public MonitorProcesses(Process iostatProcess, Process dstatProcess){
            this.iostatProcess = iostatProcess;
            this.dstatProcess = dstatProcess;
        }
    }

    /**
     * 启动 iostat 和 dstat 监控进程
     * @param Scenario   场景名
     * @param monitorInterval  监控间隔时间（—秒—）
     * @return
     * @throws IOException
     */
    public static MonitorProcesses startIOstatDstatOutput(String Scenario, String monitorInterval, String outFile) throws IOException {

        Process iostatProc = new ProcessBuilder("iostat", "-xm", monitorInterval)
                .redirectOutput(new File(Scenario + "."+ outFile + ".iostat." + monitorInterval + ".log"))
                .redirectErrorStream(true)
                .start();
        Process dstatProc = new ProcessBuilder("dstat", monitorInterval)
                .redirectOutput(new File(Scenario + "."+ outFile + ".dstat." + monitorInterval + ".log"))
                .redirectErrorStream(true)
                .start();


        return new MonitorProcesses(iostatProc,dstatProc);
    }

    /**
     * 安全销毁一个进程
     * @param process
     */
    private static void destroyIfAlive(Process process) {
        if (process != null && process.isAlive()) {
            process.destroy();
            logger.info("已终止 " + process + " 进程");
        }
    }

    /**
     * 同时销毁监控进程
     */
    public static void stopMonitoring(MonitorProcesses monitor){
        destroyIfAlive(monitor.iostatProcess);
        destroyIfAlive(monitor.dstatProcess);
    }


}

