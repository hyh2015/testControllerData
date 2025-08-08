package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


public class MonitorIOUtils {
    private static final Logger logger = LoggerFactory.getLogger(MonitorIOUtils.class);

    public static class MonitorProcesses {
        public final Process iostatProcess;
        public final Process dstatProcess;

        public MonitorProcesses(Process iostatProcess, Process dstatProcess) {
            this.iostatProcess = iostatProcess;
            this.dstatProcess = dstatProcess;
        }
    }

    public static MonitorProcesses startIOstatDstatOutput(String scenario, String monitorInterval, String outFile) throws IOException {
        Process iostatProcess = new ProcessBuilder("iostat", "-xm", monitorInterval)
                .redirectOutput(new File(scenario + "_"+outFile+"_iostat" + monitorInterval + ".log"))
                .redirectErrorStream(true)
                .start();

        Process dstatProcess = new ProcessBuilder("dstat", monitorInterval)
                .redirectOutput(new File(scenario + "_"+outFile+"_dstat" + monitorInterval + ".log"))
                .redirectErrorStream(true)
                .start();

        return new MonitorProcesses(iostatProcess, dstatProcess);
    }

    public static void destroyIfAlive(Process p) {
        if (p != null && p.isAlive()) {
            p.destroy();
        }
    }


    /**
     * 同时销毁监控进程
     * @param monitorProcesses
     */
    public static void stopMonitoring(MonitorProcesses monitorProcesses) {
        destroyIfAlive(monitorProcesses.iostatProcess);
        destroyIfAlive(monitorProcesses.dstatProcess);
    }
}
