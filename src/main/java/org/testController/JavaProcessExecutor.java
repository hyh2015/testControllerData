package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * 以子进程方式拉起被调度的读写程序。
 *
 * <p>三个程序（调度器、随机读、入库）现在打在同一个 fat jar 里，不再是三个独立 jar 文件，
 * 因此启动命令由 {@code java -jar tableMigration.jar} 改为
 * {@code java -cp <自身jar> com.test.Test} —— 同一个 jar，不同主类。
 *
 * <p>之所以仍然走子进程而不是直接方法调用，是因为被调度的两个程序依赖进程边界：
 * <ul>
 *   <li>它们的配置读在静态初始化块 / 单例构造函数里，每个 JVM 只读一次；
 *       而调度器每个场景前都会重写 config.properties 和 l2o.properties，
 *       同进程复用会让第二个场景之后静默沿用第一个场景的配置。</li>
 *   <li>随机读的工作线程是 {@code while(true)} 且把 catch 写在循环体内部，
 *       中断异常会被吞掉，只能靠杀进程停下（见下方 destroyForcibly）。</li>
 * </ul>
 */
public class JavaProcessExecutor {

    private static final Logger logger = LoggerFactory.getLogger(JavaProcessExecutor.class);

    /**
     * 主类 -> 该程序专属的 log4j2 配置。
     *
     * <p>打包成 fat jar 后，三个程序无法再靠各自 jar 里的同名 log4j2.xml 区分日志去向，
     * 所以拆成三份不同文件名，子进程启动时用 -Dlog4j.configurationFile 指定。
     * 调度器自身用默认的 log4j2.xml，不在此表中。
     */
    private static final Map<String, String> LOG4J_CONFIG;

    static {
        Map<String, String> m = new HashMap<>();
        m.put("com.test.Test", "log4j2-reader.xml");       // tableMigration：随机读负载
        m.put("com.s1.l2o.Start", "log4j2-writer.xml");    // InsertIntoOracle：入库负载
        LOG4J_CONFIG = Collections.unmodifiableMap(m);
    }

    /**
     * 通用执行java程序的方法
     *
     * @param mainClass      要执行的主类全限定名（打包前是 jar 包名）
     * @param configFileName 配置文件
     * @param timeoutHours   超时时间（单位小时，&lt;=0 表示不设置超时）
     * @param logFileName    输出日志文件名（可为 null 表示不写日志）
     */
    public static void executeJavaProcess(String mainClass, String configFileName, int timeoutHours, String logFileName) {
        List<String> commandList = buildJavaCommand(mainClass, configFileName, timeoutHours);
        ProcessBuilder builder = new ProcessBuilder(commandList);
        builder.redirectErrorStream(true);

        if (logFileName != null && !logFileName.trim().isEmpty()) {
            File logFile = new File(logFileName);
            builder.redirectOutput(logFile);
        }

        Process process = null;
        try {
            process = builder.start();
            int exitCode = process.waitFor();
            if (exitCode == 124) {
                logger.info("java程序：" + mainClass + "执行时间已到, timeout 强制终止");
            } else {
                logger.info("java程序：" + mainClass + "正常退出，退出码：" + exitCode);
            }
        } catch (InterruptedException e) {
            logger.warn("检测到外部中断信号，正在强制停止子进程：" + mainClass);
            if (process != null) {
                process.destroyForcibly();
                logger.info("子进程：" + mainClass + "已强制销毁。");
            }
            //保持中断状态，通知调用者线程已中断
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            logger.error("启动或执行java程序失败：" + mainClass, e);
        }
    }

    /**
     * 构建 Java 命令，可选支持 timeout
     *
     * @param mainClass      要执行的主类全限定名
     * @param configFileName 配置文件名（可为 null）
     * @param timeoutHours   超时时间（单位：小时，&lt;=0 表示不使用 timeout）
     * @return 构建好的命令 List
     */
    public static List<String> buildJavaCommand(String mainClass, String configFileName, int timeoutHours) {
        List<String> command = new ArrayList<>();

        if (timeoutHours > 0) {
            int timeoutSeconds = timeoutHours * 3600;
            command.add("timeout");
            command.add(String.valueOf(timeoutSeconds));
        }

        command.add(javaExecutable());
        command.add("-cp");
        command.add(resolveSelfClasspath());

        String log4jConfig = LOG4J_CONFIG.get(mainClass);
        if (log4jConfig != null) {
            command.add("-Dlog4j.configurationFile=" + log4jConfig);
        }

        command.add(mainClass);

        // 两个被调度的程序其实都忽略命令行参数（各自从当前目录读死文件名），
        // 这里仍然传递，保持与改造前一致。
        if (configFileName != null && !configFileName.trim().isEmpty()) {
            command.add(configFileName);
        }

        logger.info("执行命令：" + String.join(" ", command));
        return command;
    }

    /**
     * 定位自身所在的 fat jar，作为子进程的 -cp。
     *
     * <p>被调度的程序和调度器在同一个 jar 里，所以子进程的 classpath 就是本 jar 自己。
     */
    private static String resolveSelfClasspath() {
        try {
            URL location = JavaProcessExecutor.class.getProtectionDomain().getCodeSource().getLocation();
            if (location != null) {
                File self = new File(location.toURI());
                // 打包运行时这里是 jar 文件；IDE 里跑则是 target/classes 目录
                if (self.isFile()) {
                    return self.getAbsolutePath();
                }
            }
        } catch (Exception e) {
            logger.warn("解析自身 jar 路径失败，回退使用 java.class.path：" + e.getMessage());
        }
        // IDE 中以 class 目录运行：用完整 classpath，否则子进程找不到依赖
        return System.getProperty("java.class.path");
    }

    /**
     * 用当前 JVM 的 java 可执行文件拉起子进程，避免 PATH 里是另一个版本的 JDK。
     */
    private static String javaExecutable() {
        String javaHome = System.getProperty("java.home");
        if (javaHome != null && !javaHome.trim().isEmpty()) {
            for (String name : new String[]{"java", "java.exe"}) {
                File candidate = new File(new File(javaHome, "bin"), name);
                if (candidate.isFile()) {
                    return candidate.getAbsolutePath();
                }
            }
        }
        return "java";
    }

}
