package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class UpdateConfProperties {
    private static final Logger logger = LoggerFactory.getLogger(UpdateConfProperties.class);
    private static SceneExecutor executor = new SceneExecutor(DbManager.getProperty("db.type"));

    /**
     * 通用更新l2o.properties配置文件的方法
     * @param tableName  insert入库和copy批量入库的表
     * @param fileNum    入库文件个数
     * @param threadNum   并发数
     * @param bulkload    是否使用copy...stdin... 为true即使用，反之使用insert values.
     * @return
     */
    public static boolean updateConcurrentInsertConfig(String tableName,String fileNum,
                                                       String threadNum,Boolean bulkload){


        Path l2oPath = Paths.get("l2o.properties");

        // 构造参数配置信息
        Map<String,String> configMap = new LinkedHashMap<>();
        // 固定数据库参数配置
        configMap.put("db.user", executor.getDbUser());
        configMap.put("db.password", executor.getDbPassword());
        configMap.put("db.driver", executor.getDbDriverClass());
        configMap.put("db.url", executor.getDbURL());
        configMap.put("db.type", executor.getDbType());
        configMap.put("localfile.path", executor.getDataPath());
        // 动态参数
        configMap.put("db.bulkload", bulkload.toString());
        configMap.put("table.name", tableName);
        configMap.put("file.num", fileNum);
        configMap.put("thread.num", threadNum);

        // 写入文件
        try(BufferedWriter writer = Files.newBufferedWriter(l2oPath)){
            writer.write("# Updated by updateConcurrentInsertConfig\n");
            for(Map.Entry<String,String> entry : configMap.entrySet()){
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("[配置更新] 写入 l2o.properties 文件失败", e);
            return false;
        }

        logger.info("[配置更新] l2o.properties 更新成功 ");
        // TODO 显示输出所有配置项
        logger.info("======================l2o.properties=======================");

        try (InputStream inCheck = Files.newInputStream(l2oPath)) {
            Properties checkProps = new Properties();
            checkProps.load(inCheck);

            for (String key : checkProps.stringPropertyNames()) {
                String value = checkProps.getProperty(key);
                logger.info(key + " = " + value);
            }
        } catch (IOException e) {
            logger.error("[配置校验] 读取 l2o.properties 文件失败", e);
        }

        logger.info("======================l2o.properties end=====================");

        return true;

    }

    /**
     * 更新并发读场景的 config.properties 配置文件
     * @param queryType 要设置的 query.type 值（例如 "1" 或 "2"）
     * @return true 表示更新成功，false 表示失败
     */
    public static boolean updateConcurrentReadConfig(String queryType) {
        Path configPath = Paths.get("config.properties");

        // 构造参数配置信息
        Map<String,String> configMap = new LinkedHashMap<>();
        // 固定数据库参数配置
        configMap.put("resultdb.User", executor.getDbUser());
        configMap.put("resultdb.Pwd", executor.getDbPassword());
        configMap.put("resultdb.Url", executor.getDbURL());
        configMap.put("resultdb.driver", executor.getDbDriverClass());
        // 动态设置参数
        configMap.put("query.type", queryType);
        configMap.put("MaxThread", "100");

        // 写入文件
        try(BufferedWriter writer = Files.newBufferedWriter(configPath)){
            writer.write("# Updated by updateConcurrentReadConfig\n");
            for(Map.Entry<String,String> entry : configMap.entrySet()){
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("[配置更新] 写入 config.properties 文件失败", e);
            return false;
        }

        logger.info("[配置更新] config.properties 更新成功，query.type = " + queryType);
        // TODO 显示输出所有配置项
        logger.info("======================config.properties=======================");

        try (InputStream inCheck = Files.newInputStream(configPath)) {
            Properties checkProps = new Properties();
            checkProps.load(inCheck);

            for (String key : checkProps.stringPropertyNames()) {
                String value = checkProps.getProperty(key);
                logger.info(key + " = " + value);
            }
        } catch (IOException e) {
            logger.error("[配置校验] 读取 config.properties 文件失败", e);
        }

        logger.info("======================config.properties end=====================");

        return true;
    }


    public static boolean updateReadConfig(String recordTableNum, String maxThread) {
        Path configPath = Paths.get("config.properties");

        // 构造参数配置信息
        Map<String,String> configMap = new LinkedHashMap<>();
        // 固定数据库参数配置
        configMap.put("resultdb.User", executor.getDbUser());
        configMap.put("resultdb.Pwd", executor.getDbPassword());
        configMap.put("resultdb.Url", executor.getDbURL());
        configMap.put("resultdb.driver", executor.getDbDriverClass());
        // 动态设置参数
        configMap.put("query.type", recordTableNum);
        configMap.put("MaxThread", maxThread);

        // 写入文件
        try(BufferedWriter writer = Files.newBufferedWriter(configPath)){
            writer.write("# Updated by updateConcurrentReadConfig\n");
            for(Map.Entry<String,String> entry : configMap.entrySet()){
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("[配置更新] 写入 config.properties 文件失败", e);
            return false;
        }

        logger.info("[配置更新] config.properties 更新成功，query.type = " + recordTableNum);
        // TODO 显示输出所有配置项
        logger.info("======================config.properties=======================");

        try (InputStream inCheck = Files.newInputStream(configPath)) {
            Properties checkProps = new Properties();
            checkProps.load(inCheck);

            for (String key : checkProps.stringPropertyNames()) {
                String value = checkProps.getProperty(key);
                logger.info(key + " = " + value);
            }
        } catch (IOException e) {
            logger.error("[配置校验] 读取 config.properties 文件失败", e);
        }

        logger.info("======================config.properties end=====================");

        return true;
    }


}

