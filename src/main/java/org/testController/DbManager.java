package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.io.InputStream;

public class DbManager {

    private static final Logger logger = LoggerFactory.getLogger(DbManager.class);
    private static final Properties props = new Properties();

    static {
        try {
            String confPath = System.getProperty("conf", "allconf.properties");
            try (InputStream input = new FileInputStream(confPath)) {
                props.load(input);
            }
        } catch (Exception e) {
            throw new RuntimeException("加载数据库配置失败: " + e.getMessage(), e);
        }
    }

    public static Connection getConnection(String dbType)  {
        String driver = props.getProperty(dbType + ".driver");
        String host = props.getProperty(dbType + ".host");
        String port = props.getProperty(dbType + ".port");
        String user = props.getProperty(dbType + ".user");
        String pass = props.getProperty(dbType + ".password");
        String db = props.getProperty(dbType + ".database");

        String url = props.getProperty(dbType + ".url");
        url = url.replace("{host}", host).replace("{port}", port).replace("{database}", db);

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("无法加载数据库驱动类: " + driver);
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, user, pass);
        } catch (SQLException e) {
            throw new RuntimeException("无法获取数据库连接 URL: " + url + " USER: " + user + " PASSWORD: " + pass);
        }

        return connection;
    }



    public static String getProperty(String key) {
        return props.getProperty(key);
    }

    public static boolean isEnabled(String key){
        return Boolean.parseBoolean(props.getProperty(key,"true"));
    }
}


