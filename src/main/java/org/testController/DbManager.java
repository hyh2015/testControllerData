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
            throw new RuntimeException("加载数据库配置失败：" + e.getMessage(), e);
        }
    }

    public static Connection getConnection(String dbType)   {
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
        } catch (SQLException throwables) {
            throw new RuntimeException("无法获取数据库连接 URL:"+url + " USER:"+user+" PASSWD:"+pass);
        }

        return connection;
    }



    public static String getProperty(String key) {
        return props.getProperty(key);
    }


    /**
     * 判断某个开关是否启用。
     *
     * <p>不提供无参重载、强制每次调用都显式传 defaultValue —— 是为了让"配置里漏写
     * 这个键该怎么办"这件事在调用点就能看到，不必去翻这个方法的实现才知道。
     * 目前全部调用点都传 false：场景/安装类开关（scene*.enabled、is.install.ivory）
     * 漏配置不该意外多跑一个耗时的场景或触发一次没人要求的安装；
     * hardware.check.enabled 缺省也不检查，因为大多数实际部署的测试服务器没有 storcli，
     * 需要检查的机器显式置 true 即可。
     * 以后如果加一个"漏配置更应该默认执行"的开关，在调用点传 true 即可，
     * 不需要改这个方法本身。
     *
     * @param key 配置键名
     * @param defaultValue 键缺失时的默认值
     */
    public static boolean isEnabled(String key, boolean defaultValue){
        return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
    }
}




