package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import java.sql.Connection;

public class RecordTableSelector {

    private static final Logger logger = LoggerFactory.getLogger(RecordTableSelector.class);

    public static Map<String, Double> recordTableSqlList(Connection conn, String tableName, String prefix) throws SQLException {

        Map<String, Double> map = new LinkedHashMap<>();

        String avgreturn_secSql = String.format("select avg(return_time) \n" +
                "from (select avg(return_time)/1000 return_time,avg(count_finish_time)/1000 count_finish_time from %s" +
                " group by thread_name)", tableName);

        String avgfinish_secSql = String.format("select avg(count_finish_time)\n" +
                "from (select avg(return_time)/1000 return_time,avg(count_finish_time)/1000 count_finish_time from %s" +
                " group by thread_name)", tableName);

        String maxfinish_secSql = String.format("select avg(count_finish_time)\n" +
                "from (select max(count_finish_time)/1000 count_finish_time  from %s" +
                " group by thread_name)", tableName);

        String roud100_secSql = String.format("select avg((count_finish_time*100)/1000/count) from %s", tableName);

        String stddev_avgSql = String.format("select stddev(count_finish_time)/avg(count_finish_time) count_finish_time from %s", tableName);

//        1.avgreturn/sec
        Double avgreturn = queryForDouble(conn, avgreturn_secSql);
        map.put(prefix + "_avgreturn_per_sec", avgreturn);
        logger.info("获取到的指标数据 avgreturn/sec：" + avgreturn);

//        2. avgfinish/sec
        Double avgfinish = queryForDouble(conn, avgfinish_secSql);
        map.put(prefix + "_avgfinish_per_sec", avgfinish);
        logger.info("获取到的指标数据 avgfinish/sec：" + avgfinish);

//        3.maxfinish/sec
        Double maxfinish = queryForDouble(conn, maxfinish_secSql);
        map.put(prefix + "_maxfinish_per_sec", maxfinish);
        logger.info("获取到的指标数据 maxfinish/sec：" + maxfinish);

//        4.100r/sec
        Double roud100 = queryForDouble(conn, roud100_secSql);
        map.put(prefix + "_100r_per_sec", roud100);
        logger.info("获取到的指标数据 100r/sec：" + roud100);

//        5.stddev/avg
        Double stddev = queryForDouble(conn, stddev_avgSql);
        map.put(prefix + "_stddev_avg", stddev);
        logger.info("获取到的指标数据 stddev/avg：" + stddev);

        return map;

    }

    private static Double queryForDouble(Connection conn, String sql) throws SQLException {
        logger.info("指标获取SQL：" + sql);
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getDouble(1);
            }
        }

        return null;
    }

}
