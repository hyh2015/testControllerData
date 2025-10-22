package org.testController;

import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class RecordTableSelector {

    private static final Logger logger = LoggerFactory.getLogger(RecordTableSelector.class);



    public static void recordTableSqlList(Connection conn, String tableName) throws SQLException {



        String avgreturn_secSql = String.format("select AVG(return_time) from (\n" +
                "select avg(return_time)/1000 return_time,avg(COUNT_FINISH_TIME)/1000 COUNT_FINISH_TIME from  %s\n" +
                "group by thread_name)",tableName);

        String avgfinish_secSql = String.format("select AVG(COUNT_FINISH_TIME) from (\n" +
                "select avg(return_time)/1000 return_time,avg(COUNT_FINISH_TIME)/1000 COUNT_FINISH_TIME from  %s\n" +
                "group by thread_name)",tableName);

        String Maxfinish_secSql = String.format("select AVG(COUNT_FINISH_TIME) from (\n" +
                "select max(COUNT_FINISH_TIME)/1000 COUNT_FINISH_TIME from  %s\n" +
                "group by thread_name)", tableName);

        String sec_100rSQL = String.format("select avg( (COUNT_FINISH_TIME*100)/1000/count) from %s",tableName);

        String stddev_avgSQL = String.format("select stddev(COUNT_FINISH_TIME)/avg(COUNT_FINISH_TIME) from %s", tableName);

        logger.info("---------------------------表："+tableName+"的指标数据----------------------------------");

        // 1. avgreturn/sec
        Double avgreturn = queryForDouble(conn,avgreturn_secSql);
        logger.info("获取到的指标数据 avgreturn/sec：" + avgreturn);

        // 2. avgfinish/sec
        Double avgfinish = queryForDouble(conn,avgfinish_secSql);
        logger.info("获取到的指标数据 avgfinish/sec：" + avgfinish);

        // 3. Maxfinish/sec
        Double Maxfinish = queryForDouble(conn,Maxfinish_secSql);
        logger.info("获取到的指标数据 Maxfinish/sec：" + Maxfinish);

        // 4. 100r/sec
        Double roud100 = queryForDouble(conn,sec_100rSQL);
        logger.info("获取到的指标数据 100r/sec：" + roud100);

        // 5. stddev/avg
        Double stddev = queryForDouble(conn,stddev_avgSQL);
        logger.info("获取到的指标数据 stddev/avg：" + stddev);


    }


    private static Double queryForDouble(Connection conn, String sql) throws SQLException {
//        logger.info("指标获取SQL："+sql);
        try(Statement stmt = conn.createStatement();ResultSet rs = stmt.executeQuery(sql)){
            if (rs.next()){
                return rs.getDouble(1);
            }
        }
        return null;
    }

}

