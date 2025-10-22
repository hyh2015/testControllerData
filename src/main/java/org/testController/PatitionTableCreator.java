package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class PatitionTableCreator {

    private static final Logger logger = LoggerFactory.getLogger(PatitionTableCreator.class);

    public static void createPartitionTable(Connection conn, String partTableName, String dbType, LocalDate partStartDate, LocalDate partEndDate) throws Exception {


        try (Statement stmt = conn.createStatement()) {
//            stmt.execute("DROP TABLE IF EXISTS " + partTableName);
            String createTableSql = "";
            if (dbType.equalsIgnoreCase("Oracle")){
                createTableSql = "CREATE TABLE  "+partTableName+"(\n" +
                        "begintime date,\n" +
                        "usernum varchar2(128),\n" +
                        "imei varchar2(128),\n" +
                        "calltype varchar2(128),\n" +
                        "netid varchar2(128),\n" +
                        "lai varchar2(128),\n" +
                        "ci varchar2(128),\n" +
                        "imsi varchar2(128),\n" +
                        "start_time varchar2(128),\n" +
                        "end_time varchar2(128),\n" +
                        "longitude varchar2(128),\n" +
                        "latitude varchar2(128),\n" +
                        "lacci varchar2(128),\n" +
                        "timespan varchar2(128),\n" +
                        "extra_longitude varchar2(128),\n" +
                        "extra_latitude varchar2(128),\n" +
                        "geospan varchar2(128),\n" +
                        "anchorhash varchar2(128),\n" +
                        "extra_geohash varchar2(128),\n" +
                        "bd varchar2(128),\n" +
                        "ad varchar2(128),\n" +
                        "user_id varchar2(128),\n" +
                        "address varchar2(256),\n" +
                        "car_id varchar2(128),\n" +
                        "mac varchar2(128),\n" +
                        "mobile_mode varchar2(128),\n" +
                        "usernum1 varchar2(128),\n" +
                        "area varchar2(128),\n" +
                        "ipv4 varchar2(128),\n" +
                        "ipv6 varchar2(128),\n" +
                        "mission_id varchar2(256),\n" +
                        "bankcard_id varchar2(128)\n" +
                        ")";
            }else {
                createTableSql = "CREATE TABLE IF NOT EXISTS " + partTableName + " ("
                        + "begintime text," + "usernum text," + "imei text," + "calltype text," + "netid text," + "lai text," +
                        "ci text," + "imsi text," + "start_time text," + "end_time text," + "longitude text," + "latitude text," +
                        "lacci text," + "timespan text," + "extra_longitude text," + "extra_latitude text," + "geospan text," +
                        "anchorhash text," + "extra_geohash text," + "bd text," + "ad text," + "user_id text," + "address text," +
                        "car_id text," + "mac text," + "mobile_mode text," + "usernum1 text," + "area text," + "ipv4 text," +
                        "ipv6 text," + "mission_id text," + "bankcard_id text"
                        + ") ";
            }

            if (dbType.equalsIgnoreCase("pgdb") || dbType.equalsIgnoreCase("ivory")) {
                createTableSql += "PARTITION BY RANGE (begintime)";
                logger.info(dbType+"数据库开始创建分区表基表："+partTableName);
                stmt.execute(createTableSql);
                logger.info(dbType+"数据库的分区表："+partTableName+"基表创建成功");

                String doSql = String.format(
                        "DO $$\n" +
                                "DECLARE\n" +
                                "    start_date DATE := DATE '%s';\n" +
                                "    end_date DATE := DATE '%s';\n" +
                                "    partition_name TEXT;\n" +
                                "    table_name TEXT := '%s';\n" +
                                "BEGIN\n" +
                                "    WHILE start_date < end_date LOOP\n" +
                                "        partition_name := format('%%I_p%%s', table_name, to_char(start_date, 'YYYYMMDD'));\n" +
                                "        IF NOT EXISTS (\n" +
                                "            SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = partition_name\n" +
                                "        ) THEN\n" +
                                "            EXECUTE format(\n" +
                                "                'CREATE TABLE IF NOT EXISTS %%I PARTITION OF %%I FOR VALUES FROM (%%L::DATE) TO (%%L::DATE);',\n" +
                                "                partition_name,\n" +
                                "                table_name,\n" +
                                "                start_date,\n" +
                                "                start_date + INTERVAL '1 day'\n" +
                                "            );\n" +
                                "        END IF;\n" +
                                "        start_date := start_date + INTERVAL '1 day';\n" +
                                "    END LOOP;\n" +
                                "END\n" +
                                "$$;"
                        , partStartDate, partEndDate, partTableName);
                // 执行匿名块
                logger.info(dbType+"数据库的分区表："+partTableName +"，执行匿名块创建分区");
                stmt.execute(doSql);
                logger.info(dbType+"数据库的分区表，匿名块执行创建分区成功");

/*                LocalDate current = partStartDate;
                while (current.isBefore(partEndDate)) {
                    String partitionName = partTableName + "_p" + current.format(DateTimeFormatter.BASIC_ISO_DATE);
                    String sql = "CREATE TABLE IF NOT EXISTS " + partitionName
                            + " PARTITION OF " + partTableName
                            + " FOR VALUES FROM ('" + current + "') TO ('" + current.plusDays(1) + "')";
                    stmt.execute(sql);
                    current = current.plusDays(1);
                }*/
            } else if (dbType.equalsIgnoreCase("highgo")
                    || dbType.equalsIgnoreCase("vastdata")
                    || dbType.equalsIgnoreCase("Oracle")) {
                StringBuilder partitionSql = new StringBuilder(createTableSql);
                partitionSql.append("PARTITION BY RANGE (begintime) (");

                LocalDate current = partStartDate;
                while (current.isBefore(partEndDate)) {
                    String partitionName = partTableName + "_p" + current.format(DateTimeFormatter.BASIC_ISO_DATE);
                    String valueLessThan = current.plusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                    partitionSql.append("PARTITION ").append(partitionName)
                            .append(" VALUES LESS THAN (TO_DATE('").append(valueLessThan).append("','YYYYMMDD')),");
                    current = current.plusDays(1);
                }
                partitionSql.setLength(partitionSql.length() - 1);
                partitionSql.append(")");
                logger.info(dbType+"数据库开始执行创建分区表："+partTableName);
                stmt.execute(partitionSql.toString());
                logger.info(dbType+"数据库创建分区表："+partTableName+"成功");
            } else {
                throw new UnsupportedOperationException("不支持的数据库类型: " + dbType);
            }
        }
    }

}

