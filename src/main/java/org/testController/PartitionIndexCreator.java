package org.testController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class PartitionIndexCreator {

    private static final Logger logger = LoggerFactory.getLogger(PartitionIndexCreator.class);

    /**
     * 数据库 IVORY | PG 使用匿名块创建分区索引
     *
     * @param conn
     * @param tableName
     * @param indexFields
     * @param indexNames
     */
    public static void createPartitionIndexesPgIvory(Connection conn, String tableName, List<String> indexFields, List<String> indexNames) {
        if (indexFields.size() != indexNames.size()) {
            throw new IllegalArgumentException("字段名与索引名数量不一致");
        }

        for (int i = 0; i < indexFields.size(); i++) {
            String columnName = indexFields.get(i);
            String indexName = indexNames.get(i);
            String execSql = buildAnonymousBlock(tableName, columnName, indexName);

            logger.info("开始创建分区索引: {} ", indexName);
//            logger.info("创建分区索引执行SQL：{} ", execSql);


            long start = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(execSql);
            } catch (SQLException throwables) {
                logger.error("表：" + tableName + "上创建分区索引：" + indexName + "失败");
                throwables.printStackTrace();
            }
            long end = System.currentTimeMillis();
            logger.info("索引 {} 创建耗时: {} 秒", indexName, (end - start) / 1000);
        }
    }

    private static String buildAnonymousBlock(String tableName, String colName, String indexName) {
        return "DO $$\n" +
                "DECLARE\n" +
                "    part RECORD;\n" +
                "BEGIN\n" +
                "    FOR part IN (\n" +
                "        SELECT inhrelid::regclass AS partition_name\n" +
                "        FROM pg_inherits\n" +
                "        WHERE inhparent = '" + tableName + "'::regclass\n" +
                "    ) LOOP\n" +
                "        EXECUTE format('CREATE INDEX " + indexName + "_%s ON %s(" + colName + ");', part.partition_name, part.partition_name);\n" +
                "    END LOOP;\n" +
                "END$$;";
    }


    /**
     *  数据库 HIGHGO | VASTBASE | ORACLE 创建分区索引
     * @param conn
     * @param tableName
     * @param indexFields
     * @param indexNames
     * @param dbType
     */
    public static void createPartitionIndexesHgVb(Connection conn, String tableName, List<String> indexFields, List<String> indexNames, String dbType) {

        if (indexFields.size() != indexNames.size()) {
            throw new IllegalArgumentException("字段名与索引名数量不一致");
        }

        for (int i = 0; i < indexFields.size(); i++) {
            String columnName = indexFields.get(i);
            String indexName = indexNames.get(i);
            String execSql = buildCreateIndexSQL(dbType, indexName, tableName, columnName);

            logger.info("开始创建分区索引: {} ", indexName);
            logger.info("创建分区索引执行SQL：{} ", execSql);

            long start = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(execSql);
            } catch (SQLException throwables) {
                logger.error("表：" + tableName + "上创建分区索引：" + indexName + "失败");
                throwables.printStackTrace();
            }
            long end = System.currentTimeMillis();
            logger.info("索引 {} 创建耗时: {} 秒", indexName, (end - start) / 1000);
        }

    }


    private static String buildCreateIndexSQL(String dbType, String indexName, String tableName, String columnName) {
        String sql = "";
        if (dbType.equalsIgnoreCase("Oracle")) {
            sql = String.format("CREATE INDEX %s ON %s (%s) LOCAL", indexName, tableName, columnName);
        } else {
            sql = String.format("CREATE INDEX %s ON %s (%s)", indexName, tableName, columnName);
        }

        return sql;
    }


    /**
     * 在 GBase 8s数据库上创建分区索引
     * @param conn
     * @param tableName
     * @param indexFields
     * @param indexNames
     * @param dbType
     */
    public static void createPartitionIndexesGBase8s(Connection conn, String tableName, List<String> indexFields, List<String> indexNames, String dbType) {

        if (indexFields.size() != indexNames.size()) {
            throw new IllegalArgumentException("字段名与索引名数量不一致");
        }

        for (int i = 0; i < indexFields.size(); i++) {
            String columnName = indexFields.get(i);
            String indexName = indexNames.get(i);
            String execSql = buildCreateIndexSQL(dbType, indexName, tableName, columnName);

            logger.info("开始创建分区索引: {} ", indexName);
            logger.info("创建分区索引执行SQL：{} ", execSql);

            long start = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(execSql);
            } catch (SQLException throwables) {
                logger.error("表：" + tableName + "上创建分区索引：" + indexName + "失败");
                throwables.printStackTrace();
            }
            long end = System.currentTimeMillis();
            logger.info("索引 {} 创建耗时: {} 秒", indexName, (end - start) / 1000);
        }

    }

}

