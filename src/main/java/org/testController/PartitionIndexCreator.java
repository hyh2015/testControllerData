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
     *  todo 未测试
     *
     * @param conn
     * @param tableName
     * @param indexFields
     * @param indexNames
     * @throws Exception
     */
    public static void createPartitionIndexesPgIvory(Connection conn, String tableName, List<String> indexFields, List<String> indexNames) throws Exception {
        if (indexFields.size() != indexNames.size()) {
            throw new IllegalArgumentException("字段名与索引名数量不一致");
        }

        for (int i = 0; i < indexFields.size(); i++) {
            String columnName = indexFields.get(i);
            String indexName = indexNames.get(i);
            String block = buildAnonymousBlock(tableName, columnName, indexName);

            logger.info("开始创建分区索引: {} on {}({})", indexName, tableName, columnName);
            long start = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(block);
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


    public static void createPartitionIndexesHighgoVastbaseOracle(Connection conn, String tableName, List<String> indexFields, List<String> indexNames, String dbType) {
        if (indexFields.size() != indexNames.size()) {
            throw new IllegalArgumentException("字段名与索引名数量不一致");
        }

        if (indexFields.size() != indexNames.size()) {
            throw new IllegalArgumentException("字段名与索引名数量不一致");
        }

        for (int i = 0; i < indexFields.size(); i++) {
            String columnName = indexFields.get(i);
            String indexName = indexNames.get(i);
            String exceSql = buildCreateIndexSQL(dbType, tableName, columnName, indexName);

            logger.info("开始创建分区索引: {} ", indexName);
            logger.info("创建分区索引执行SQL: {} ", exceSql);

            long start = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(exceSql);
            } catch (SQLException e) {
                logger.error("表：" + tableName + "上创建分区索引：" + indexName + "失败");
                throw new RuntimeException(e);
            }
            long end = System.currentTimeMillis();
            logger.info("索引 {} 创建耗时: {} 秒", indexName, (end - start) / 1000);
        }

    }

    public static String buildCreateIndexSQL(String dbType, String tableName, String columnName, String indexName) {
        String sql = "";
        if (dbType.equalsIgnoreCase("Oracle")) {
            sql = String.format("CREATE INDEX %s ON %s(%s) LOCAL", tableName, columnName, indexName);
        } else {
            sql = String.format("CREATE INDEX %s ON %s(%s)", tableName, columnName, indexName);
        }
        return sql;
    }
}
