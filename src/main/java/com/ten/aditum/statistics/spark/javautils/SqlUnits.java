package com.ten.aditum.statistics.spark.javautils;

/**
 * sql语句工具类
 */
public class SqlUnits {

    /**
     * 拼接sql查询语句
     */
    public static String concatSQL(String totalSql, String currentSql) {
        StringBuilder sqlBuilder = new StringBuilder(currentSql);
        sqlBuilder.insert(currentSql.length(), ")");
        sqlBuilder.insert(0, "(");

        // 加入where
        totalSql = trimSpace(totalSql);
        if (!totalSql.contains("WHERE")) {
            totalSql += " WHERE ";
            sqlBuilder.insert(0, totalSql);
        } else {
            sqlBuilder.insert(0, totalSql + " AND ");
        }

        return sqlBuilder.toString();
    }

    /**
     * 去除sql查询语句多出的or
     *
     * @param sql
     * @return
     */
    public static String trimOr(String sql) {
        // 去除行尾空格
        sql = trimSpace(sql);
        if (sql.endsWith("OR") || sql.endsWith("or")) {
            sql = sql.substring(0, sql.length() - 2);
        }

        return sql;
    }

    /**
     * 去除sql查询语句行首行尾空格
     */
    public static String trimSpace(String sql) {
        //  去除行尾空格
        while (sql.endsWith(" ")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        while (sql.startsWith(" ")) {
            sql = sql.substring(1);
        }

        return sql;
    }

    /**
     * 生成使用with···as···的sql语句
     *
     * @param withSql
     * @param tableName
     * @param key
     * @return
     */
    public static String concatSQL(String withSql, String tableName, String key) {
        StringBuilder sqlBuilder = new StringBuilder(withSql);
        sqlBuilder.insert(withSql.length(), ")");
        sqlBuilder.insert(0, "WITH T AS (");
        String currentSql = "SELECT " + tableName + ".* FROM T," + tableName + " WHERE T." + key +
                " = " + tableName + "." + key;
        sqlBuilder.insert(sqlBuilder.length(), currentSql);

        return sqlBuilder.toString();
    }
}
