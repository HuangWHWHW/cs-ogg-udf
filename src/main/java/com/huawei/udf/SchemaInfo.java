package com.huawei.udf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

public class SchemaInfo {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    private String tableName;

    private HashMap<String, String> columnInfo = new HashMap<>();
    private List<String> primaryKeys = new ArrayList<>();

    public SchemaInfo(String tableName) throws SQLException {
        this.tableName = tableName;
        Connection connection = DWSManager.getConnection();
        Statement statement = connection.createStatement();
        try {
            // get primary keys
            DatabaseMetaData dbmd = connection.getMetaData();
            ResultSet rs = null;
            String[] tableNames = tableName.split("\\.");
            if (tableNames.length > 0) {
                rs = dbmd.getPrimaryKeys(null, tableNames[0], tableNames[1]);
            } else {
                rs = dbmd.getPrimaryKeys(null, null, tableName);
            }
            while (rs.next()) {
                primaryKeys.add(String.valueOf(rs.getObject(4)));
            }

            // get column info
            String sql = "select * from " + tableName + " where 1=0;";
            ResultSetMetaData rsmd = statement.executeQuery(sql).getMetaData();

            int colNum = rsmd.getColumnCount();
            for (int i = 0; i < colNum; i++) {
                String colName = rsmd.getColumnName(i + 1);
                String colType = rsmd.getColumnTypeName(i + 1);
                columnInfo.put(colName, getCSType(colType));
            }
        } finally {
            statement.close();
        }
    }

    private void printColumName() {
        int count = 0;
        StringBuffer sb = new StringBuffer();
        logger.info("Print column name for table: " + tableName + "\n");
        for (String colName: columnInfo.keySet()) {
            if (count > 0) {
                sb.append(",");
            }
            sb.append(colName);
        }
        logger.info(sb.toString());
    }

    private String getCSType(String dwsType) {
        if (dwsType.equals("bit") || dwsType.equals("bypea")) {
            return "ARRAY[TINYINT]";
        } else if (dwsType.equals("int2")) {
            return "SMALLINT";
        } else if (dwsType.equals("int4")) {
            return "INT";
        } else if (dwsType.equals("int8")) {
            return "BIGINT";
        } else if (dwsType.equals("float4")) {
            return "FLOAT";
        } else if (dwsType.equals("float8") || dwsType.equals("money")) {
            return "DOUBLE";
        } else if (dwsType.equals("numeric")
                || dwsType.equals("decimal")) {
            return "DECIMAL";
        } else {
            return "STRING";
        }
    }

    public boolean isStringType(String columnName) {
        return columnInfo.get(columnName.toLowerCase(Locale.US)).equals("STRING");
    }

    public boolean isPrimaryKey(String columnName) {
        return primaryKeys.contains(columnName);
    }
}
