package com.huawei.udf;

import java.sql.SQLException;
import java.util.HashMap;

public class TableInfo {
    private static HashMap<String, SchemaInfo> schemaInfo = new HashMap<>();
    public static SchemaInfo getOrCreateSchemaInfo(String tableName) throws SQLException {
        if (!schemaInfo.containsKey(tableName)) {
            schemaInfo.put(tableName, new SchemaInfo(tableName));
        }
        return schemaInfo.get(tableName);
    }
}
