package com.huawei.udf;

import com.google.gson.JsonElement;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdfScalarFunction extends ScalarFunction {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    transient private JsonParser parser = null;

    private HashMap<String, String> tableMap = new HashMap<>();

    private HashMap<String, String> colMap = new HashMap<>();

    public UdfScalarFunction() {}

    @Override
    public void open(FunctionContext context) {
        parser = new JsonParser();
    }

    private String getValue(SchemaInfo schemaInfo, String name, Entry<String, JsonElement> tuple) {
        if (schemaInfo.isStringType(name)) {
            return "'" + tuple.getValue().getAsString() + "'";
        } else {
            return tuple.getValue().getAsString();
        }
    }

    private String getDwsColName(String sourceTable, String colName) {
        String fullName = sourceTable + "." + colName;
        return colMap.getOrDefault(fullName, colName);
    }

    private String generateUpdateSql(String table, JsonObject before, JsonObject after) throws SQLException {
        // primary key -> value
        HashMap<String, String> primary = new HashMap<>();
        String dwsTable = tableMap.getOrDefault(table, table);
        StringBuffer sb = new StringBuffer();
        sb.append("update ").append(dwsTable).append(" set ");

        // build "after" statement
        Iterator<Entry<String, JsonElement>> it =  after.entrySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            Entry<String, JsonElement> tuple = it.next();
            String name = getDwsColName(table, tuple.getKey());

            SchemaInfo schemaInfo = TableInfo.getOrCreateSchemaInfo(dwsTable);
            if (schemaInfo.isPrimaryKey(name)) {
                primary.put(name, getValue(schemaInfo, name, tuple));
            } else {
                if (count > 0) {
                    sb.append(",");
                }
                sb.append(name + "=" + getValue(schemaInfo, name, tuple));
                count++;
            }
        }

        // build "before" statement
        if (!primary.isEmpty()) {
            sb.append(" where ");
            count = 0;
            for (Entry<String, String> entry : primary.entrySet()) {
                if (count > 0) {
                    sb.append(" and ");
                }
                sb.append(entry.getKey() + "=" + entry.getValue());
                count++;
            }
        } else if (before != null && before.entrySet().size() > 0) {
            sb.append(" where ");
            it = before.entrySet().iterator();
            count = 0;
            while (it.hasNext()) {
                if (count > 0) {
                    sb.append(" and ");
                }
                Entry<String, JsonElement> tuple = it.next();
                String name = getDwsColName(table, tuple.getKey());
                sb.append(name + "=" + getValue(TableInfo.getOrCreateSchemaInfo(dwsTable), name, tuple));
                count++;
            }
        }
        sb.append(";");
        return sb.toString();
    }

    private String generateInsertSql(String table, JsonObject after) throws SQLException {
        String dwsTable = tableMap.getOrDefault(table, table);
        StringBuffer sb = new StringBuffer();
        StringBuffer values = new StringBuffer();
        sb.append("insert into ").append(dwsTable).append("(");
        values.append(" values(");

        // build "after" statement
        Iterator<Entry<String, JsonElement>> it =  after.entrySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            if (count > 0) {
                sb.append(",");
                values.append(",");
            }
            Entry<String, JsonElement> tuple = it.next();
            String name = getDwsColName(table, tuple.getKey());

            sb.append(name);
            values.append(getValue(TableInfo.getOrCreateSchemaInfo(dwsTable), name, tuple));
            count++;
        }
        sb.append(") ");
        values.append(")");
        String result = sb.toString() + values.toString() + ";";
        return result;
    }

    private void buildTableMap(String tableMap) {
        String[] infos = tableMap.split(",");
        for (String pair : infos) {
            String dwsTable = pair.split("=")[0];
            String sourceTable = pair.split("=")[1];
            this.tableMap.put(sourceTable, dwsTable);
        }
    }

    private void parseColMap(String colMap) {
        String[] splitColMaps = colMap.split(",");
        for (String map: splitColMaps) {
            String key = map.split("=")[0];
            String value = map.split("=")[1];
            this.colMap.put(key, value);
        }
    }

    public int eval(String tableName, String opType, String before, String after,
                    String dwsUrl, String dwsUser, String dwsPasswd, String tableMap, String colMap) throws SQLException, InterruptedException {
        eval(tableName, opType, before, after, dwsUrl, dwsUser, dwsPasswd, tableMap, colMap, -1);
        return 0;
    }

    public int eval(String tableName, String opType, String before, String after,
                    String dwsUrl, String dwsUser, String dwsPasswd,
                    String tableMap, String colMap, int retryTimes) throws SQLException, InterruptedException {
        parseColMap(colMap);
        boolean finish = false;
        int retryCount = 1;
        String executeSql = "";
        Connection connect = DWSManager.getOrCreateConnection(dwsUrl, dwsUser, dwsPasswd);
        connect.setAutoCommit(false);
        Statement statement = connect.createStatement();
        try {
            while (!finish) {
                try {
                    buildTableMap(tableMap);
                    JsonObject objAfter = (after == null || after == "") ? null : parser.parse(after).getAsJsonObject();
                    if (opType.equals("I")) {
                        executeSql = generateInsertSql(tableName, objAfter);
                        statement.execute(executeSql);
                    } else if (opType.equals("U")) {
                        JsonObject objBefore = parser.parse(before).getAsJsonObject();
                        executeSql = generateUpdateSql(tableName, objBefore, objAfter);
                        statement.execute(executeSql);
                    }
                    connect.commit();
                    finish = true;
                } catch (Exception e) {
                    logger.error("Run SQL: " + executeSql + " failed, ", e);
                    connect.rollback();
                    logger.info("Begin to retry after 1000ms. Times: " + retryCount++);
                    if (retryTimes != -1 && retryCount > retryTimes) {
                        throw new RuntimeException("Execute job failed after " + retryTimes + " retries.");
                    }
                    Thread.sleep(1000);
                }
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
        return 0;
    }

    @Override
    public void close() throws SQLException {
        DWSManager.close();
    }
}