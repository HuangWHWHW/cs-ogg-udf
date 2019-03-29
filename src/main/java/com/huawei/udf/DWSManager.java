package com.huawei.udf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DWSManager {
    private static Connection connection = null;
    public static Connection getOrCreateConnection(String url, String user, String passwd) throws SQLException {
        if (connection == null) {
            connection = DriverManager.getConnection(url, user, passwd);
        }
        return connection;
    }

    public static void setConnection(Connection connect) {
        connection = connect;
    }

    public static Connection getConnection() {
        return connection;
    }

    public static void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        connection = null;
    }
}
