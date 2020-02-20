package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionManager {

    private static String server;
    private static String database;
    private static String driverName;
    private static String user;
    private static String password;
    private static Connection sqlConnection;
    private static Connection repConnection;

    public static void setProperties(String server, String database, String user, String password, String driverName) {
        ConnectionManager.server = server;
        ConnectionManager.database = database;
        ConnectionManager.user = user;
        ConnectionManager.driverName = driverName;

        if(password == null) {
            password = "";
        }

        ConnectionManager.password = password;

    }

    public static void createReplicationConnection() throws Exception {

        String url = "jdbc:postgresql://" + ConnectionManager.server + "/" + ConnectionManager.database;

        Properties props = new Properties();
        props.put("user", ConnectionManager.user);
        props.put("password", ConnectionManager.password);
        props.put("assumeMinServerVersion", "10");
        props.put("replication", "database");
        props.put("preferQueryMode", "simple");

        Connection conn = null;
        Class.forName(ConnectionManager.driverName);
        conn = DriverManager.getConnection(url, props);
        ConnectionManager.repConnection = conn;
    }

    public static Connection getReplicationConnection() {
        return ConnectionManager.repConnection;
    }

    public static void closeReplicationConnection() throws Exception{
        ConnectionManager.repConnection.close();
    }

    public static void createSQLConnection() throws Exception{

        String url = "jdbc:postgresql://" + ConnectionManager.server + "/" + ConnectionManager.database;

        Properties props = new Properties();
        props.put("user", ConnectionManager.user);
        props.put("password", ConnectionManager.password);

        Connection conn = null;
        Class.forName(ConnectionManager.driverName);			
        conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        ConnectionManager.sqlConnection = conn;
    }

    public static Connection getSQLConnection() {
        return ConnectionManager.sqlConnection;
    }

    public static void closeSQLConnection() throws Exception{
        ConnectionManager.sqlConnection.close();
    }



}
