/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionManager {

    private String server;
    private String database;
    private String driverName;
    private String user;
    private String password;
    private Connection sqlConnection;
    private Connection repConnection;

    public void setProperties(String server, String database, String user, String password, String driverName) {
        this.server = server;
        this.database = database;
        this.user = user;
        this.driverName = driverName;

        if (password == null)
            password = "";

        this.password = password;
    }

    public void createReplicationConnection() throws Exception {

        String url = "jdbc:postgresql://" + this.server + "/" + this.database;

        Properties props = new Properties();
        props.put("user", this.user);
        props.put("password", this.password);
        props.put("assumeMinServerVersion", "10");
        props.put("replication", "database");
        props.put("preferQueryMode", "simple");

        Connection conn = null;
        Class.forName(this.driverName);
        conn = DriverManager.getConnection(url, props);
        this.repConnection = conn;
    }

    public Connection getReplicationConnection() {
        return this.repConnection;
    }

    public void closeReplicationConnection() throws Exception {
        this.repConnection.close();
    }

    public void createSQLConnection() throws Exception {

        String url = "jdbc:postgresql://" + this.server + "/" + this.database;

        Properties props = new Properties();
        props.put("user", this.user);
        props.put("password", this.password);

        Connection conn = null;
        Class.forName(this.driverName);
        conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        this.sqlConnection = conn;
    }

    public Connection getSQLConnection() {
        return this.sqlConnection;
    }

    public void closeSQLConnection() throws Exception {
        this.sqlConnection.close();
    }

}
