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

package org.apache.nifi.cdc.postgresql.pgeasyreplication;

import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

public class ConnectionManager {

    private String server;
    private String database;
    private String user;
    private String password;
    private Connection sqlConnection;
    private Connection repConnection;
    private ComponentLog log;

    public void setProperties(String server, String database, String user, String password, ComponentLog log) {
        this.server = server;
        this.database = database;
        this.user = user;

        if (password == null) {
            password = "";
        }

        this.password = password;
        this.log = Objects.requireNonNull(log);
    }

    public void createReplicationConnection() throws SQLException {

        String url = "jdbc:postgresql://" + this.server + "/" + this.database;
        log.debug("Creating SQL replication connection for {}", url);

        Properties props = new Properties();
        props.put("user", this.user);
        props.put("password", this.password);
        props.put("assumeMinServerVersion", "10");
        props.put("replication", "database");
        props.put("preferQueryMode", "simple");

        this.repConnection = DriverManager.getConnection(url, props);
    }

    public Connection getReplicationConnection() {
        return this.repConnection;
    }

    public void closeReplicationConnection() throws SQLException {
        if (repConnection != null) {
            log.debug("Closing SQL replication connection");
            repConnection.close();
        }
    }

    public void createSQLConnection() throws SQLException {
        String url = "jdbc:postgresql://" + this.server + "/" + this.database;
        log.debug("Creating SQL connection for {}", url);

        Properties props = new Properties();
        props.put("user", this.user);
        props.put("password", this.password);

        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        this.sqlConnection = conn;
    }

    public Connection getSQLConnection() {
        return this.sqlConnection;
    }

    public void closeSQLConnection() throws SQLException {
        if (sqlConnection != null) {
            log.debug("Closing SQL connection");
            sqlConnection.close();
        }
    }

}
