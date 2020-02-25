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
package org.apache.nifi.cdc.postgresql

import org.apache.nifi.cdc.postgresql.pgEasyReplication.ConnectionManager

import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.TimeoutException

/**
 * A mock implementation for PostgreSQL , in order to unit test the connection and event handling logic
 */
class MockConnectionManager extends ConnectionManager{
    
    String server
    String database
    String driverName
    String user
    String password
    
    Connection sqlConnection
    Connection repConnection
    
    @Override
    void setProperties(String server, String database, String user, String password, String driverName) {
        this.server = server;
        this.database = database;
        this.user = user;
        this.driverName = driverName;

        if (password == null) {
            password = "";
        }

        this.password = password;

    }

    
    
}


