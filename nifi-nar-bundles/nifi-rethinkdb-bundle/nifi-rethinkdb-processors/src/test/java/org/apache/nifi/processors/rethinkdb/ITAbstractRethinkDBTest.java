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
package org.apache.nifi.processors.rethinkdb;

import org.apache.nifi.util.TestRunner;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;

/**
 * Abstract base class for RethinkDB integration tests
 */
public class ITAbstractRethinkDBTest {
    protected TestRunner runner;
    protected Connection connection;
    protected String dbName = "test";
    protected String dbHost = "localhost";
    protected String dbPort = "28015";
    protected String user = "admin";
    protected String password = "admin";
    protected String table = "test";

    public void setUp() throws Exception {
        runner.setProperty(AbstractRethinkDBProcessor.DB_NAME, dbName);
        runner.setProperty(AbstractRethinkDBProcessor.DB_HOST, dbHost);
        runner.setProperty(AbstractRethinkDBProcessor.DB_PORT, dbPort);
        runner.setProperty(AbstractRethinkDBProcessor.USERNAME, user);
        runner.setProperty(AbstractRethinkDBProcessor.PASSWORD, password);
        runner.setProperty(AbstractRethinkDBProcessor.TABLE_NAME, table);
        runner.setProperty(AbstractRethinkDBProcessor.CHARSET, "UTF-8");

        connection = RethinkDB.r.connection().user(user, password).db(dbName).hostname(dbHost).port(Integer.parseInt(dbPort)).connect();
    }

    public void tearDown() throws Exception {
        runner = null;
        connection.close();
        connection = null;
    }

}