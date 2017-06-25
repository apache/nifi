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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.util.List;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.json.simple.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;

import net.minidev.json.JSONObject;

/**
 * Integration test for rethink db. Please ensure that the rethinkdb is running
 * on local host with default port and has database test with table test and user
 * admin with password admin before running the integration tests or set the attributes in the
 * test accordingly.
 */
public class ITPutRethinkDbTest {
    private TestRunner runner;
    private Connection connection;
    private String dbName = "test";
    private String dbHost = "localhost";
    private String dbPort = "28015";
    private String user = "admin";
    private String password = "admin";
    private String table = "test";

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutRethinkDb.class);
        runner.setProperty(PutRethinkDb.DB_NAME, dbName);
        runner.setProperty(PutRethinkDb.DB_HOST, dbHost);
        runner.setProperty(PutRethinkDb.DB_PORT, dbPort);
        runner.setProperty(PutRethinkDb.USERNAME, user);
        runner.setProperty(PutRethinkDb.PASSWORD, password);
        runner.setProperty(PutRethinkDb.TABLE_NAME, table);
        runner.setProperty(PutRethinkDb.CHARSET, "UTF-8");
        runner.setProperty(PutRethinkDb.CONFLICT_STRATEGY, PutRethinkDb.CONFLICT_STRATEGY_UPDATE);
        runner.setProperty(PutRethinkDb.DURABILITY, PutRethinkDb.DURABILITY_HARD);
        runner.setProperty(PutRethinkDb.MAX_DOCUMENTS_SIZE, "1 KB");
        runner.assertValid();

        connection = RethinkDB.r.connection().user(user, password).db(dbName).hostname(dbHost).port(Integer.parseInt(dbPort)).connect();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
        connection.close();
        connection = null;
    }

    @Test
    public void testValidSingleMessage() {
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 0L, count);

        JSONObject message = new JSONObject();
        message.put("hello", "rethinkdb");
        byte [] bytes = message.toJSONString().getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutRethinkDb.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDb.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"1");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1L, count);
    }

    @Test
    public void testValidSingleMessageTwiceConflictUpdate() {
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 0L, count);

        JSONObject message = new JSONObject();
        message.put("id", "rethinkdb");
        byte [] bytes = message.toJSONString().getBytes();
        runner.enqueue(bytes);
        runner.run(1,false,true);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDb.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"1");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        runner.enqueue(bytes);
        runner.run(1,true,true);

        flowFiles = runner.getFlowFilesForRelationship(PutRethinkDb.REL_SUCCESS);
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"1");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1L, count);
    }

    @Test
    public void testValidSingleMessageTwiceConflictError() {
        runner.setProperty(PutRethinkDb.CONFLICT_STRATEGY, PutRethinkDb.CONFLICT_STRATEGY_ERROR);
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 0L, count);

        JSONObject message = new JSONObject();
        message.put("id", "rethinkdb");
        byte [] bytes = message.toJSONString().getBytes();
        runner.enqueue(bytes);
        runner.run(1,false,true);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDb.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"1");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        runner.enqueue(bytes);
        runner.run(1,true,true);

        flowFiles = runner.getFlowFilesForRelationship(PutRethinkDb.REL_FAILURE);
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_ERROR_KEY),"1");
        assertNotNull(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertNotNull(flowFiles.get(1).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY));

        count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1L, count);
    }

    @Test
    public void testValidArrayMessage() {
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 0L, count);

        JSONObject message1 = new JSONObject();
        message1.put("hello", "rethinkdb");
        JSONObject message2 = new JSONObject();
        message2.put("hello2", "rethinkdb2");
        JSONArray array = new JSONArray();
        array.add(message1);
        array.add(message2);
        byte [] bytes = array.toJSONString().getBytes();

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutRethinkDb.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDb.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"2");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDb.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 2L, count);
    }
}