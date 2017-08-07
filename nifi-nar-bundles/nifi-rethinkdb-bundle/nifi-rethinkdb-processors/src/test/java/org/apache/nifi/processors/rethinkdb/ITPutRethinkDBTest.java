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
import org.apache.nifi.util.TestRunners;
import org.json.simple.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.rethinkdb.RethinkDB;

import net.minidev.json.JSONObject;

/**
 * Integration test for RethinkDB. Please ensure that the RethinkDB is running
 * on local host with default port and has database test with table test and user
 * admin with password admin before running the integration tests or set the attributes in the
 * test accordingly.
 */
@Ignore("Comment this out for running tests against a real instance of RethinkDB")
public class ITPutRethinkDBTest extends ITAbstractRethinkDBTest {

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutRethinkDB.class);
        super.setUp();
        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "1 KB");
        runner.setProperty(PutRethinkDB.CONFLICT_STRATEGY, PutRethinkDB.CONFLICT_STRATEGY_UPDATE);
        runner.setProperty(PutRethinkDB.DURABILITY, PutRethinkDB.DURABILITY_HARD);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testValidSingleMessage() {
        runner.assertValid();
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 0L, count);

        JSONObject message = new JSONObject();
        message.put("hello", "rethinkdb");
        byte [] bytes = message.toJSONString().getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutRethinkDB.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"1");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1L, count);
    }

    @Test
    public void testValidSingleMessageTwiceConflictUpdate() {
        runner.assertValid();
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 0L, count);

        JSONObject message = new JSONObject();
        message.put("id", "rethinkdb");
        byte [] bytes = message.toJSONString().getBytes();
        runner.enqueue(bytes);
        runner.run(1,false,true);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"1");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        runner.enqueue(bytes);
        runner.run(1,true,true);

        flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_SUCCESS);
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(1).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"1");
        assertEquals(flowFiles.get(1).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1L, count);
    }

    @Test
    public void testValidSingleMessageTwiceConflictError() {
        runner.setProperty(PutRethinkDB.CONFLICT_STRATEGY, PutRethinkDB.CONFLICT_STRATEGY_ERROR);
        runner.assertValid();
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 0L, count);

        JSONObject message = new JSONObject();
        message.put("id", "rethinkdb");
        byte [] bytes = message.toJSONString().getBytes();
        runner.enqueue(bytes);
        runner.run(1,false,true);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"1");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        runner.enqueue(bytes);
        runner.run(1,true,true);

        flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_FAILURE);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY),"1");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY));

        count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1L, count);
    }

    @Test
    public void testValidArrayMessage() {
        runner.assertValid();
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
        runner.assertAllFlowFilesTransferred(PutRethinkDB.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY),"0");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"2");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");

        count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 2L, count);
    }
}