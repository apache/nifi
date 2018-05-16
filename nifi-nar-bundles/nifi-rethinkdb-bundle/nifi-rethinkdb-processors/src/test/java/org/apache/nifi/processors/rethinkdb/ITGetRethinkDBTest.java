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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.rethinkdb.RethinkDB;

import net.minidev.json.JSONObject;

/**
 * Integration test for getting documents from RethinkDB. Please ensure that the RethinkDB is running
 * on local host with default port and has database test with table test and user
 * <code>admin</code> with password <code>admin</code> before running the integration tests or set the attributes in the
 * test accordingly.
 */
@Ignore("Comment this out for running tests against a real instance of RethinkDB")
public class ITGetRethinkDBTest extends ITAbstractRethinkDBTest {

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(GetRethinkDB.class);
        super.setUp();
        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "1 KB");
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 0L, count);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testGetDocumentById() {
        JSONObject message = new JSONObject();
        message.put("id", "1");
        message.put("value", "one");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1L, count);

        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();
        props.put("rethinkdb.id","1");

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_SUCCESS);
        assertEquals("Flow file count should be same", 1, flowFiles.size());
        assertEquals("Error should be null",null, flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE));
        assertEquals("Content should be same size", message.toString().length(), flowFiles.get(0).getSize());
        flowFiles.get(0).assertContentEquals(message.toString());
    }

    @Test
    public void testGetDocumentByIdNotFound() {
        JSONObject message = new JSONObject();
        message.put("id", "1");
        message.put("value", "one");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1L, count);

        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();
        props.put("rethinkdb.id", String.valueOf(System.currentTimeMillis()));

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_NOT_FOUND, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_NOT_FOUND);
        assertEquals("Flow file count should be same", 1, flowFiles.size());
        assertNotNull("Error should not be null", flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE));
        assertEquals("Content should be same size", 0, flowFiles.get(0).getSize());
    }

    @Test
    public void testGetDocumentByHardCodedId() {
        JSONObject message = new JSONObject();
        message.put("id", "2");
        message.put("value", "two");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1, count);

        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "2");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_SUCCESS);
        assertEquals("Flow file count should be same", 1, flowFiles.size());
        assertEquals("Error should be null",null, flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE));
        assertEquals("Content should be same size", message.toString().length(), flowFiles.get(0).getSize());
        flowFiles.get(0).assertContentEquals(message.toString());
    }

    @Test
    public void testGetDocumentTooBig() {
        JSONObject message = new JSONObject();
        message.put("id", "2");
        message.put("value", "two");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        assertEquals("Count should be same", 1, count);

        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "2");
        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "2B");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_FAILURE);
        assertEquals("Flow file count should be same", 1, flowFiles.size());
        String errorMessage = flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE);
        assertNotNull("Error should be not be null", errorMessage);
        assertEquals("Error message should be same", "Document too big size " + message.toJSONString().length() + " bytes",errorMessage);
    }
}