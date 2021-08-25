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

import com.rethinkdb.RethinkDB;
import net.minidev.json.JSONObject;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration test for getting documents from RethinkDB. Please ensure that the RethinkDB is running
 * on local host with default port and has database test with table test and user
 * <code>admin</code> with password <code>admin</code> before running the integration tests or set the attributes in the
 * test accordingly.
 */
@Disabled("Comment this out for running tests against a real instance of RethinkDB")
public class ITGetRethinkDBTest extends ITAbstractRethinkDBTest {

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(GetRethinkDB.class);
        super.setUp();
        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "1 KB");
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        Assertions.assertEquals(0L, count, "Count should be same");
    }

    @AfterEach
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
        Assertions.assertEquals(1L, count, "Count should be same");

        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();
        props.put("rethinkdb.id","1");

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_SUCCESS);
        Assertions.assertEquals(1, flowFiles.size(), "Flow file count should be same");
        Assertions.assertEquals(null, flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE), "Error should be null");
        Assertions.assertEquals(message.toString().length(), flowFiles.get(0).getSize(), "Content should be same size");
        flowFiles.get(0).assertContentEquals(message.toString());
    }

    @Test
    public void testGetDocumentByIdNotFound() {
        JSONObject message = new JSONObject();
        message.put("id", "1");
        message.put("value", "one");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        Assertions.assertEquals(1L, count, "Count should be same");

        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();
        props.put("rethinkdb.id", String.valueOf(System.currentTimeMillis()));

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_NOT_FOUND, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_NOT_FOUND);
        Assertions.assertEquals(1, flowFiles.size(), "Flow file count should be same");
        Assertions.assertNotNull(flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE), "Error should not be null");
        Assertions.assertEquals(0, flowFiles.get(0).getSize(), "Content should be same size");
    }

    @Test
    public void testGetDocumentByHardCodedId() {
        JSONObject message = new JSONObject();
        message.put("id", "2");
        message.put("value", "two");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        Assertions.assertEquals(1, count, "Count should be same");

        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "2");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_SUCCESS);
        Assertions.assertEquals(1, flowFiles.size(), "Flow file count should be same");
        Assertions.assertEquals(null, flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE), "Error should be null");
        Assertions.assertEquals(message.toString().length(), flowFiles.get(0).getSize(), "Content should be same size");
        flowFiles.get(0).assertContentEquals(message.toString());
    }

    @Test
    public void testGetDocumentTooBig() {
        JSONObject message = new JSONObject();
        message.put("id", "2");
        message.put("value", "two");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        Assertions.assertEquals(1, count, "Count should be same");

        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "2");
        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "2B");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_FAILURE);
        Assertions.assertEquals(1, flowFiles.size(), "Flow file count should be same");
        String errorMessage = flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE);
        Assertions.assertNotNull(errorMessage, "Error should be not be null");
        Assertions.assertEquals("Document too big size " + message.toJSONString().length() + " bytes",errorMessage, "Error message should be same");
    }
}