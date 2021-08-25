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
 * Integration test for deleting documents from RethinkDB. Please ensure that the RethinkDB is running
 * on local host with default port and has database test with table test and user
 * <code>admin</code> with password <code>admin</code> before running the integration tests or set the attributes in the
 * test accordingly.
 */
@Disabled("Comment this out for running tests against a real instance of RethinkDB")
public class ITDeleteRethinkDBTest extends ITAbstractRethinkDBTest {

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(DeleteRethinkDB.class);
        super.setUp();
        runner.setProperty(DeleteRethinkDB.DURABILITY, "soft");
        runner.setProperty(DeleteRethinkDB.RETURN_CHANGES, "true");

        connection = RethinkDB.r.connection().user(user, password).db(dbName).hostname(dbHost).port(Integer.parseInt(dbPort)).connect();
        RethinkDB.r.db(dbName).table(table).delete().run(connection);
        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        Assertions.assertEquals(0L, count, "Count should be same");
    }

    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testDeleteDocumentById() {
        JSONObject message = new JSONObject();
        message.put("id", "1");
        message.put("value", "one");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        Assertions.assertEquals(1L, count, "Count should be same");

        runner.setProperty(AbstractRethinkDBProcessor.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");

        Map<String, String> props = new HashMap<>();
        props.put("rethinkdb.id","1");

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_SUCCESS, 1);

        String changeMessage = ("{\"old_val\":" + message + "}");

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_SUCCESS);
        Assertions.assertEquals(1, flowFiles.size(), "Flow file count should be same");
        Assertions.assertEquals(null, flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE), "Error should be null");
        Assertions.assertEquals(changeMessage.length(), flowFiles.get(0).getSize(), "Content should be same size");
        flowFiles.get(0).assertContentEquals(changeMessage);
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_DELETED_KEY), "1");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_ERROR_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_INSERTED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_REPLACED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_SKIPPED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_UNCHANGED_KEY),"0");

        Map<String,Object> document = RethinkDB.r.db(dbName).table(table).get("1").run(connection);
        Assertions.assertEquals(document, null, "Document should be null");
    }

    @Test
    public void testDeleteDocumentByIdNoChanges() {
        JSONObject message = new JSONObject();
        message.put("id", "11");
        message.put("value", "one");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        Assertions.assertEquals(1L, count, "Count should be same");

        runner.setProperty(AbstractRethinkDBProcessor.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");
        runner.setProperty(DeleteRethinkDB.RETURN_CHANGES, "false");

        Map<String, String> props = new HashMap<>();
        props.put("rethinkdb.id","11");

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_SUCCESS);
        Assertions.assertEquals(1, flowFiles.size(), "Flow file count should be same");
        Assertions.assertEquals(null, flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE), "Error should be null");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_DELETED_KEY), "1");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_ERROR_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_INSERTED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_REPLACED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_SKIPPED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_UNCHANGED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getSize(), 0);

        Map<String,Object> document = RethinkDB.r.db(dbName).table(table).get("1").run(connection);
        Assertions.assertEquals(document, null, "Document should be null");
    }

    @Test
    public void testDeleteDocumentByIdNotFound() {
        JSONObject message = new JSONObject();
        message.put("id", "1");
        message.put("value", "one");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        Assertions.assertEquals(1L, count, "Count should be same");

        runner.setProperty(AbstractRethinkDBProcessor.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");

        Map<String, String> props = new HashMap<>();
        props.put("rethinkdb.id", String.valueOf(System.currentTimeMillis()));

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_NOT_FOUND, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_NOT_FOUND);
        Assertions.assertEquals(1, flowFiles.size(), "Flow file count should be same");
        Assertions.assertNotNull(flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE), "Error should not be null");
        Assertions.assertEquals(0, flowFiles.get(0).getSize(), "Content should be same size");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_DELETED_KEY), "0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_ERROR_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_INSERTED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_REPLACED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_SKIPPED_KEY),"1");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_UNCHANGED_KEY),"0");

        Map<String,Object> document = RethinkDB.r.db(dbName).table(table).get("1").run(connection);
        Assertions.assertNotNull(document, "Document should not be null");
        Assertions.assertEquals(document.get("id"), "1", "id should be same");
    }

    @Test
    public void testDeleteDocumentByHardCodedId() {
        JSONObject message = new JSONObject();
        message.put("id", "2");
        message.put("value", "two");
        RethinkDB.r.db(dbName).table(table).insert(message).run(connection);

        long count = RethinkDB.r.db(dbName).table(table).count().run(connection);
        Assertions.assertEquals(1, count, "Count should be same");

        runner.setProperty(AbstractRethinkDBProcessor.RETHINKDB_DOCUMENT_ID, "2");

        Map<String, String> props = new HashMap<>();

        runner.enqueue(new byte [] {},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_SUCCESS, 1);

        String changeMessage = ("{\"old_val\":" + message + "}");

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_SUCCESS);
        Assertions.assertEquals(1, flowFiles.size(), "Flow file count should be same");
        Assertions.assertEquals(null, flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE), "Error should be null");
        Assertions.assertEquals(changeMessage.length(), flowFiles.get(0).getSize(), "Content should be same size");
        flowFiles.get(0).assertContentEquals(changeMessage);

        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_DELETED_KEY), "1");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_ERROR_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_INSERTED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_REPLACED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_SKIPPED_KEY),"0");
        Assertions.assertEquals(flowFiles.get(0).getAttribute(DeleteRethinkDB.RETHINKDB_DELETE_RESULT_UNCHANGED_KEY),"0");

        Map<String,Object> document = RethinkDB.r.db(dbName).table(table).get("2").run(connection);
        Assertions.assertEquals(document, null, "Document should be null");
    }

}