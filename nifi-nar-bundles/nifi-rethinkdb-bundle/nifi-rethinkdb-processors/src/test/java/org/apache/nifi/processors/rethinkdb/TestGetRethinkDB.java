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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.rethinkdb.net.Connection;

public class TestGetRethinkDB {
    private static final String DOCUMENT_ID = "id1";
    private TestRunner runner;
    private AbstractRethinkDBProcessor mockGetRethinkDB;
    private Map<String,Object> document;

    @Before
    public void setUp() throws Exception {
        mockGetRethinkDB = new GetRethinkDB() {
            @Override
            protected Connection makeConnection() {
                return null;
            }

            @Override
            protected Map<String,Object> getDocument(String id, String readMode) {
                return document;
            }
        };
        runner = TestRunners.newTestRunner(mockGetRethinkDB);
        runner.setProperty(GetRethinkDB.DB_NAME, "test");
        runner.setProperty(GetRethinkDB.DB_HOST, "host1");
        runner.setProperty(GetRethinkDB.DB_PORT, "1234");
        runner.setProperty(GetRethinkDB.USERNAME, "u1");
        runner.setProperty(GetRethinkDB.PASSWORD, "p1");
        runner.setProperty(GetRethinkDB.TABLE_NAME, "t1");
        runner.setProperty(GetRethinkDB.CHARSET, "UTF-8");
        runner.setProperty(GetRethinkDB.READ_MODE, "single");
        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");
        runner.setProperty(GetRethinkDB.MAX_DOCUMENTS_SIZE, "1 KB");
        runner.assertValid();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void testDefaultValid() {
        runner.assertValid();
    }

    @Test
    public void testBlankHost() {
        runner.setProperty(GetRethinkDB.DB_HOST, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyPort() {
        runner.setProperty(GetRethinkDB.DB_PORT, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyDBName() {
        runner.setProperty(GetRethinkDB.DB_NAME, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyUsername() {
        runner.setProperty(GetRethinkDB.USERNAME, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyPassword() {
        runner.setProperty(GetRethinkDB.PASSWORD, "p1");
        runner.assertValid();
    }

    @Test
    public void testCharsetUTF8() {
        runner.setProperty(GetRethinkDB.CHARSET, "UTF-8");
        runner.assertValid();
    }

    @Test
    public void testCharsetBlank() {
        runner.setProperty(GetRethinkDB.CHARSET, "");
        runner.assertNotValid();
    }
    @Test
    public void testZeroMaxDocumentSize() {
        runner.setProperty(GetRethinkDB.MAX_DOCUMENTS_SIZE, "0");
        runner.assertNotValid();
    }

    @Test
    public void testBlankReadMode() {
        runner.setProperty(GetRethinkDB.READ_MODE, "");
        runner.assertNotValid();
    }

    @Test
    public void testSizeGreaterThanThreshold() {
        runner.setProperty(GetRethinkDB.MAX_DOCUMENTS_SIZE, "1 B");
        runner.assertValid();
        document = new HashMap<>();
        document.put("hello", "rethinkdb");

        HashMap<String,String> props = new HashMap<>();
        props.put("rethinkdb.id", DOCUMENT_ID);

        runner.enqueue(new byte[]{}, props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_FAILURE);
        assertNotNull(flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE));
    }

    @Test
    public void testNotFound() {
        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "1 B");
        runner.assertValid();

        HashMap<String,String> props = new HashMap<>();
        props.put("rethinkdb.id", DOCUMENT_ID);

        runner.enqueue(new byte[]{}, props);

        runner.run(1,true,true);

        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_NOT_FOUND, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_NOT_FOUND);
        assertNotNull(flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE));
        flowFiles.get(0).assertAttributeEquals(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE,"Document with id '" + DOCUMENT_ID + "' not found");
    }

    @Test
    public void testBlankId() {
        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "1 B");
        runner.assertValid();
        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");
        Map<String,String> props = new HashMap<>();

        runner.enqueue(new byte[]{},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_FAILURE);
        assertNotNull(flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE));
        flowFiles.get(0).assertAttributeEquals(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE,GetRethinkDB.DOCUMENT_ID_EMPTY_MESSAGE);
    }

    @Test
    public void testNullId() {
        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "1 B");
        runner.assertValid();
        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, "${rethinkdb.id}");
        Map<String,String> props = new HashMap<>();
        props.put("rethinkdb.id", null);
        runner.enqueue(new byte[]{},props);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_FAILURE);
        assertNotNull(flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE));
        flowFiles.get(0).assertAttributeEquals(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE,GetRethinkDB.DOCUMENT_ID_EMPTY_MESSAGE);
    }

    @Test
    public void testValidSingleMessage() {
        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "1 MB");
        runner.assertValid();
        document = new HashMap<>();
        document.put("hello", "rethinkdb");

        HashMap<String,String> props = new HashMap<>();
        props.put("rethinkdb.id", DOCUMENT_ID);

        runner.enqueue(new byte[]{}, props);

        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractRethinkDBProcessor.REL_SUCCESS, 1);
        Gson gson = new Gson();

        String json = gson.toJson(document);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(json.toString());
        assertNull(flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE));

    }

    @Test
    public void testGetThrowsException() {
        mockGetRethinkDB = new GetRethinkDB() {
            @Override
            protected Connection makeConnection() {
                return null;
            }

            @Override
            protected Map<String,Object> getDocument(String id, String readMode) {
                throw new RuntimeException("testException");
            }
        };

        runner = TestRunners.newTestRunner(mockGetRethinkDB);
        runner.setProperty(AbstractRethinkDBProcessor.DB_NAME, "test");
        runner.setProperty(AbstractRethinkDBProcessor.DB_HOST, "host1");
        runner.setProperty(AbstractRethinkDBProcessor.DB_PORT, "1234");
        runner.setProperty(AbstractRethinkDBProcessor.USERNAME, "u1");
        runner.setProperty(AbstractRethinkDBProcessor.PASSWORD, "p1");
        runner.setProperty(AbstractRethinkDBProcessor.TABLE_NAME, "t1");
        runner.setProperty(AbstractRethinkDBProcessor.CHARSET, "UTF-8");
        runner.setProperty(GetRethinkDB.READ_MODE, "single");

        runner.setProperty(AbstractRethinkDBProcessor.MAX_DOCUMENTS_SIZE, "1 KB");
        runner.setProperty(GetRethinkDB.RETHINKDB_DOCUMENT_ID, DOCUMENT_ID);

        runner.assertValid();

        HashMap<String,String> props = new HashMap<>();
        props.put("rethinkdb.id", DOCUMENT_ID);

        runner.enqueue(new byte[]{}, props);

        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutRethinkDB.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractRethinkDBProcessor.REL_FAILURE);
        assertNotNull(flowFiles.get(0).getAttribute(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE));
        flowFiles.get(0).assertAttributeEquals(AbstractRethinkDBProcessor.RETHINKDB_ERROR_MESSAGE,"testException");
   }
}