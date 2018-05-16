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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.json.simple.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.net.Connection;
import net.minidev.json.JSONObject;

public class TestPutRethinkDB {
    private TestRunner runner;
    private PutRethinkDB mockPutRethinkDB;
    protected HashMap<String,Object> result = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        result.put(PutRethinkDB.RESULT_DELETED_KEY, 0L);
        result.put(PutRethinkDB.RESULT_ERROR_KEY, 0L);
        result.put(PutRethinkDB.RESULT_GENERATED_KEYS_KEY,
                "[\"4056b236-1d8a-492a-b67f-8cf99e363cc6\"]");
        result.put(PutRethinkDB.RESULT_INSERTED_KEY, 1L);
        result.put(PutRethinkDB.RESULT_REPLACED_KEY, 0L);
        result.put(PutRethinkDB.RESULT_SKIPPED_KEY, 0L);
        result.put(PutRethinkDB.RESULT_UNCHANGED_KEY, 0L);
        result.put(PutRethinkDB.RESULT_FIRST_ERROR_KEY, "");
        result.put(PutRethinkDB.RESULT_WARNINGS_KEY, "");
        mockPutRethinkDB = new PutRethinkDB() {
            @Override
            protected Connection makeConnection() {
                return null;
            }
            @Override
            protected HashMap<String, Object> runInsert(Insert insert) {
                return result;
            }
        };
        runner = TestRunners.newTestRunner(mockPutRethinkDB);
        runner.setProperty(PutRethinkDB.DB_NAME, "test");
        runner.setProperty(PutRethinkDB.DB_HOST, "host1");
        runner.setProperty(PutRethinkDB.DB_PORT, "1234");
        runner.setProperty(PutRethinkDB.USERNAME, "u1");
        runner.setProperty(PutRethinkDB.PASSWORD, "p1");
        runner.setProperty(PutRethinkDB.TABLE_NAME, "t1");
        runner.setProperty(PutRethinkDB.CHARSET, "UTF-8");
        runner.setProperty(PutRethinkDB.CONFLICT_STRATEGY, PutRethinkDB.CONFLICT_STRATEGY_UPDATE);
        runner.setProperty(PutRethinkDB.DURABILITY, PutRethinkDB.DURABILITY_HARD);
        runner.setProperty(PutRethinkDB.MAX_DOCUMENTS_SIZE, "1 KB");
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
        runner.setProperty(PutRethinkDB.DB_HOST, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyPort() {
        runner.setProperty(PutRethinkDB.DB_PORT, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyDBName() {
        runner.setProperty(PutRethinkDB.DB_NAME, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyUsername() {
        runner.setProperty(PutRethinkDB.USERNAME, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyPassword() {
        runner.setProperty(PutRethinkDB.PASSWORD, "p1");
        runner.assertValid();
    }

    @Test
    public void testCharsetUTF8() {
        runner.setProperty(PutRethinkDB.CHARSET, "UTF-8");
        runner.assertValid();
    }

    @Test
    public void testCharsetBlank() {
        runner.setProperty(PutRethinkDB.CHARSET, "");
        runner.assertNotValid();
    }
    @Test
    public void testZeroMaxDocumentSize() {
        runner.setProperty(PutRethinkDB.MAX_DOCUMENTS_SIZE, "0");
        runner.assertNotValid();
    }

    @Test
    public void testBlankConflictStrategy() {
        runner.setProperty(PutRethinkDB.CONFLICT_STRATEGY, "");
        runner.assertNotValid();
    }

    @Test
    public void testBlankDurability() {
        runner.setProperty(PutRethinkDB.DURABILITY, "");
        runner.assertNotValid();
    }

    @Test
    public void testSizeGreaterThanThreshold() {
        runner.setProperty(PutRethinkDB.MAX_DOCUMENTS_SIZE, "1 B");
        runner.assertValid();
        byte [] bytes = new byte[2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 'a';
        }
        runner.enqueue(bytes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutRethinkDB.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_FAILURE);
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_ERROR_MESSAGE));
    }

    @Test
    public void testValidSingleMessage() {
        runner.setProperty(PutRethinkDB.MAX_DOCUMENTS_SIZE, "1 MB");
        runner.assertValid();
        result.remove(PutRethinkDB.RESULT_FIRST_ERROR_KEY);
        result.remove(PutRethinkDB.RESULT_WARNINGS_KEY);
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
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"null");
    }

    @Test
    public void testInsertThrowsException() {
        mockPutRethinkDB = new PutRethinkDB() {
            @Override
            protected Connection makeConnection() {
                return null;
            }
            @Override
            protected HashMap<String, Object> runInsert(Insert insert) {
                throw new RuntimeException("testException");
            }
        };
        runner = TestRunners.newTestRunner(mockPutRethinkDB);
        runner.setProperty(PutRethinkDB.DB_NAME, "test");
        runner.setProperty(PutRethinkDB.DB_HOST, "host1");
        runner.setProperty(PutRethinkDB.DB_PORT, "1234");
        runner.setProperty(PutRethinkDB.USERNAME, "u1");
        runner.setProperty(PutRethinkDB.PASSWORD, "p1");
        runner.setProperty(PutRethinkDB.TABLE_NAME, "t1");
        runner.setProperty(PutRethinkDB.CHARSET, "UTF-8");
        runner.setProperty(PutRethinkDB.CONFLICT_STRATEGY, PutRethinkDB.CONFLICT_STRATEGY_UPDATE);
        runner.setProperty(PutRethinkDB.DURABILITY, PutRethinkDB.DURABILITY_HARD);
        runner.setProperty(PutRethinkDB.MAX_DOCUMENTS_SIZE, "1 KB");
        runner.assertValid();

        JSONObject message = new JSONObject();
        message.put("hello", "rethinkdb");
        byte [] bytes = message.toJSONString().getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutRethinkDB.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_ERROR_MESSAGE),"testException");
    }

    @Test(expected=AssertionError.class)
    public void testMakeConnectionThrowsException() {
        mockPutRethinkDB = new PutRethinkDB() {
            @Override
            protected Connection makeConnection() {
                throw new RuntimeException("testException");
            }
        };
        runner = TestRunners.newTestRunner(mockPutRethinkDB);
        runner.setProperty(PutRethinkDB.DB_NAME, "test");
        runner.setProperty(PutRethinkDB.DB_HOST, "host1");
        runner.setProperty(PutRethinkDB.DB_PORT, "1234");
        runner.setProperty(PutRethinkDB.USERNAME, "u1");
        runner.setProperty(PutRethinkDB.PASSWORD, "p1");
        runner.setProperty(PutRethinkDB.TABLE_NAME, "t1");
        runner.setProperty(PutRethinkDB.CHARSET, "UTF-8");
        runner.setProperty(PutRethinkDB.CONFLICT_STRATEGY, PutRethinkDB.CONFLICT_STRATEGY_UPDATE);
        runner.setProperty(PutRethinkDB.DURABILITY, PutRethinkDB.DURABILITY_HARD);
        runner.setProperty(PutRethinkDB.MAX_DOCUMENTS_SIZE, "1 KB");
        runner.assertValid();
        JSONObject message = new JSONObject();
        message.put("hello", "rethinkdb");

        byte [] bytes = message.toJSONString().getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
    }

    @Test
    public void testMessageError() {
        runner.setProperty(PutRethinkDB.MAX_DOCUMENTS_SIZE, "1 MB");
        runner.assertValid();
        result.put(PutRethinkDB.RESULT_FIRST_ERROR_KEY,"Error occured");
        result.put(PutRethinkDB.RESULT_ERROR_KEY,1L);
        result.put(PutRethinkDB.RESULT_INSERTED_KEY,0L);
        result.remove(PutRethinkDB.RESULT_WARNINGS_KEY);
        JSONObject message = new JSONObject();
        message.put("hello", "rethinkdb");
        byte [] bytes = message.toJSONString().getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutRethinkDB.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), "0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY),"1");
        assertNotNull(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY));
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY),"0");
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY),"Error occured");
    }

    @Test
    public void testValidArrayMessage() {
        runner.setProperty(PutRethinkDB.MAX_DOCUMENTS_SIZE, "1 MB");
        runner.assertValid();
        result.remove(PutRethinkDB.RESULT_FIRST_ERROR_KEY);
        result.remove(PutRethinkDB.RESULT_WARNINGS_KEY);
        result.put(PutRethinkDB.RESULT_INSERTED_KEY,2L);
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
    }
    @Test
    public void testInvalidSingleMessage() {
        byte [] bytes = "bad json".getBytes();

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutRethinkDB.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_FAILURE);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_ERROR_MESSAGE), "null");
    }

    @Test
    public void testInvalidEmptySingleMessage() {
        byte [] bytes = "".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutRethinkDB.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutRethinkDB.REL_FAILURE);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY), null);
        assertEquals(flowFiles.get(0).getAttribute(PutRethinkDB.RETHINKDB_ERROR_MESSAGE), "Empty message size 0");
    }

}