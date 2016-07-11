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
package org.apache.nifi.processors.standard;

import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.apache.nifi.processors.standard.db.impl.GenericDatabaseAdapter;
import org.apache.nifi.processors.standard.db.impl.OracleDatabaseAdapter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Unit tests for the GenerateTableFetch processor.
 */
public class TestGenerateTableFetch {

    private GenerateTableFetch processor;
    private TestRunner runner;
    private DatabaseAdapter dbAdapter;

    @Before
    public void setUp() throws Exception {
        processor = new GenerateTableFetch();
        runner = TestRunners.newTestRunner(processor);
        dbAdapter = new GenericDatabaseAdapter();
    }

    @Test
    public void getSelectFromClauseGeneric() throws Exception {
        assertEquals("SELECT * FROM myTable", dbAdapter.getSelectStatement("myTable", null, null, null, null, null));
        assertEquals("SELECT myCol1, myCol2 FROM myTable", dbAdapter.getSelectStatement("myTable", "myCol1, myCol2", null, null, null, null));
        assertEquals("SELECT * FROM myTable LIMIT 10", dbAdapter.getSelectStatement("myTable", null, null, null, 10, null));
        assertEquals("SELECT * FROM myTable OFFSET 10", dbAdapter.getSelectStatement("myTable", null, null, null, null, 10));
        assertEquals("SELECT * FROM myTable LIMIT 2 OFFSET 10", dbAdapter.getSelectStatement("myTable", null, null, null, 2, 10));
    }

    @Test
    public void getSelectFromClauseOracle() throws Exception {
        dbAdapter = new OracleDatabaseAdapter();
        assertEquals("SELECT * FROM myTable",
                dbAdapter.getSelectStatement("myTable", null, null, null, null, null));
        assertEquals("SELECT myCol1, myCol2 FROM myTable",
                dbAdapter.getSelectStatement("myTable", "myCol1, myCol2", null, null, null, null));
        assertEquals("SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM myTable) a WHERE ROWNUM <= 10) WHERE rnum > 0",
                dbAdapter.getSelectStatement("myTable", null, null, null, 10, null));
        assertEquals("SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM myTable) a) WHERE rnum > 10",
                dbAdapter.getSelectStatement("myTable", null, null, null, null, 10));
        assertEquals("SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM myTable) a WHERE ROWNUM <= 12) WHERE rnum > 10",
                dbAdapter.getSelectStatement("myTable", null, null, null, 2, 10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void getSelectFromClauseNullTableName() throws Exception {
        assertEquals("SELECT * FROM myTable", dbAdapter.getSelectStatement(null, null, null, null, null, null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void getSelectFromClauseEmptyTableName() throws Exception {
        assertEquals("SELECT * FROM myTable", dbAdapter.getSelectStatement("", null, null, null, null, null));
    }

    @Test
    public void testOnTrigger() throws Exception {
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "5");
        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("db.table.fullname", "person");
            put("db.table.count", "100");
        }});

        runner.run();
        runner.assertTransferCount(GenerateTableFetch.REL_SUCCESS, 20);
        runner.assertTransferCount(GenerateTableFetch.REL_ORIGINAL, 1);
    }

    @Test
    public void testOnTriggerExtraPartition() throws Exception {
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "3");
        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("db.table.fullname", "person");
            put("db.table.count", "100");
        }});

        runner.run();
        runner.assertTransferCount(GenerateTableFetch.REL_SUCCESS, 34);
        runner.assertTransferCount(GenerateTableFetch.REL_ORIGINAL, 1);
    }

    @Test
    public void testOnTriggerFetchAll() throws Exception {
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "0");
        runner.setProperty(GenerateTableFetch.COLUMN_NAMES, "id, col1");
        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("db.table.fullname", "person");
            put("db.table.count", "100");
        }});

        runner.run();
        runner.assertTransferCount(GenerateTableFetch.REL_SUCCESS, 1);
        runner.assertTransferCount(GenerateTableFetch.REL_ORIGINAL, 1);
        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(GenerateTableFetch.REL_SUCCESS).get(0);
        assertNotNull(resultFlowFile);
        resultFlowFile.assertContentEquals("SELECT id, col1 FROM person");
    }

    @Test
    public void testOnTriggerNoCount() throws Exception {
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "10");
        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("db.table.fullname", "person");
        }});

        runner.run();
        runner.assertTransferCount(GenerateTableFetch.REL_FAILURE, 1);
        runner.assertTransferCount(GenerateTableFetch.REL_ORIGINAL, 0);
        runner.assertTransferCount(GenerateTableFetch.REL_SUCCESS, 0);
    }

    @Test
    public void testOnTriggerNoTableName() throws Exception {
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "10");
        runner.enqueue(new byte[0]);

        runner.run();
        runner.assertTransferCount(GenerateTableFetch.REL_FAILURE, 1);
        runner.assertTransferCount(GenerateTableFetch.REL_ORIGINAL, 0);
        runner.assertTransferCount(GenerateTableFetch.REL_SUCCESS, 0);
    }

    @Test
    public void testOnTriggerNoIncomingFlowFile() throws Exception {
        runner.setIncomingConnection(false);
        runner.run();
        processor.getRelationships().forEach(rel -> runner.assertTransferCount(rel, 0));
    }
}