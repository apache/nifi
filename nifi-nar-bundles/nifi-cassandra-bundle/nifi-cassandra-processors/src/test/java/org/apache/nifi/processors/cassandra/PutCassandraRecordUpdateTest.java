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
package org.apache.nifi.processors.cassandra;

import com.datastax.driver.core.Statement;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class PutCassandraRecordUpdateTest {
    private PutCassandraRecord testSubject;

    @Mock
    private RecordSchema schema;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        testSubject = new PutCassandraRecord();
    }

    @Test
    public void testGenerateUpdateWithEmptyKeyList() {
        Stream.of("", ",", ",,,").forEach(updateKeys -> testGenerateUpdate(
                "keyspace.table",
                updateKeys,
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("stringField", "newStringValue")
                ),
                new IllegalArgumentException("No Update Keys were specified")
        ));
    }

    @Test
    public void testGenerateUpdateWithMissingKey() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField,missingKeyField",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("stringField", "newStringValue")
                ),
                new IllegalArgumentException("Update key 'missingKeyField' is not present in the record schema")
        );
    }

    @Test
    public void testGenerateUpdateWithInvalidUpdateMethod() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                "invalidUpdateMethod",
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("longField", 15L)
                ),
                new IllegalArgumentException("Update Method 'invalidUpdateMethod' is not valid.")
        );
    }

    @Test
    public void testGenerateUpdateIncrementString() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("stringField", "15")
                ),
                new IllegalArgumentException("Field 'stringField' is not of type Number")
        );
    }

    @Test
    public void testGenerateUpdateSimpleTableName() {
        testGenerateUpdate(
                "table",
                "keyField1",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField1", 1),
                        new Tuple<>("stringField", "newStringValue")
                ),
                "UPDATE table SET stringField='newStringValue' WHERE keyField1=1;"
        );
    }

    @Test
    public void testGenerateUpdateKeyspacedTableName() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField1",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField1", 1),
                        new Tuple<>("stringField", "newStringValue")
                ),
                "UPDATE keyspace.table SET stringField='newStringValue' WHERE keyField1=1;"
        );
    }

    @Test
    public void testGenerateUpdateMultipleKeys() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField1,keyField2,keyField3",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField1", 1),
                        new Tuple<>("keyField2", "key2"),
                        new Tuple<>("keyField3", 123L),
                        new Tuple<>("stringField", "newStringValue")
                ),
                "UPDATE keyspace.table SET stringField='newStringValue' WHERE keyField1=1 AND keyField2='key2' AND keyField3=123;"
        );
    }

    @Test
    public void testGenerateUpdateIncrementLong() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("longField", 15L)
                ),
                "UPDATE keyspace.table SET longField=longField+15 WHERE keyField=1;"
        );
    }

    @Test
    public void testGenerateUpdateDecrementLong() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                PutCassandraRecord.DECR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("longField", 15L)
                ),
                "UPDATE keyspace.table SET longField=longField-15 WHERE keyField=1;"
        );
    }

    @Test
    public void testGenerateUpdateIncrementInteger() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("integerField", 15)
                ),
                "UPDATE keyspace.table SET integerField=integerField+15 WHERE keyField=1;"
        );
    }

    @Test
    public void testGenerateUpdateIncrementFloat() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("floatField", 15.05F)
                ),
                "UPDATE keyspace.table SET floatField=floatField+15 WHERE keyField=1;"
        );
    }

    @Test
    public void testGenerateUpdateIncrementDouble() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("doubleField", 15.05D)
                ),
                "UPDATE keyspace.table SET doubleField=doubleField+15 WHERE keyField=1;"
        );
    }

    @Test
    public void testGenerateUpdateSetMultipleValues() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("stringField", "newStringValue"),
                        new Tuple<>("integerField", 15),
                        new Tuple<>("longField", 67L)
                ),
                "UPDATE keyspace.table SET stringField='newStringValue',integerField=15,longField=67 WHERE keyField=1;"
        );
    }

    @Test
    public void testGenerateUpdateIncrementMultipleValues() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("integerField", 15),
                        new Tuple<>("longField", 67L)
                ),
                "UPDATE keyspace.table SET integerField=integerField+15,longField=longField+67 WHERE keyField=1;"
        );
    }

    @Test
    public void testGenerateUpdateDecrementMultipleValues() {
        testGenerateUpdate(
                "keyspace.table",
                "keyField",
                PutCassandraRecord.DECR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("integerField", 15),
                        new Tuple<>("longField", 67L)
                ),
                "UPDATE keyspace.table SET integerField=integerField-15,longField=longField-67 WHERE keyField=1;"
        );
    }

    private void testGenerateUpdate(String table, String updateKeys, String updateMethod, List<Tuple<String, Object>> records, String expected) {
        Map<String, Object> recordContentMap = records.stream()
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));

        List<String> fieldNames = records.stream().map(Tuple::getKey).collect(Collectors.toList());

        when(schema.getFieldNames()).thenReturn(fieldNames);
        Statement actual = testSubject.generateUpdate(table, schema, updateKeys, updateMethod, recordContentMap);

        assertEquals(expected, actual.toString());
    }

    private <E extends Exception> void testGenerateUpdate(String table, String updateKeys, String updateMethod, List<Tuple<String, Object>> records, E expected) {
        Map<String, Object> recordContentMap = records.stream()
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));

        List<String> fieldNames = records.stream().map(Tuple::getKey).collect(Collectors.toList());

        when(schema.getFieldNames()).thenReturn(fieldNames);
        try {
            testSubject.generateUpdate("keyspace.table", schema, updateKeys, updateMethod, recordContentMap);
            fail();
        } catch (Exception e) {
            assertEquals(expected.getClass(), e.getClass());
            assertEquals(expected.getMessage(), e.getMessage());
        }
    }
}
