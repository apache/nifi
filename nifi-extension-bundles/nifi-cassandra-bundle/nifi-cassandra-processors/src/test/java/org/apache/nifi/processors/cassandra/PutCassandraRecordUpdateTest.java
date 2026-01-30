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
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class PutCassandraRecordUpdateTest {

    private PutCassandraRecord testSubject;
    private RecordSchema schema;

    @BeforeEach
    void setUp() {
        MockPutCassandraRecordUpdate processor = new MockPutCassandraRecordUpdate();
        processor.connectToCassandra(null);
        this.testSubject = processor;
    }

    @Test
    public void testGenerateUpdateWithEmptyKeyList() {
        Stream.of("", ",", ",,,").forEach(updateKeys -> testGenerateUpdate(
                "testks.testtbl",
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
                "my_keyspace.mytable",
                "keyfield,missingKeyField",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("stringfield", "newStringValue")
                ),
                new IllegalArgumentException("Update key 'missingKeyField' missing from record data")
        );
    }

    @Test
    public void testGenerateUpdateWithInvalidUpdateMethod() {
        testGenerateUpdate(
                "sampleks.my_table",
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
    public void testGenerateUpdateSimpleTableName() {
        testGenerateUpdate(
                "my_table",
                "keyfield",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("stringField", "newStringValue")
                ),
                "UPDATE my_table SET stringfield=? WHERE keyfield=?",
                Arrays.asList("newStringValue", 1)
        );
    }

    @Test
    public void testGenerateUpdateKeyspaceTableName() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("stringfield", "newStringValue")
                ),
                "UPDATE my_keyspace.my_table SET stringfield=? WHERE keyfield=?",
                Arrays.asList("newStringValue", 1)
        );
    }

    @Test
    public void testGenerateUpdateMultipleKeys() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield,keyfield2,keyfield3",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("keyfield2", "key2"),
                        new Tuple<>("keyfield3", 123L),
                        new Tuple<>("stringfield", "newStringValue")
                ),
                "UPDATE my_keyspace.my_table SET stringfield=? WHERE keyfield=? AND keyfield2=? AND keyfield3=?",
                Arrays.asList("newStringValue", 1, "key2", 123L)
        );
    }

    @Test
    public void testGenerateUpdateIncrementLong() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("longfield", 15L)
                ),
                "UPDATE my_keyspace.my_table SET longfield=longfield+? WHERE keyfield=?",
                Arrays.asList(15L, 1)
        );
    }

    @Test
    public void testGenerateUpdateDecrementLong() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield",
                PutCassandraRecord.DECR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("longfield", 15L)
                ),
                "UPDATE my_keyspace.my_table SET longfield=longfield-? WHERE keyfield=?",
                Arrays.asList(15L, 1)
        );
    }

    @Test
    public void testGenerateUpdateIncrementInteger() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("integerfield", 15)
                ),
                "UPDATE my_keyspace.my_table SET integerfield=integerfield+? WHERE keyfield=?",
                Arrays.asList(15, 1)
        );
    }

    @Test
    public void testGenerateUpdateIncrementFloat() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("floatfield", 15.05F)
                ),
                "UPDATE my_keyspace.my_table SET floatfield=floatfield+? WHERE keyfield=?",
                Arrays.asList(15.05F, 1)
        );
    }

    @Test
    public void testGenerateUpdateIncrementDouble() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("doublefield", 15.05D)
                ),
                "UPDATE my_keyspace.my_table SET doublefield=doublefield+? WHERE keyfield=?",
                Arrays.asList(15.05D, 1)
        );
    }

    @Test
    public void testGenerateUpdateSetMultipleValues() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield",
                PutCassandraRecord.SET_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("stringfield", "newStringValue"),
                        new Tuple<>("integerfield", 15),
                        new Tuple<>("longfield", 67L)
                ),
                "UPDATE my_keyspace.my_table SET stringfield=?, integerfield=?, longfield=? WHERE keyfield=?",
                Arrays.asList("newStringValue", 15, 67L, 1)
        );
    }

    @Test
    public void testGenerateUpdateIncrementMultipleValues() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield",
                PutCassandraRecord.INCR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("integerfield", 15),
                        new Tuple<>("longfield", 67L)
                ),
                "UPDATE my_keyspace.my_table SET integerfield=integerfield+?, longfield=longfield+? WHERE keyfield=?",
                Arrays.asList(15, 67L, 1)
        );
    }

    @Test
    public void testGenerateUpdateDecrementMultipleValues() {
        testGenerateUpdate(
                "my_keyspace.my_table",
                "keyfield",
                PutCassandraRecord.DECR_TYPE.getValue(),
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("integerfield", 15),
                        new Tuple<>("longfield", 67L)
                ),
                "UPDATE my_keyspace.my_table SET integerfield=integerfield-?, longfield=longfield-? WHERE keyfield=?",
                Arrays.asList(15, 67L, 1)
        );
    }

    private void testGenerateUpdate(
            String table,
            String updateKeys,
            String updateMethod,
            List<Tuple<String, Object>> records,
            String expectedCql,
            List<Object> expectedValues
    ) {
        Map<String, Object> recordContentMap = new LinkedHashMap<>();
        for (Tuple<String, Object> tuple : records) {
            recordContentMap.put(tuple.getKey(), tuple.getValue());
        }

        Statement<?> actual = testSubject.generateUpdate(
                table,
                schema,
                updateKeys,
                updateMethod,
                recordContentMap
        );

        assertTrue(actual instanceof SimpleStatement, "Expected a SimpleStatement");
        SimpleStatement stmt = (SimpleStatement) actual;

        assertEquals(expectedCql, stmt.getQuery());

        if (expectedValues != null) {
            List<Object> actualValues = stmt.getPositionalValues();
            assertEquals(expectedValues.size(), actualValues.size(), "Count of bound values mismatch");

            for (int i = 0; i < expectedValues.size(); i++) {
                Object expected = expectedValues.get(i);
                Object actualVal = actualValues.get(i);

                if (expected instanceof Number && actualVal instanceof Number) {
                    assertEquals(((Number) expected).doubleValue(), ((Number) actualVal).doubleValue(), 0.001,
                            "Numeric mismatch at index " + i);
                } else {
                    assertEquals(String.valueOf(expected), String.valueOf(actualVal),
                            "Value mismatch at index " + i);
                }
            }
        }
    }

    private <E extends Exception> void testGenerateUpdate(
            String table,
            String updateKeys,
            String updateMethod,
            List<Tuple<String, Object>> records,
            E expected
    ) {
        Map<String, Object> recordContentMap = records.stream()
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));

        Exception actual = assertThrows(
                expected.getClass(),
                () -> testSubject.generateUpdate(
                        table,
                        schema,
                        updateKeys,
                        updateMethod,
                        recordContentMap
                )
        );

        assertEquals(expected.getMessage(), actual.getMessage());
    }

    private static class MockPutCassandraRecordUpdate extends PutCassandraRecord {

        private CqlSession mockSession;

        @Override
        protected void connectToCassandra(ProcessContext context) {
            mockSession = mock(CqlSession.class);

            try {
                lenient().when(mockSession.getKeyspace()).thenReturn(Optional.of(CqlIdentifier.fromCql("sampleks")));

                Metadata metadata = mock(Metadata.class);
                KeyspaceMetadata mockKeyspace = mock(KeyspaceMetadata.class);
                TableMetadata mockTable = mock(TableMetadata.class);

                lenient().when(mockSession.getMetadata()).thenReturn(metadata);
                lenient().when(metadata.getKeyspace(anyString())).thenReturn(Optional.of(mockKeyspace));
                lenient().when(mockKeyspace.getTable(anyString())).thenReturn(Optional.of(mockTable));

                ColumnMetadata nameCol = mock(ColumnMetadata.class);
                lenient().when(nameCol.getType()).thenReturn(DataTypes.TEXT);
                lenient().when(nameCol.getName()).thenReturn(CqlIdentifier.fromCql("keyfield"));

                ColumnMetadata key2Col = mock(ColumnMetadata.class);
                lenient().when(key2Col.getType()).thenReturn(DataTypes.TEXT);
                lenient().when(key2Col.getName()).thenReturn(CqlIdentifier.fromCql("keyfield2"));

                ColumnMetadata key3Col = mock(ColumnMetadata.class);
                lenient().when(key3Col.getType()).thenReturn(DataTypes.BIGINT);
                lenient().when(key3Col.getName()).thenReturn(CqlIdentifier.fromCql("keyfield3"));


                ColumnMetadata stringCol = mock(ColumnMetadata.class);
                lenient().when(stringCol.getType()).thenReturn(DataTypes.TEXT);
                lenient().when(stringCol.getName()).thenReturn(CqlIdentifier.fromCql("stringfield"));

                ColumnMetadata intCol = mock(ColumnMetadata.class);
                lenient().when(intCol.getType()).thenReturn(DataTypes.INT);
                lenient().when(intCol.getName()).thenReturn(CqlIdentifier.fromCql("integerfield"));

                ColumnMetadata longCol = mock(ColumnMetadata.class);
                lenient().when(longCol.getType()).thenReturn(DataTypes.BIGINT);
                lenient().when(longCol.getName()).thenReturn(CqlIdentifier.fromCql("longfield"));

                Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
                columns.put(nameCol.getName(), nameCol);
                columns.put(stringCol.getName(), stringCol);
                columns.put(intCol.getName(), intCol);
                columns.put(longCol.getName(), longCol);
                columns.put(key2Col.getName(), key2Col);
                columns.put(key3Col.getName(), key3Col);

                lenient().when(mockTable.getColumns()).thenReturn(columns);

                this.cassandraSession.set(mockSession);

            } catch (Exception e) {
                fail("Failed to setup mock Cassandra session: " + e.getMessage());
            }
        }
    }
}
