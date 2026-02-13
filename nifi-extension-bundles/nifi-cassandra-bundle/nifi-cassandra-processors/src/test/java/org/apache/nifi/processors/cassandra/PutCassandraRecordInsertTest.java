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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PutCassandraRecordInsertTest {
    private MockPutCassandraRecordInsert testSubject;

    @Mock
    private RecordSchema schema;

    @BeforeEach
    public void setUp() {
        testSubject = new MockPutCassandraRecordInsert();
        testSubject.connectToCassandra(null);
    }

    @Test
    public void testGenerateInsert() {
        testGenerateInsert(
                "my_keyspace.my_table",
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("integerfield", 15),
                        new Tuple<>("longfield", 67L),
                        new Tuple<>("stringfield", "abcd")
                ),
                Arrays.asList(
                        new Tuple<>("keyfield", RecordFieldType.INT.getDataType()),
                        new Tuple<>("integerfield", RecordFieldType.INT.getDataType()),
                        new Tuple<>("longfield", RecordFieldType.LONG.getDataType()),
                        new Tuple<>("stringfield", RecordFieldType.STRING.getDataType())
                ),
                "INSERT INTO my_keyspace.my_table (keyfield,integerfield,longfield,stringfield) VALUES (?,?,?,?)",
                Arrays.asList(1, 15, 67L, "abcd")
        );
    }

    @Test
    public void testGenerateInsertStringArray() {
        testGenerateInsert(
                "my_keyspace.my_table",
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("integerfield", 15),
                        new Tuple<>("arrayfield", new Object[]{"test1", "test2"})
                ),
                Arrays.asList(
                        new Tuple<>("keyfield", RecordFieldType.INT.getDataType()),
                        new Tuple<>("integerfield", RecordFieldType.INT.getDataType()),
                        new Tuple<>("arrayfield", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()))
                ),
                "INSERT INTO my_keyspace.my_table (keyfield,integerfield,arrayfield) VALUES (?,?,?)",
                Arrays.asList(1, 15, Arrays.asList("test1", "test2"))
        );
    }

    @Test
    public void testGenerateInsertTableNotFound() {
        Map<String, Object> recordContentMap = Collections.singletonMap("keyfield", 1);

        assertThrows(org.apache.nifi.processor.exception.ProcessException.class, () -> {
            testSubject.generateInsert("wrong_keyspace.wrong_table", schema, recordContentMap);
        });
    }

    @Test
    public void testGenerateInsertWithUUID() {
        String uuidString = "550e8400-e29b-41d4-a716-446655440000";
        testGenerateInsert(
                "my_keyspace.my_table",
                Arrays.asList(
                        new Tuple<>("keyfield", 1),
                        new Tuple<>("uuidfield", uuidString)
                ),
                Arrays.asList(
                        new Tuple<>("keyfield", RecordFieldType.INT.getDataType()),
                        new Tuple<>("uuidfield", RecordFieldType.STRING.getDataType())
                ),
                "INSERT INTO my_keyspace.my_table (keyfield,uuidfield) VALUES (?,?)",
                Arrays.asList(1, java.util.UUID.fromString(uuidString))
        );
    }

    @Test
    public void testGenerateInsertWithNulls() {
        testGenerateInsert(
                "my_keyspace.my_table",
                Arrays.asList(
                        new Tuple<>("keyfield", 101),
                        new Tuple<>("stringfield", null)
                ),
                Arrays.asList(
                        new Tuple<>("keyfield", RecordFieldType.INT.getDataType()),
                        new Tuple<>("stringfield", RecordFieldType.STRING.getDataType())
                ),
                "INSERT INTO my_keyspace.my_table (keyfield,stringfield) VALUES (?,?)",
                Arrays.asList(101, null)
        );
    }

    @Test
    public void testGenerateInsertQuotedTable() {
        testGenerateInsert(
                "my_keyspace.\"MyTable\"",
                Collections.singletonList(new Tuple<>("keyfield", 1)),
                Collections.singletonList(new Tuple<>("keyfield", RecordFieldType.INT.getDataType())),
                "INSERT INTO my_keyspace.\"MyTable\" (keyfield) VALUES (?)",
                Collections.singletonList(1)
        );
    }

    @Test
    public void testGenerateInsertTypeMismatch() {
        assertThrows(ProcessException.class, () -> {
            testGenerateInsert(
                    "my_keyspace.my_table",
                    Arrays.asList(
                            new Tuple<>("keyfield", 1),
                            new Tuple<>("integerfield", "this_is_not_a_number")
                    ),
                    Arrays.asList(
                            new Tuple<>("keyfield", RecordFieldType.INT.getDataType()),
                            new Tuple<>("integerfield", RecordFieldType.STRING.getDataType())
                    ),
                    "THIS WILL NOT BE REACHED",
                    null
            );
        });
    }

    private void testGenerateInsert(
            String table,
            List<Tuple<String, Object>> records,
            List<Tuple<String, org.apache.nifi.serialization.record.DataType>> recordSchema,
            String expectedCql,
            List<Object> expectedValues
    ) {
        Map<String, Object> recordContentMap = new LinkedHashMap<>();
        for (Tuple<String, Object> t : records) {
            recordContentMap.put(t.getKey(), t.getValue());
        }

        Map<String, org.apache.nifi.serialization.record.DataType> recordSchemaMap =
                recordSchema.stream()
                        .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));

        List<String> fieldNames = records.stream()
                .map(Tuple::getKey)
                .collect(Collectors.toList());

        when(schema.getFieldNames()).thenReturn(fieldNames);
        when(schema.getDataType(anyString()))
                .thenAnswer(i -> Optional.of(recordSchemaMap.get(i.getArgument(0))));

        Statement<?> stmt = testSubject.generateInsert(table, schema, recordContentMap);
        assertTrue(stmt instanceof SimpleStatement, "Expected SimpleStatement");

        SimpleStatement simple = (SimpleStatement) stmt;

        assertEquals(expectedCql, simple.getQuery());

        List<Object> actualValues = simple.getPositionalValues();
        assertEquals(expectedValues.size(), actualValues.size(), "Bind value count mismatch");

        for (int i = 0; i < expectedValues.size(); i++) {
            Object expected = expectedValues.get(i);
            Object actual = actualValues.get(i);

            if (expected instanceof Number && actual instanceof Number) {
                assertEquals(
                        ((Number) expected).doubleValue(),
                        ((Number) actual).doubleValue(),
                        0.0001,
                        "Numeric mismatch at index " + i
                );
            } else {
                assertEquals(
                        String.valueOf(expected),
                        String.valueOf(actual),
                        "Value mismatch at index " + i
                );
            }
        }
    }

    private static class MockPutCassandraRecordInsert extends PutCassandraRecord {

        private CqlSession mockSession;

        @Override
        protected void connectToCassandra(ProcessContext context) {
            mockSession = mock(CqlSession.class);

            try {
                lenient().when(mockSession.getKeyspace())
                        .thenReturn(Optional.of(CqlIdentifier.fromCql("sampleks")));

                Metadata metadata = mock(Metadata.class);
                KeyspaceMetadata mockKeyspace = mock(KeyspaceMetadata.class);
                TableMetadata mockTable = mock(TableMetadata.class);

                lenient().when(mockSession.getMetadata()).thenReturn(metadata);
                lenient().when(metadata.getKeyspace(anyString()))
                        .thenReturn(Optional.of(mockKeyspace));
                lenient().when(mockKeyspace.getTable(anyString()))
                        .thenReturn(Optional.of(mockTable));

                ColumnMetadata keyCol = mock(ColumnMetadata.class);
                lenient().when(keyCol.getType()).thenReturn(DataTypes.INT);
                lenient().when(keyCol.getName()).thenReturn(CqlIdentifier.fromCql("keyfield"));

                ColumnMetadata intCol = mock(ColumnMetadata.class);
                lenient().when(intCol.getType()).thenReturn(DataTypes.INT);
                lenient().when(intCol.getName()).thenReturn(CqlIdentifier.fromCql("integerfield"));

                ColumnMetadata longCol = mock(ColumnMetadata.class);
                lenient().when(longCol.getType()).thenReturn(DataTypes.BIGINT);
                lenient().when(longCol.getName()).thenReturn(CqlIdentifier.fromCql("longfield"));

                ColumnMetadata stringCol = mock(ColumnMetadata.class);
                lenient().when(stringCol.getType()).thenReturn(DataTypes.TEXT);
                lenient().when(stringCol.getName()).thenReturn(CqlIdentifier.fromCql("stringfield"));

                ColumnMetadata arrayCol = mock(ColumnMetadata.class);
                lenient().when(arrayCol.getType())
                        .thenReturn(DataTypes.listOf(DataTypes.TEXT));
                lenient().when(arrayCol.getName())
                        .thenReturn(CqlIdentifier.fromCql("arrayfield"));

                Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
                columns.put(keyCol.getName(), keyCol);
                columns.put(intCol.getName(), intCol);
                columns.put(longCol.getName(), longCol);
                columns.put(stringCol.getName(), stringCol);
                columns.put(arrayCol.getName(), arrayCol);

                lenient().when(mockTable.getColumns()).thenReturn(columns);

                this.cassandraSession.set(mockSession);

            } catch (Exception e) {
                fail("Failed to setup mock Cassandra session: " + e.getMessage());
            }
        }
    }
}
