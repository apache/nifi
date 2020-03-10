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

import com.datastax.driver.core.querybuilder.Insert;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class PutCassandraRecordInsertTest {
    private PutCassandraRecord testSubject;

    @Mock
    private RecordSchema schema;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        testSubject = new PutCassandraRecord();
    }

    @Test
    public void testGenerateInsert() {
        testGenerateInsert(
                "keyspace.table",
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("integerField", 15),
                        new Tuple<>("longField", 67L),
                        new Tuple<>("stringField", "abcd")
                ),
                Arrays.asList(
                        new Tuple<>("keyField", RecordFieldType.INT.getDataType()),
                        new Tuple<>("integerField", RecordFieldType.INT.getDataType()),
                        new Tuple<>("longField", RecordFieldType.LONG.getDataType()),
                        new Tuple<>("stringField", RecordFieldType.STRING.getDataType())
                ),
                "INSERT INTO keyspace.table (keyField,integerField,longField,stringField) VALUES (1,15,67,'abcd');"
        );
    }

    @Test
    public void testGenerateInsertStringArray() {
        testGenerateInsert(
                "keyspace.table",
                Arrays.asList(
                        new Tuple<>("keyField", 1),
                        new Tuple<>("integerField", 15),
                        new Tuple<>("arrayField", new Object[]{"test1", "test2"})
                ),
                Arrays.asList(
                        new Tuple<>("keyField", RecordFieldType.INT.getDataType()),
                        new Tuple<>("integerField", RecordFieldType.INT.getDataType()),
                        new Tuple<>("arrayField", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()))
                ),
                "INSERT INTO keyspace.table (keyField,integerField,arrayField) VALUES (1,15,['test1','test2']);"
        );
    }

    private void testGenerateInsert(String table, List<Tuple<String, Object>> records, List<Tuple<String, org.apache.nifi.serialization.record.DataType>> recordSchema, String expected) {
        Map<String, Object> recordContentMap = records.stream()
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));

        Map<String, Object> recordSchemaMap = recordSchema.stream()
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));

        List<String> fieldNames = records.stream().map(Tuple::getKey).collect(Collectors.toList());

        when(schema.getFieldNames()).thenReturn(fieldNames);
        when(schema.getDataType(anyString())).thenAnswer(i -> Optional.of(recordSchemaMap.get(i.getArgument(0))));

        Insert actual = (Insert)testSubject.generateInsert(table, schema, recordContentMap);
        actual.setForceNoValues(true);

        // Codecs are normally registered in the onScheduled method
        testSubject.registerAdditionalCodecs();

        assertEquals(expected, actual.getQueryString());
    }
}
