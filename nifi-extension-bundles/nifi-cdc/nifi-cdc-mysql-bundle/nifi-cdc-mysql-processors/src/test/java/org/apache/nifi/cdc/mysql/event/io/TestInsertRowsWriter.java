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
package org.apache.nifi.cdc.mysql.event.io;


import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.nifi.cdc.event.ColumnDefinition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Types;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class TestInsertRowsWriter {

    @Test
    public void testGetWritableObject() throws IOException {
        InsertRowsWriter insertRowsWriter = new InsertRowsWriter();
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", null, null); //null
            verify(g).writeNullField("fieldName");
            verifyNoMoreInteractions(g);
        }
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", new ColumnDefinition(true, Types.INTEGER), null); //null
            verify(g).writeNullField("fieldName");
            verifyNoMoreInteractions(g);
        }
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", new ColumnDefinition(false, Types.BIGINT), (int) -77); //null
            verify(g).writeFieldName("fieldName");
            verify(g).writeRawValue("18446744073709551539");
            verifyNoMoreInteractions(g);
        }
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", new ColumnDefinition(false, Types.INTEGER), (int) -77); //null
            verify(g).writeFieldName("fieldName");
            verify(g).writeRawValue("4294967219");
            verifyNoMoreInteractions(g);
        }
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", new ColumnDefinition(false, Types.SMALLINT), (int) -77); //null
            verify(g).writeFieldName("fieldName");
            verify(g).writeRawValue("65459");
            verifyNoMoreInteractions(g);
        }
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", new ColumnDefinition(false, Types.TINYINT), (int) -77); //null
            verify(g).writeFieldName("fieldName");
            verify(g).writeRawValue("179");
            verifyNoMoreInteractions(g);
        }
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", new ColumnDefinition(true, Types.TINYINT), (int) -77); //null
            verify(g).writeObjectField("fieldName", -77);
            verifyNoMoreInteractions(g);
        }
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", new ColumnDefinition(false, Types.TINYINT), (byte) 1); //null
            verify(g).writeFieldName("fieldName");
            verify(g).writeRawValue( "1");
            verifyNoMoreInteractions(g);
        }
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", new ColumnDefinition(true, Types.TINYINT), (byte) 1); //null
            verify(g).writeObjectField("fieldName", (byte) 1);
            verifyNoMoreInteractions(g);
        }
        try (JsonGenerator g = mock(JsonGenerator.class)) {
            insertRowsWriter.writeObjectAsValueField(g, "fieldName", new ColumnDefinition(null, Types.VARCHAR), "Hello".getBytes()); //null
            verify(g).writeObjectField("fieldName", "Hello");
            verifyNoMoreInteractions(g);
        }
    }

}