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
package org.apache.nifi.jasn1;

import org.apache.nifi.jasn1.example.BasicTypes;
import org.apache.nifi.jasn1.example.Composite;
import org.apache.nifi.jasn1.example.Recursive;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Depends on generated test classes
 */
public class TestRecordSchemaProvider {

    @Test
    public void testBasicTypes() {
        final RecordSchemaProvider provider = new RecordSchemaProvider();
        final RecordSchema schema = provider.get(BasicTypes.class);

        assertEquals("BasicTypes", schema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", schema.getSchemaNamespace().orElse(null));

        final Optional<RecordField> bField = schema.getField("b");
        assertTrue(bField.isPresent());
        assertEquals(RecordFieldType.BOOLEAN.getDataType(), bField.get().getDataType());

        final Optional<RecordField> iField = schema.getField("i");
        assertTrue(iField.isPresent());
        assertEquals(RecordFieldType.BIGINT.getDataType(), iField.get().getDataType());

        final Optional<RecordField> octStrField = schema.getField("octStr");
        assertTrue(octStrField.isPresent());
        assertEquals(RecordFieldType.STRING.getDataType(), octStrField.get().getDataType());

        final Optional<RecordField> utf8StrField = schema.getField("utf8Str");
        assertTrue(utf8StrField.isPresent());
        assertEquals(RecordFieldType.STRING.getDataType(), utf8StrField.get().getDataType());
    }

    @Test
    public void testComposite() {
        final RecordSchemaProvider provider = new RecordSchemaProvider();
        final RecordSchema schema = provider.get(Composite.class);

        // Child should be a record.
        final Optional<RecordField> childField = schema.getField("child");
        assertTrue(childField.isPresent());
        final DataType childDataType = childField.get().getDataType();
        assertEquals(RecordFieldType.RECORD.getDataType(), childDataType);
        RecordSchema childSchema = ((RecordDataType) childDataType).getChildSchema();
        assertEquals("BasicTypes", childSchema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", childSchema.getSchemaNamespace().orElse(null));

        // Children should be an array of records.
        final Optional<RecordField> childrenField = schema.getField("children");
        assertTrue(childrenField.isPresent());
        final DataType childrenDataType = childrenField.get().getDataType();
        assertEquals(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getDataType()), childrenDataType);
        final DataType childrenElementDataType = ((ArrayDataType) childrenDataType).getElementType();
        assertEquals(RecordFieldType.RECORD.getDataType(), childrenElementDataType);
        childSchema = ((RecordDataType) childrenElementDataType).getChildSchema();
        assertEquals("BasicTypes", childSchema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", childSchema.getSchemaNamespace().orElse(null));

        // Unordered should be an array of records.
        final Optional<RecordField> unorderedField = schema.getField("unordered");
        assertTrue(unorderedField.isPresent());
        final DataType unorderedDataType = unorderedField.get().getDataType();
        assertEquals(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getDataType()), unorderedDataType);
        final DataType unorderedElementDataType = ((ArrayDataType) unorderedDataType).getElementType();
        assertEquals(RecordFieldType.RECORD.getDataType(), unorderedElementDataType);
        childSchema = ((RecordDataType) unorderedElementDataType).getChildSchema();
        assertEquals("BasicTypes", childSchema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", childSchema.getSchemaNamespace().orElse(null));

    }


    @Test
    public void testRecursive() {
        final RecordSchemaProvider provider = new RecordSchemaProvider();
        final RecordSchema schema = provider.get(Recursive.class);

        // Children should be an array of records.
        final Optional<RecordField> childrenField = schema.getField("children");
        assertTrue(childrenField.isPresent());
        final DataType childrenDataType = childrenField.get().getDataType();
        assertEquals(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getDataType()), childrenDataType);
        final DataType childrenElementDataType = ((ArrayDataType) childrenDataType).getElementType();
        assertEquals(RecordFieldType.RECORD.getDataType(), childrenElementDataType);
        final RecordSchema childSchema = ((RecordDataType) childrenElementDataType).getChildSchema();
        assertEquals("Recursive", childSchema.getSchemaName().orElse(null));
        assertEquals("org.apache.nifi.jasn1.example", childSchema.getSchemaNamespace().orElse(null));

    }
}
