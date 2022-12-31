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

package org.apache.nifi.serialization;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestSimpleRecordSchema {

    @Test
    void testPreventsTwoFieldsWithSameAlias() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("goodbye", RecordFieldType.STRING.getDataType(), null, set("baz", "bar")));

        assertThrows(IllegalArgumentException.class, () -> new SimpleRecordSchema(fields));
    }

    @Test
    void testPreventsTwoFieldsWithSameName() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType()));

        assertThrows(IllegalArgumentException.class, () -> new SimpleRecordSchema(fields));
    }

    @Test
    void testPreventsTwoFieldsWithConflictingNamesAliases() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("bar", RecordFieldType.STRING.getDataType()));

        assertThrows(IllegalArgumentException.class, () -> new SimpleRecordSchema(fields));
    }

    @Test
    void testHashCodeAndEqualsWithSelfReferencingSchema() {
        final SimpleRecordSchema schema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);

        final List<RecordField> personFields = new ArrayList<>();
        personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        personFields.add(new RecordField("sibling", RecordFieldType.RECORD.getRecordDataType(schema)));

        schema.setFields(personFields);
        assertEquals(schema, schema);

        final SimpleRecordSchema secondSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
        secondSchema.setFields(personFields);
        assertEquals(schema.hashCode(), secondSchema.hashCode());
        assertEquals(schema, secondSchema);
        assertEquals(secondSchema, schema);
    }

    @Test
    void testEqualsSimpleSchema() {
        final String nameOfField1 = "field1";
        final String nameOfField2 = "field2";
        final DataType typeOfField1 = RecordFieldType.INT.getDataType();
        final DataType typeOfField2 = RecordFieldType.STRING.getDataType();
        final String schemaName = "schemaName";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName, namespace);
        final SimpleRecordSchema schema2 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName, namespace);

        assertEquals(schema1, schema2);
    }

    @Test
    void testEqualsSimpleSchemaEvenIfSchemaNameAndNameSpaceAreDifferent() {
        final String nameOfField1 = "field1";
        final String nameOfField2 = "field2";
        final DataType typeOfField1 = RecordFieldType.INT.getDataType();
        final DataType typeOfField2 = RecordFieldType.STRING.getDataType();
        final String schemaName1 = "schemaName1";
        final String schemaName2 = "schemaName2";
        final String namespace1 = "namespace1";
        final String namespace2 = "namespace2";

        final SimpleRecordSchema schema1 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName1, namespace1);
        final SimpleRecordSchema schema2 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName2, namespace2);

        assertEquals(schema1, schema2);
    }

    @Test
    void testNotEqualsSimpleSchemaDifferentTypes() {
        final String nameOfField1 = "field1";
        final String nameOfField2 = "field2";
        final DataType typeOfField1 = RecordFieldType.INT.getDataType();
        final DataType typeOfField2 = RecordFieldType.STRING.getDataType();
        final String schemaName = "schemaName";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField1, schemaName, namespace);
        final SimpleRecordSchema schema2 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName, namespace);

        assertNotEquals(schema1, schema2);
    }

    @Test
    void testNotEqualsSimpleSchemaDifferentFieldNames() {
        final String nameOfField1 = "fieldA";
        final String nameOfField2 = "fieldB";
        final String nameOfField3 = "fieldC";
        final DataType typeOfField1 = RecordFieldType.INT.getDataType();
        final DataType typeOfField2 = RecordFieldType.STRING.getDataType();
        final String schemaName = "schemaName";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName, namespace);
        final SimpleRecordSchema schema2 = createSchemaWithTwoFields(nameOfField1, nameOfField3, typeOfField1, typeOfField2, schemaName, namespace);

        assertNotEquals(schema1, schema2);
    }

    @Test
    void testEqualsRecursiveSchema() {
        final String field1 = "field1";
        final String field2 = "field2";
        final String schemaName = "schemaName";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createRecursiveSchema(field1, field2, schemaName, namespace);
        final SimpleRecordSchema schema2 = createRecursiveSchema(field1, field2, schemaName, namespace);

        assertEquals(schema1, schema2);
        assertEquals(schema2, schema1);
    }

    @Test
    void testNotEqualsRecursiveSchemaIfSchemaNameIsDifferent() {
        final String field1 = "field1";
        final String field2 = "field2";
        final String schemaName1 = "schemaName1";
        final String schemaName2 = "schemaName2";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createRecursiveSchema(field1, field2, schemaName1, namespace);
        final SimpleRecordSchema schema2 = createRecursiveSchema(field1, field2, schemaName2, namespace);

        assertThrows(StackOverflowError.class, () -> schema1.equals(schema2));
        assertThrows(StackOverflowError.class, () -> schema2.equals(schema1));
    }

    @Test
    void testNotEqualsRecursiveSchemaIfNamespaceIsDifferent() {
        final String field1 = "fieldA";
        final String field2 = "fieldB";
        final String schemaName = "schemaName1";
        final String namespace1 = "namespace1";
        final String namespace2 = "namespace2";

        final SimpleRecordSchema schema1 = createRecursiveSchema(field1, field2, schemaName, namespace1);
        final SimpleRecordSchema schema2 = createRecursiveSchema(field1, field2, schemaName, namespace2);

        assertThrows(StackOverflowError.class, () -> schema1.equals(schema2));
        assertThrows(StackOverflowError.class, () -> schema2.equals(schema1));
    }

    private SimpleRecordSchema createSchemaWithTwoFields(String nameOfField1, String nameOfField2,
                                                         DataType typeOfField1, DataType typeOfField2,
                                                         String schemaName, String schemaNamespace) {
        final SimpleRecordSchema schema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField(nameOfField1, typeOfField1));
        fields.add(new RecordField(nameOfField2, typeOfField2));
        schema.setFields(fields);
        schema.setSchemaName(schemaName);
        schema.setSchemaNamespace(schemaNamespace);
        return schema;
    }

    private SimpleRecordSchema createRecursiveSchema(String nameOfSimpleField, String nameOfRecursiveField,
                                                     String schemaName, String schemaNamespace) {
        final SimpleRecordSchema schema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField(nameOfSimpleField, RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField(nameOfRecursiveField, RecordFieldType.RECORD.getRecordDataType(schema)));
        schema.setFields(fields);
        schema.setSchemaName(schemaName);
        schema.setSchemaNamespace(schemaNamespace);
        return schema;
    }

    private Set<String> set(final String... values) {
        final Set<String> set = new HashSet<>();
        Collections.addAll(set, values);
        return set;
    }

}
