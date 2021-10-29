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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSimpleRecordSchema {

    @Test
    public void testPreventsTwoFieldsWithSameAlias() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("goodbye", RecordFieldType.STRING.getDataType(), null, set("baz", "bar")));

        try {
            new SimpleRecordSchema(fields);
            Assert.fail("Was able to create two fields with same alias");
        } catch (final IllegalArgumentException expected) {
        }
    }

    @Test
    public void testPreventsTwoFieldsWithSameName() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType()));

        try {
            new SimpleRecordSchema(fields);
            Assert.fail("Was able to create two fields with same name");
        } catch (final IllegalArgumentException expected) {
        }
    }

    @Test
    public void testPreventsTwoFieldsWithConflictingNamesAliases() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("bar", RecordFieldType.STRING.getDataType()));

        try {
            new SimpleRecordSchema(fields);
            Assert.fail("Was able to create two fields with conflicting names/aliases");
        } catch (final IllegalArgumentException expected) {
        }
    }

    @Test
    public void testHashCodeAndEqualsWithSelfReferencingSchema() {
        final SimpleRecordSchema schema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);

        final List<RecordField> personFields = new ArrayList<>();
        personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        personFields.add(new RecordField("sibling", RecordFieldType.RECORD.getRecordDataType(schema)));

        schema.setFields(personFields);

        schema.hashCode();
        assertTrue(schema.equals(schema));

        final SimpleRecordSchema secondSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
        secondSchema.setFields(personFields);
        assertTrue(schema.equals(secondSchema));
        assertTrue(secondSchema.equals(schema));
    }

    @Test
    public void testEqualsSimpleSchema() {
        // GIVEN
        final String nameOfField1 = "field1";
        final String nameOfField2 = "field2";
        final DataType typeOfField1 = RecordFieldType.INT.getDataType();
        final DataType typeOfField2 = RecordFieldType.STRING.getDataType();
        final String schemaName = "schemaName";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName, namespace);
        final SimpleRecordSchema schema2 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName, namespace);

        // WHEN, THEN
        assertTrue(schema1.equals(schema2));
    }

    @Test
    public void testEqualsSimpleSchemaEvenIfSchemaNameAndNameSpaceAreDifferent() {
        // GIVEN
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

        // WHEN, THEN
        assertTrue(schema1.equals(schema2));
    }

    @Test
    public void testNotEqualsSimpleSchemaDifferentTypes() {
        // GIVEN
        final String nameOfField1 = "field1";
        final String nameOfField2 = "field2";
        final DataType typeOfField1 = RecordFieldType.INT.getDataType();
        final DataType typeOfField2 = RecordFieldType.STRING.getDataType();
        final String schemaName = "schemaName";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField1, schemaName, namespace);
        final SimpleRecordSchema schema2 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName, namespace);

        // WHEN, THEN
        assertFalse(schema1.equals(schema2));
    }

    @Test
    public void testNotEqualsSimpleSchemaDifferentFieldNames() {
        // GIVEN
        final String nameOfField1 = "field1";
        final String nameOfField2 = "field2";
        final String nameOfField3 = "field3";
        final DataType typeOfField1 = RecordFieldType.INT.getDataType();
        final DataType typeOfField2 = RecordFieldType.STRING.getDataType();
        final String schemaName = "schemaName";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createSchemaWithTwoFields(nameOfField1, nameOfField2, typeOfField1, typeOfField2, schemaName, namespace);
        final SimpleRecordSchema schema2 = createSchemaWithTwoFields(nameOfField1, nameOfField3, typeOfField1, typeOfField2, schemaName, namespace);

        // WHEN, THEN
        assertFalse(schema1.equals(schema2));
    }

    @Test
    public void testEqualsRecursiveSchema() {
        final String field1 = "field1";
        final String field2 = "field2";
        final String schemaName = "schemaName";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createRecursiveSchema(field1, field2, schemaName, namespace);
        final SimpleRecordSchema schema2 = createRecursiveSchema(field1, field2, schemaName, namespace);

        assertTrue(schema1.equals(schema2));
        assertTrue(schema2.equals(schema1));
    }

    @Test(expected = StackOverflowError.class)
    public void testNotEqualsRecursiveSchemaIfSchemaNameIsDifferent() {
        final String field1 = "field1";
        final String field2 = "field2";
        final String schemaName1 = "schemaName1";
        final String schemaName2 = "schemaName2";
        final String namespace = "namespace";

        final SimpleRecordSchema schema1 = createRecursiveSchema(field1, field2, schemaName1, namespace);
        final SimpleRecordSchema schema2 = createRecursiveSchema(field1, field2, schemaName2, namespace);

        assertFalse(schema1.equals(schema2)); // Causes StackOverflowError
    }

    @Test(expected = StackOverflowError.class)
    public void testNotEqualsRecursiveSchemaIfNamespaceIsDifferent() {
        final String field1 = "field1";
        final String field2 = "field2";
        final String schemaName = "schemaName1";
        final String namespace1 = "namespace1";
        final String namespace2 = "namespace2";

        final SimpleRecordSchema schema1 = createRecursiveSchema(field1, field2, schemaName, namespace1);
        final SimpleRecordSchema schema2 = createRecursiveSchema(field1, field2, schemaName, namespace2);

        assertFalse(schema1.equals(schema2));
        assertFalse(schema2.equals(schema1));
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
        for (final String value : values) {
            set.add(value);
        }
        return set;
    }

}
