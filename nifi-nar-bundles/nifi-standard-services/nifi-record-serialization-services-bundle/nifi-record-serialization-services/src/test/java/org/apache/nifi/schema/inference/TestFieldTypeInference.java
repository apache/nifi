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
package org.apache.nifi.schema.inference;

import com.google.common.collect.Sets;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.collect.Collections2.permutations;
import static org.junit.Assert.assertEquals;

public class TestFieldTypeInference {
    private FieldTypeInference testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new FieldTypeInference();
    }

    @Test
    public void testToDataTypeWith_SHORT_INT_LONG_shouldReturn_LONG() {
        // GIVEN
        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.SHORT.getDataType(),
                RecordFieldType.INT.getDataType(),
                RecordFieldType.LONG.getDataType()
        );

        DataType expected = RecordFieldType.LONG.getDataType();

        // WHEN
        // THEN
        runWithAllPermutations(this::testToDataTypeShouldReturnSingleType, dataTypes, expected);
    }

    @Test
    public void testToDataTypeWith_INT_FLOAT_ShouldReturn_INT_FLOAT() {
        // GIVEN
        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.FLOAT.getDataType()
        );

        Set<DataType> expected = Sets.newHashSet(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.FLOAT.getDataType()
        );

        // WHEN
        // THEN
        runWithAllPermutations(this::testToDataTypeShouldReturnChoice, dataTypes, expected);
    }

    @Test
    public void testToDataTypeWith_INT_STRING_shouldReturn_INT_STRING() {
        // GIVEN
        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.STRING.getDataType()
        );


        Set<DataType> expected = Sets.newHashSet(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.STRING.getDataType()
        );

        // WHEN
        // THEN
        runWithAllPermutations(this::testToDataTypeShouldReturnChoice, dataTypes, expected);
    }

    @Test
    public void testToDataTypeWith_INT_FLOAT_STRING_shouldReturn_INT_FLOAT_STRING() {
        // GIVEN
        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.FLOAT.getDataType(),
                RecordFieldType.STRING.getDataType()
        );

        Set<DataType> expected = Sets.newHashSet(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.FLOAT.getDataType(),
                RecordFieldType.STRING.getDataType()
        );

        // WHEN
        // THEN
        runWithAllPermutations(this::testToDataTypeShouldReturnChoice, dataTypes, expected);
    }

    @Test
    public void testToDataTypeWithMultipleRecord() {
        // GIVEN
        String fieldName = "fieldName";
        DataType fieldType1 = RecordFieldType.INT.getDataType();
        DataType fieldType2 = RecordFieldType.FLOAT.getDataType();
        DataType fieldType3 = RecordFieldType.STRING.getDataType();

        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.RECORD.getRecordDataType(createRecordSchema(fieldName, fieldType1)),
                RecordFieldType.RECORD.getRecordDataType(createRecordSchema(fieldName, fieldType2)),
                RecordFieldType.RECORD.getRecordDataType(createRecordSchema(fieldName, fieldType3)),
                RecordFieldType.RECORD.getRecordDataType(createRecordSchema(fieldName, fieldType2))
        );

        DataType expected = RecordFieldType.RECORD.getRecordDataType(createRecordSchema(
                fieldName,
                RecordFieldType.CHOICE.getChoiceDataType(
                        fieldType1,
                        fieldType2,
                        fieldType3
                )
        ));

        // WHEN
        // THEN
        runWithAllPermutations(this::testToDataTypeShouldReturnSingleType, dataTypes, expected);
    }

    @Test
    public void testToDataTypeWithMultipleComplexRecord() {
        // GIVEN
        String fieldName1 = "fieldName1";
        String fieldName2 = "fieldName2";
        String fieldName3 = "fieldName3";

        List<DataType> dataTypes = Arrays.asList(
            RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Arrays.asList(
                new RecordField(fieldName1, RecordFieldType.INT.getDataType()),
                new RecordField(fieldName2, RecordFieldType.STRING.getDataType())
            ))),
            RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Arrays.asList(
                new RecordField(fieldName1, RecordFieldType.INT.getDataType()),
                new RecordField(fieldName3, RecordFieldType.BOOLEAN.getDataType())
            )))
        );

        DataType expected = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Arrays.asList(
            new RecordField(fieldName1, RecordFieldType.INT.getDataType()),
            new RecordField(fieldName2, RecordFieldType.STRING.getDataType()),
            new RecordField(fieldName3, RecordFieldType.BOOLEAN.getDataType())
        )));

        // WHEN
        // THEN
        runWithAllPermutations(this::testToDataTypeShouldReturnSingleType, dataTypes, expected);
    }

    @Test
    public void testToDataTypeWhenDecimal() {
        // GIVEN
        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.DECIMAL.getDecimalDataType(10, 1),
                RecordFieldType.DECIMAL.getDecimalDataType(10, 3),
                RecordFieldType.DECIMAL.getDecimalDataType(7, 3),
                RecordFieldType.DECIMAL.getDecimalDataType(7, 5),
                RecordFieldType.DECIMAL.getDecimalDataType(7, 7),
                RecordFieldType.FLOAT.getDataType(),
                RecordFieldType.DOUBLE.getDataType()
        );

        DataType expected = RecordFieldType.DECIMAL.getDecimalDataType(10, 7);

        // WHEN
        // THEN
        runWithAllPermutations(this::testToDataTypeShouldReturnSingleType, dataTypes, expected);
    }

    private SimpleRecordSchema createRecordSchema(String fieldName, DataType fieldType) {
        return new SimpleRecordSchema(Arrays.asList(
                new RecordField(fieldName, fieldType)
        ));
    }

    private <I, E> void runWithAllPermutations(BiFunction<List<I>, E, ?> test, List<I> input, E expected) {
        permutations(input).forEach(inputPermutation -> test.apply(inputPermutation, expected));
    }

    private Void testToDataTypeShouldReturnChoice(List<DataType> dataTypes, Set<DataType> expected) {
        // GIVEN
        dataTypes.forEach(testSubject::addPossibleDataType);

        // WHEN
        DataType actual = testSubject.toDataType();

        // THEN
        assertEquals(expected, new HashSet<>(((ChoiceDataType) actual).getPossibleSubTypes()));

        return null;
    }

    private Void testToDataTypeShouldReturnSingleType(List<DataType> dataTypes, DataType expected) {
        // GIVEN
        dataTypes.forEach(testSubject::addPossibleDataType);

        // WHEN
        DataType actual = testSubject.toDataType();

        // THEN
        assertEquals(expected, actual);

        return null;
    }
}
