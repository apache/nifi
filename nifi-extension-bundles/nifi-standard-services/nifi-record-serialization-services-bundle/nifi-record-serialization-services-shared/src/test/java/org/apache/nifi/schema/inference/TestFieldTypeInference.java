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

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFieldTypeInference {
    private FieldTypeInference testSubject;

    @BeforeEach
    public void setUp() throws Exception {
        testSubject = new FieldTypeInference();
    }

    @Test
    public void testIntegerCombinedWithDouble() {
        final FieldTypeInference inference = new FieldTypeInference();
        inference.addPossibleDataType(RecordFieldType.INT.getDataType());
        inference.addPossibleDataType(RecordFieldType.DOUBLE.getDataType());

        assertEquals(RecordFieldType.DOUBLE.getDataType(), inference.toDataType());
    }

    @Test
    public void testIntegerCombinedWithFloat() {
        final FieldTypeInference inference = new FieldTypeInference();
        inference.addPossibleDataType(RecordFieldType.INT.getDataType());
        inference.addPossibleDataType(RecordFieldType.FLOAT.getDataType());

        assertEquals(RecordFieldType.FLOAT.getDataType(), inference.toDataType());
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
        final List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.FLOAT.getDataType()
        );

        final DataType expected = RecordFieldType.FLOAT.getDataType();
        runWithAllPermutations(this::testToDataTypeShouldReturnSingleType, dataTypes, expected);
    }

    @Test
    public void testToDataTypeWith_INT_STRING_shouldReturn_INT_STRING() {
        // GIVEN
        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.STRING.getDataType()
        );


        Set<DataType> expected = new HashSet<>(Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.STRING.getDataType()
        ));

        // WHEN
        // THEN
        runWithAllPermutations(this::testToDataTypeShouldReturnChoice, dataTypes, expected);
    }

    @Test
    public void testToDataTypeWith_INT_FLOAT_STRING_shouldReturn_FLOAT_STRING() {
        final List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.FLOAT.getDataType(),
                RecordFieldType.STRING.getDataType()
        );

        final Set<DataType> expected = new HashSet<>(Arrays.asList(
                RecordFieldType.FLOAT.getDataType(),
                RecordFieldType.STRING.getDataType()
        ));

        runWithAllPermutations(this::testToDataTypeShouldReturnChoice, dataTypes, expected);
    }

    @Test
    public void testToDataTypeWithMultipleRecord() {
        final String fieldName = "fieldName";
        final DataType intType = RecordFieldType.INT.getDataType();
        final DataType floatType = RecordFieldType.FLOAT.getDataType();
        final DataType stringType = RecordFieldType.STRING.getDataType();

        final List<DataType> dataTypes = Arrays.asList(
            RecordFieldType.RECORD.getRecordDataType(createRecordSchema(fieldName, intType)),
            RecordFieldType.RECORD.getRecordDataType(createRecordSchema(fieldName, floatType)),
            RecordFieldType.RECORD.getRecordDataType(createRecordSchema(fieldName, stringType)),
            RecordFieldType.RECORD.getRecordDataType(createRecordSchema(fieldName, floatType))
        );

        final RecordSchema expectedSchema = createRecordSchema(fieldName, RecordFieldType.CHOICE.getChoiceDataType(floatType, stringType));
        final DataType expecteDataType = RecordFieldType.RECORD.getRecordDataType(expectedSchema);

        runWithAllPermutations(this::testToDataTypeShouldReturnSingleType, dataTypes, expecteDataType);
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
        return new SimpleRecordSchema(Collections.singletonList(
            new RecordField(fieldName, fieldType)
        ));
    }

    private <I, E> void runWithAllPermutations(BiFunction<List<I>, E, ?> test, List<I> input, E expected) {
        test.apply(input, expected);
    }

    private Void testToDataTypeShouldReturnChoice(List<DataType> dataTypes, Set<DataType> expected) {
        dataTypes.forEach(testSubject::addPossibleDataType);

        DataType actual = testSubject.toDataType();
        assertEquals(expected, new HashSet<>(((ChoiceDataType) actual).getPossibleSubTypes()));
        return null;
    }

    private Void testToDataTypeShouldReturnSingleType(List<DataType> dataTypes, DataType expected) {
        dataTypes.forEach(testSubject::addPossibleDataType);

        DataType actual = testSubject.toDataType();
        assertEquals(expected, actual);
        return null;
    }
}
