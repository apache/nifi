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

import com.beanit.asn1bean.ber.types.BerBoolean;
import com.beanit.asn1bean.ber.types.BerInteger;
import com.beanit.asn1bean.ber.types.BerOctetString;
import com.beanit.asn1bean.ber.types.string.BerIA5String;
import com.beanit.asn1bean.ber.types.string.BerUTF8String;
import org.apache.nifi.jasn1.complex.InheritingIntegerAndStringWrapper;
import org.apache.nifi.jasn1.complex.SequenceOfIntegerWrapper;
import org.apache.nifi.jasn1.example.BasicTypeSet;
import org.apache.nifi.jasn1.example.BasicTypes;
import org.apache.nifi.jasn1.example.Composite;
import org.apache.nifi.jasn1.example.Recursive;
import org.apache.nifi.jasn1.util.JASN1ReadRecordTester;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Depends on generated test classes
 */
public class TestJASN1RecordReaderWithComplexTypes implements JASN1ReadRecordTester {
    @Test
    public void testSequenceOfInteger() throws Exception {
        String dataFile = "target/sequence_of_integer_wrapper.dat";

        SequenceOfIntegerWrapper.Value value = new SequenceOfIntegerWrapper.Value();
        value.getBerInteger().add(new BerInteger(1234));
        value.getBerInteger().add(new BerInteger(567));

        SequenceOfIntegerWrapper berValue = new SequenceOfIntegerWrapper();
        berValue.setValue(value);

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", new BigInteger[]{BigInteger.valueOf(1234), BigInteger.valueOf(567)});
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BIGINT.getDataType())))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testBasicTypes() throws Exception {
        String dataFile = "target/basicTypes.dat";

        BasicTypes basicTypes = new BasicTypes();
        basicTypes.setB(new BerBoolean(true));
        basicTypes.setI(new BerInteger(789));
        basicTypes.setOctStr(new BerOctetString(new byte[]{1, 2, 3, 4, 5}));
        basicTypes.setUtf8Str(new BerUTF8String("Some UTF-8 String. こんにちは世界。"));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("b", true);
            put("i", BigInteger.valueOf(789));
            put("octStr", "0102030405");
            put("utf8Str", "Some UTF-8 String. こんにちは世界。");
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("b", RecordFieldType.BOOLEAN.getDataType()),
                new RecordField("i", RecordFieldType.BIGINT.getDataType()),
                new RecordField("octStr", RecordFieldType.STRING.getDataType()),
                new RecordField("utf8Str", RecordFieldType.STRING.getDataType())
        ));

        testReadRecord(dataFile, basicTypes, expectedValues, expectedSchema);
    }

    @Test
    public void testComposite() throws Exception {
        String dataFile = "target/composite.dat";

        BasicTypes child = new BasicTypes();
        child.setB(new BerBoolean(true));
        child.setI(new BerInteger(789));
        child.setOctStr(new BerOctetString(new byte[]{1, 2, 3, 4, 5}));

        BasicTypes child1 = new BasicTypes();
        child1.setB(new BerBoolean(true));
        child1.setI(new BerInteger(0));
        child1.setOctStr(new BerOctetString(new byte[]{0, 0, 0}));

        BasicTypes child2 = new BasicTypes();
        child2.setB(new BerBoolean(false));
        child2.setI(new BerInteger(1));
        child2.setOctStr(new BerOctetString(new byte[]{1, 1, 1}));

        BasicTypes child3 = new BasicTypes();
        child3.setB(new BerBoolean(true));
        child3.setI(new BerInteger(2));
        child3.setOctStr(new BerOctetString(new byte[]{2, 2, 2}));

        Composite.Children children = new Composite.Children();
        children.getBasicTypes().add(child1);
        children.getBasicTypes().add(child2);
        children.getBasicTypes().add(child3);

        BasicTypes unordered1 = new BasicTypes();
        unordered1.setB(new BerBoolean(true));
        unordered1.setI(new BerInteger(0));
        unordered1.setOctStr(new BerOctetString(new byte[]{0, 0, 0}));

        BasicTypes unordered2 = new BasicTypes();
        unordered2.setB(new BerBoolean(false));
        unordered2.setI(new BerInteger(1));
        unordered2.setOctStr(new BerOctetString(new byte[]{1, 1, 1}));

        BasicTypeSet unordered = new BasicTypeSet();
        unordered.getBasicTypes().add(unordered1);
        unordered.getBasicTypes().add(unordered2);

        Composite.Numbers numbers = new Composite.Numbers();
        numbers.getBerInteger().add(new BerInteger(0));
        numbers.getBerInteger().add(new BerInteger(1));
        numbers.getBerInteger().add(new BerInteger(2));
        numbers.getBerInteger().add(new BerInteger(3));

        Composite composite = new Composite();
        composite.setChild(child);
        composite.setChildren(children);
        composite.setNumbers(numbers);
        composite.setUnordered(unordered);

        SimpleRecordSchema expectedChildSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("b", RecordFieldType.BOOLEAN.getDataType()),
                new RecordField("i", RecordFieldType.BIGINT.getDataType()),
                new RecordField("octStr", RecordFieldType.STRING.getDataType()),
                new RecordField("utf8Str", RecordFieldType.STRING.getDataType())
        ));

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("child", RecordFieldType.RECORD.getRecordDataType(expectedChildSchema)),
                new RecordField("children", RecordFieldType.ARRAY.getArrayDataType(
                        RecordFieldType.RECORD.getRecordDataType(expectedChildSchema)
                )),
                new RecordField("numbers", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BIGINT.getDataType())),
                new RecordField("unordered", RecordFieldType.ARRAY.getArrayDataType(
                        RecordFieldType.RECORD.getRecordDataType(expectedChildSchema)
                ))
        ));

        Function<Record, RecordSchema> expectedSchemaProvider = actualRecord -> {
            /**
             * Resolving lazy schema in actual by calling {@link RecordDataType#getChildSchema()}
             */
            ((RecordDataType) actualRecord.getSchema().getField("child").get().getDataType()).getChildSchema();
            ((RecordDataType)((ArrayDataType) actualRecord.getSchema().getField("children").get().getDataType()).getElementType()).getChildSchema();
            ((RecordDataType)((ArrayDataType) actualRecord.getSchema().getField("unordered").get().getDataType()).getElementType()).getChildSchema();

            return expectedSchema;
        };

        Function<Record, Map<String, Object>> expectedValuesProvider = __ -> new HashMap<String, Object>() {{
            put(
                    "child",
                    new MapRecord(expectedChildSchema, new HashMap<String, Object>() {{
                        put("b", true);
                        put("i", BigInteger.valueOf(789));
                        put("octStr", "0102030405");
                    }})
            );
            put(
                    "children",
                    new MapRecord[]{
                            new MapRecord(expectedChildSchema, new HashMap<String, Object>() {{
                                put("b", true);
                                put("i", BigInteger.valueOf(0));
                                put("octStr", "000000");
                            }}),
                            new MapRecord(expectedChildSchema, new HashMap<String, Object>() {{
                                put("b", false);
                                put("i", BigInteger.valueOf(1));
                                put("octStr", "010101");
                            }}),
                            new MapRecord(expectedChildSchema, new HashMap<String, Object>() {{
                                put("b", true);
                                put("i", BigInteger.valueOf(2));
                                put("octStr", "020202");
                            }})
                    }
            );
            put(
                    "unordered",
                    new MapRecord[]{
                            new MapRecord(expectedChildSchema, new HashMap<String, Object>() {{
                                put("b", true);
                                put("i", BigInteger.valueOf(0));
                                put("octStr", "000000");
                            }}),
                            new MapRecord(expectedChildSchema, new HashMap<String, Object>() {{
                                put("b", false);
                                put("i", BigInteger.valueOf(1));
                                put("octStr", "010101");
                            }})
                    }
            );
            put(
                    "numbers",
                    new BigInteger[]{
                            BigInteger.valueOf(0), BigInteger.valueOf(1), BigInteger.valueOf(2), BigInteger.valueOf(3),
                    }
            );
        }};

        testReadRecord(dataFile, composite, expectedValuesProvider, expectedSchemaProvider);
    }

    @Test
    public void testRecursive() throws Exception {
        String dataFile = "target/recursive.dat";

        Recursive recursive = new Recursive();
        Recursive.Children children = new Recursive.Children();
        Recursive child1 = new Recursive();
        Recursive child2 = new Recursive();
        Recursive.Children grandChildren1 = new Recursive.Children();
        Recursive grandChild11 = new Recursive();

        grandChild11.setName(new BerIA5String("grandChildName11".getBytes()));
        grandChild11.setChildren(new Recursive.Children());

        grandChildren1.getRecursive().add(grandChild11);

        child1.setName(new BerIA5String("childName1".getBytes()));
        child1.setChildren(grandChildren1);

        child2.setName(new BerIA5String("childName2".getBytes()));
        child2.setChildren(new Recursive.Children());

        children.getRecursive().add(child1);
        children.getRecursive().add(child2);

        recursive.setName(new BerIA5String("name".getBytes()));
        recursive.setChildren(children);

        /**
         * Cannot resolve children schema, neither in expected nor in actual.
         * A resolved recursive schema MUST NOT go through an equals check, unless IDENTICAL to the compared one.
         * The resolution of the recursive schema results in a cyclic reference graph which in turn leads to
         *  StackOverflowError when trying to compare to a similar resolved recursive schema.
         */
        SimpleRecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("children", RecordFieldType.ARRAY.getArrayDataType(
                        RecordFieldType.RECORD.getRecordDataType(() -> null)
                ))
        ));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("name", "name");
            put("children", new MapRecord[]{
                    new MapRecord(expectedSchema, new HashMap<String, Object>() {{
                        put("name", "childName1");
                        put("children", new MapRecord[]{
                                new MapRecord(expectedSchema, new HashMap<String, Object>() {{
                                    put("name", "grandChildName11");
                                    put("children", new MapRecord[0]);
                                }})
                        });
                    }}),
                    new MapRecord(expectedSchema, new HashMap<String, Object>() {{
                        put("name", "childName2");
                        put("children", new MapRecord[0]);
                    }}),
            });
        }};

        testReadRecord(dataFile, recursive, expectedValues, expectedSchema);
    }

    @Test
    public void testInheritance() throws Exception {
        String dataFile = "target/inheriting_integer_and_string_wrapper.dat";

        InheritingIntegerAndStringWrapper berValue = new InheritingIntegerAndStringWrapper();
        berValue.setI(new BerInteger(53286));
        berValue.setStr(new BerUTF8String("Some UTF-8 String. こんにちは世界。"));

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("i", RecordFieldType.BIGINT.getDataType()),
            new RecordField("str", RecordFieldType.STRING.getDataType())
        ));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("i", BigInteger.valueOf(53286L));
            put("str", "Some UTF-8 String. こんにちは世界。");
        }};

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }
}
