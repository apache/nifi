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

package org.apache.nifi.record.path;

import org.apache.nifi.record.path.exception.RecordPathException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.uuid5.Uuid5Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"SameParameterValue"})
public class TestRecordPath {

    private static final String USER_TIMEZONE_PROPERTY_NAME = "user.timezone";
    private static final String INITIAL_USER_TIMEZONE = System.getProperty(USER_TIMEZONE_PROPERTY_NAME);
    private static final TimeZone INITIAL_DEFAULT_TIMEZONE = TimeZone.getDefault();

    private static final ZoneId TEST_ZONE_ID = ZoneId.of("America/Phoenix");
    private static final TimeZone TEST_TIME_ZONE = TimeZone.getTimeZone(TEST_ZONE_ID);

    @BeforeAll
    public static void setTestTimezone() {
        System.setProperty(USER_TIMEZONE_PROPERTY_NAME, TEST_ZONE_ID.getId());
        TimeZone.setDefault(TEST_TIME_ZONE);
    }

    @AfterAll
    public static void setSystemTimezone() {
        if (INITIAL_USER_TIMEZONE != null) {
            System.setProperty(USER_TIMEZONE_PROPERTY_NAME, INITIAL_USER_TIMEZONE);
        }
        if (INITIAL_DEFAULT_TIMEZONE != null) {
            TimeZone.setDefault(INITIAL_DEFAULT_TIMEZONE);
        }
    }

    private final Record record = createExampleRecord();
    private final Record mainAccountRecord = record.getAsRecord("mainAccount", getAccountSchema());

    @Test
    void supportsReferenceToRootRecord() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/"));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertEquals(Optional.empty(), fieldValue.getParent());
        assertEquals(record, fieldValue.getValue());
    }

    @Test
    void supportsReferenceToDirectChildField() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/name"));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertFieldValue(record, "name", "John Doe", fieldValue);
    }

    @Test
    void supportsReferenceToNestedChildField() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/balance"));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertFieldValue(mainAccountRecord, "balance", 123.45, fieldValue);
    }

    @Test
    void supportsReferenceToSelf() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/name/."));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertFieldValue(record, "name", "John Doe", fieldValue);
    }

    @Test
    void supportsReferenceToParentField() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/balance/.."));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertFieldValue(record, "mainAccount", record.getValue("mainAccount"), fieldValue);
    }

    @Test
    void supportsReferenceWithRelativePath() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/././balance"));

        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertFieldValue(mainAccountRecord, "balance", 123.45, fieldValue);
    }

    @Test
    void supportsReferenceStartingWithRelativePath() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("./../mainAccount"));

        final FieldValue relativeParentPathContext = new StandardFieldValue(
                record, recordFieldOf("root", recordTypeOf(record.getSchema())), null
        );
        final FieldValue relativePathContext = new StandardFieldValue(
                "John Doe", recordFieldOf("name", RecordFieldType.STRING), relativeParentPathContext
        );
        // relative paths work on the context instead of base record; to showcase this we pass an empty record
        final Record emptyRecord = new MapRecord(recordSchemaOf(), Collections.emptyMap());
        final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, emptyRecord, relativePathContext);
        assertFieldValue(record, "mainAccount", record.getValue("mainAccount"), fieldValue);
    }

    @Test
    public void supportsReferenceToEscapedFieldName() {
        final RecordSchema schema = recordSchemaOf(
                recordFieldOf("full,name", RecordFieldType.STRING)
        );
        final Record record = new MapRecord(schema, Map.of("full,name", "John Doe"));

        final FieldValue fieldValue = evaluateSingleFieldValue("/'full,name'", record);
        assertFieldValue(record, "full,name", "John Doe", fieldValue);
    }

    @Test
    void supportsWildcardReference() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount/*"));

        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        final Record targetParent = mainAccountRecord;
        assertAll(
                () -> assertEquals(3, fieldValues.size()),
                () -> assertFieldValue(targetParent, "id", 1, fieldValues.getFirst()),
                () -> assertFieldValue(targetParent, "balance", 123.45, fieldValues.get(1)),
                () -> assertFieldValue(targetParent, "address", getAddressRecord(targetParent), fieldValues.get(2))
        );
    }

    @Test
    void supportsReferenceToDescendant() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("//id"));

        final Record record = reduceRecord(this.record, "id", "mainAccount", "accounts");
        final Record mainAccountRecord = TestRecordPath.this.mainAccountRecord;
        final Record[] accountRecords = (Record[]) record.getAsArray("accounts");
        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        assertAll(
                () -> assertEquals(4, fieldValues.size()),
                () -> assertFieldValue(record, "id", 48, fieldValues.getFirst()),
                () -> assertFieldValue(mainAccountRecord, "id", 1, fieldValues.get(1)),
                () -> assertFieldValue(accountRecords[0], "id", 6, fieldValues.get(2)),
                () -> assertFieldValue(accountRecords[1], "id", 9, fieldValues.get(3))
        );
    }

    @Test
    void supportsReferenceToChildFieldOfDescendant() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("//address/city"));

        final Record record = reduceRecord(this.record, "id", "mainAccount", "accounts");
        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        final Record mainAccountRecord = TestRecordPath.this.mainAccountRecord;
        final Record[] accountRecords = (Record[]) record.getAsArray("accounts");
        assertAll(
                () -> assertEquals(3, fieldValues.size()),
                () -> assertFieldValue(getAddressRecord(mainAccountRecord), "city", "Boston", fieldValues.getFirst()),
                () -> assertFieldValue(getAddressRecord(accountRecords[0]), "city", "Las Vegas", fieldValues.get(1)),
                () -> assertFieldValue(getAddressRecord(accountRecords[1]), "city", "Austin", fieldValues.get(2))
        );
    }

    @Test
    void supportsReferenceToDescendantWithWildcard() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/mainAccount//*"));

        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        final Record mainAccountRecord = TestRecordPath.this.mainAccountRecord;
        final Record addressRecord = getAddressRecord(mainAccountRecord);
        assertAll(
                () -> assertEquals(5, fieldValues.size()),
                () -> assertFieldValue(mainAccountRecord, "id", 1, fieldValues.getFirst()),
                () -> assertFieldValue(mainAccountRecord, "balance", 123.45, fieldValues.get(1)),
                () -> assertFieldValue(mainAccountRecord, "address", addressRecord, fieldValues.get(2)),
                () -> assertFieldValue(addressRecord, "city", "Boston", fieldValues.get(3)),
                () -> assertFieldValue(addressRecord, "state", "Massachusetts", fieldValues.get(4))
        );
    }

    @Test
    void supportsReferenceToDescendantFieldsInRecordInsideArray() {
        final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("//city"));

        final RecordSchema schema = recordSchemaOf(
                recordFieldOf("array", arrayTypeOf(RecordFieldType.RECORD))
        );
        final Record addressRecord = createAddressRecord("Paris", "France");
        final Record accountRecord = createAccountRecord();
        final Record record = new MapRecord(schema, new HashMap<>(Map.of(
                "array", new Record[]{accountRecord, addressRecord}
        )));
        final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
        assertAll(
                () -> assertEquals(2, fieldValues.size()),
                () -> assertFieldValue(getAddressRecord(accountRecord), "city", "Boston", fieldValues.getFirst()),
                () -> assertFieldValue(addressRecord, "city", "Paris", fieldValues.get(1))
        );
    }

    @Test
    public void supportsToEscapeQuotesInLiterals() {
        record.setValue("attributes", new HashMap<>(Map.of(
                "Joe's Lama", "cute",
                "Angela \"Mutti\" Merkel", "powerful"
        )));

        assertFieldValue(record, "attributes", "cute", evaluateSingleFieldValue("/attributes['Joe\\'s Lama']", record));
        assertFieldValue(record, "attributes", "powerful", evaluateSingleFieldValue("/attributes[\"Angela \\\"Mutti\\\" Merkel\"]", record));
    }

    @Test
    public void supportsJavaEscapeSequencesInLiterals() {
        record.setValue("attributes", new HashMap<>(Map.of(
                " \t\r\n", "whitespace"
        )));

        assertFieldValue(record, "attributes", "whitespace", evaluateSingleFieldValue("/attributes[' \\t\\r\\n']", record));
    }

    @Nested
    class ArrayReferences {
        private final String[] friendValues = (String[]) record.getAsArray("friends");

        @Test
        public void supportReferenceToSingleArrayIndex() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[0]"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "friends", friendValues[0], fieldValue);
        }

        @Test
        public void supportReferenceToMultipleArrayIndices() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[1, 3]"));

            String[] expectedValues = new String[]{friendValues[1], friendValues[3]};
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", expectedValues, fieldValues);
        }

        @Test
        public void supportReferenceToNegativeArrayIndex() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[-2]"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "friends", friendValues[friendValues.length - 2], fieldValue);
        }

        @Test
        public void supportReferenceToRangeOfArrayIndices() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[1..2]"));

            String[] expectedValues = new String[]{friendValues[1], friendValues[2]};
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", expectedValues, fieldValues);
        }

        @Test
        public void supportReferenceWithWildcardAsArrayIndex() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[*]"));

            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", friendValues, fieldValues);
        }

        @Test
        public void canReferenceSameArrayItemMultipleTimes() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[1, 1..1, -3]"));

            String[] expectedValues = new String[]{friendValues[1], friendValues[1], friendValues[1]};
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", expectedValues, fieldValues);
        }

        @Test
        public void supportReferenceWithCombinationOfArrayAccesses() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[1..2, 0, -1]"));

            String[] expectedValues = new String[]{
                    friendValues[1], friendValues[2], friendValues[0], friendValues[friendValues.length - 1]
            };
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", expectedValues, fieldValues);
        }

        @Test
        public void canReferenceArrayItemsUsingRelativePath() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends/.[3]"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "friends", friendValues[3], fieldValue);
        }

        @Test
        public void predicateCanBeAppliedOnSingleArrayItem() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[1][. = 'Jane']"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "friends", friendValues[1], fieldValue);
        }

        @Test
        public void predicateCanBeAppliedOnMultipleArrayItems() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[0, 1, 2][. != 'Jane']"));

            String[] expectedValues = new String[]{friendValues[0], friendValues[2]};
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "friends", expectedValues, fieldValues);
        }

        @Test
        public void supportsUpdatingArrayItem() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[0]"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            fieldValue.updateValue("Theo");

            final FieldValue updatedFieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "friends", "Theo", updatedFieldValue);
        }

        @Test
        public void yieldsNoResultWhenArrayRangeCountsDown() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[3..2]"));

            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertEquals(List.of(), fieldValues);
        }

        @Test
        public void yieldsNoResultWhenArrayIndexOutOfBounds() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/friends[9001]"));

            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertEquals(List.of(), fieldValues);
        }
    }

    @Nested
    class MapReferences {
        private final Map<String, String> attributes = Map.of(
                "key1", "value1",
                "key2", "value2",
                "key3", "value3"
        );

        @BeforeEach
        public void setUp() {
            record.setValue("attributes", new LinkedHashMap<>(attributes));
        }

        @Test
        public void supportReferenceToSingleMapKey() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['key1']"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "attributes", attributes.get("key1"), fieldValue);

            fieldValue.updateValue("updatedKey");
            final FieldValue updatedFieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "attributes", "updatedKey", updatedFieldValue);
        }

        @Test
        public void supportReferenceToMultipleMapKeys() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['key1', 'key3']"));

            String[] expectedValues = new String[]{attributes.get("key1"), attributes.get("key3")};
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "attributes", expectedValues, fieldValues);
        }

        @Test
        public void supportReferenceWithWildcardAsMapKey() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes[*]"));

            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "attributes", attributes.values().toArray(), fieldValues);
        }

        @Test
        public void canReferenceSameMapItemMultipleTimes() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['key1', 'key3', 'key1']"));

            String[] expectedValues = new String[]{
                    attributes.get("key1"), attributes.get("key3"), attributes.get("key1")
            };
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "attributes", expectedValues, fieldValues);
        }

        @Test
        public void canReferenceMapItemsUsingRelativePath() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes/.['key3']"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "attributes", attributes.get("key3"), fieldValue);
        }

        @Test
        public void predicateCanBeAppliedOnSingleMapItem() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['key2'][. = 'value2']"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "attributes", attributes.get("key2"), fieldValue);
        }

        @Test
        public void predicateCanBeAppliedOnMultipleMapItems() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['key1', 'key2', 'key3'][. != 'value2']"));

            String[] expectedValues = new String[]{
                    attributes.get("key1"), attributes.get("key3")
            };
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
            assertSingleFieldMultipleValueResult(record, "attributes", expectedValues, fieldValues);
        }

        @Test
        public void supportsUpdatingMapItem() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['key1']"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            fieldValue.updateValue("updatedKey");

            final FieldValue updatedFieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "attributes", "updatedKey", updatedFieldValue);
        }

        @Test
        public void yieldsNullWhenMapKeyNotFound() {
            final RecordPath recordPath = assertDoesNotThrow(() -> RecordPath.compile("/attributes['nope']"));

            final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
            assertFieldValue(record, "attributes", null, fieldValue);
        }
    }

    @Nested
    class FieldTypes {
        @Test
        public void supportsReferenceToFieldOfTypeBigInt() {
            supportsRecordFieldType(
                    RecordFieldType.BIGINT,
                    List.of(
                            BigInteger.valueOf(5623351),
                            BigInteger.ZERO,
                            BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
                            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)

                    ),
                    List.of(
                            (short) 1, // SHORT
                            (byte) 2, // BYTE
                            43, // INT
                            47L // LONG
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeBoolean() {
            supportsRecordFieldType(
                    RecordFieldType.BOOLEAN,
                    List.of(
                            false,
                            true
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeByte() {
            supportsRecordFieldType(
                    RecordFieldType.BYTE,
                    List.of(
                            (byte) 0,
                            (byte) 1,
                            Byte.MIN_VALUE,
                            Byte.MAX_VALUE
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeChar() {
            supportsRecordFieldType(
                    RecordFieldType.CHAR,
                    List.of(
                            '0',
                            'a',
                            ' ',
                            Character.MIN_VALUE,
                            Character.MAX_VALUE
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeDate() {
            supportsRecordFieldType(
                    RecordFieldType.DATE,
                    List.of(
                            Date.valueOf("2024-08-18")
                    ),
                    List.of(
                            Timestamp.valueOf("2024-08-18 09:45:27") // TIMESTAMP
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeDecimal() {
            supportsRecordFieldType(
                    RecordFieldType.DECIMAL,
                    List.of(
                            BigDecimal.valueOf(1.234567890123456789),
                            BigDecimal.valueOf(0)
                    ),
                    List.of(
                            2.4f, // FLOAT
                            6.8d // DOUBLE
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeDouble() {
            supportsRecordFieldType(
                    RecordFieldType.DOUBLE,
                    List.of(
                            0d,
                            1d,
                            1.1d,
                            -1.1d,
                            Double.MIN_VALUE,
                            Double.MAX_VALUE,
                            Double.NaN
                    ),
                    List.of(
                            2.4f // FLOAT
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeFloat() {
            supportsRecordFieldType(
                    RecordFieldType.FLOAT,
                    List.of(
                            0f,
                            1f,
                            1.1f,
                            -1.1f,
                            Float.MIN_VALUE,
                            Float.MAX_VALUE,
                            Float.NaN
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeInt() {
            supportsRecordFieldType(
                    RecordFieldType.INT,
                    List.of(
                            0,
                            1,
                            -1,
                            Integer.MIN_VALUE,
                            Integer.MAX_VALUE
                    ),
                    List.of(
                            (short) 2, // SHORT
                            (byte) 43 // BYTE
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeLong() {
            supportsRecordFieldType(
                    RecordFieldType.LONG,
                    List.of(
                            1234567890L,
                            0L,
                            ((long) Integer.MAX_VALUE) + 1L,
                            ((long) Integer.MIN_VALUE) - 1L,
                            Long.MIN_VALUE,
                            Long.MAX_VALUE

                    ),
                    List.of(
                            (short) 1, // SHORT
                            (byte) 2, // BYTE
                            43 // INT
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeShort() {
            supportsRecordFieldType(
                    RecordFieldType.SHORT,
                    List.of(
                            (short) 2412,
                            (short) 0,
                            Short.MIN_VALUE,
                            Short.MAX_VALUE

                    ),
                    List.of(
                            (byte) 2 // BYTE
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeEnum() {
            final List<String> enumValues = List.of("alice", "Bob", "EVE");

            supportsRecordFieldType(
                    RecordFieldType.ENUM.getEnumDataType(enumValues),
                    enumValues
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeString() {
            supportsRecordFieldType(
                    RecordFieldType.STRING,
                    List.of(
                            "Test",
                            "X",
                            "",
                            " \n\r\t"
                    ),
                    List.of(
                            true, // BOOLEAN
                            (byte) 5, // BYTE
                            'c', // CHAR
                            (short) 123, // SHORT
                            0, // INT
                            BigInteger.TWO, // BIGINT
                            44L, // LONG
                            12.3f, // FLOAT
                            0.79f, // DOUBLE
                            BigDecimal.valueOf(10.32547698), // DECIMAL
                            Date.valueOf("2024-08-18"), // DATE
                            Time.valueOf("09:45:27"), // TIME
                            Timestamp.valueOf("2024-08-18 09:45:27") // TIMESTAMP
                            // ENUM is represented as a String value and thus does not need to be tested
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeTime() {
            supportsRecordFieldType(
                    RecordFieldType.TIME,
                    List.of(
                            Time.valueOf("09:45:27")
                    ),
                    List.of(
                            Timestamp.valueOf("2024-08-18 09:45:27") // TIMESTAMP
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeTimestamp() {
            supportsRecordFieldType(
                    RecordFieldType.TIMESTAMP,
                    List.of(
                            Timestamp.valueOf("2024-08-18 09:45:27")
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeUuid() {
            supportsRecordFieldType(
                    RecordFieldType.UUID,
                    List.of(
                            UUID.randomUUID(),
                            UUID.fromString("cca14e57-c79e-4fb9-8235-afeb503380df"),
                            UUID.nameUUIDFromBytes(new byte[0])
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeArray() {
            supportsRecordFieldType(
                    arrayTypeOf(RecordFieldType.STRING),
                    List.of(
                            new String[0],
                            new String[]{"CAT"},
                            new String[]{"a", "b", "c", "x", "y", "z"}
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeMap() {
            supportsRecordFieldType(
                    mapTypeOf(RecordFieldType.INT),
                    List.of(
                            Map.of(),
                            Map.of("first", 1),
                            Map.of("a", 1, "b", 2, "c", 3)
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeRecord() {
            supportsRecordFieldType(
                    recordTypeOf(getAccountSchema()),
                    List.of(
                            createAccountRecord()
                    )
            );
        }

        @Test
        public void supportsReferenceToFieldOfTypeChoice() {
            supportsRecordFieldType(
                    choiceTypeOf(
                            RecordFieldType.INT,
                            RecordFieldType.STRING,
                            recordTypeOf(getAccountSchema())
                    ),
                    List.of(
                            25,
                            "Alice",
                            createAccountRecord()
                    )
            );
        }

        private static <T> void supportsRecordFieldType(
                final RecordFieldType expectedType,
                final List<T> expectedUnchangedValues) {
            supportsRecordFieldType(expectedType, expectedUnchangedValues, List.of());
        }

        private static <T> void supportsRecordFieldType(
                final RecordFieldType expectedType,
                final List<T> expectedUnchangedValues,
                final List<Object> expectedAdjustedValues) {
            supportsRecordFieldType(expectedType.getDataType(), expectedUnchangedValues, expectedAdjustedValues);
        }

        private static <T> void supportsRecordFieldType(
                final DataType expectedType,
                final List<T> expectedUnchangedValues) {
            supportsRecordFieldType(expectedType, expectedUnchangedValues, List.of());
        }

        private static <T> void supportsRecordFieldType(
                final DataType expectedType,
                final List<T> expectedUnchangedValues,
                final List<Object> expectedAdjustedValues) {
            final Stream<Map.Entry<Object, Object>> expectedValues = Stream.concat(
                    expectedUnchangedValues.stream().map(value -> Map.entry(value, value)),
                    expectedAdjustedValues.stream().map(
                            value -> Map.entry(value, DataTypeUtils.convertType(value, expectedType, "field"))
                    )
            );

            final Executable nullCheck = () -> {
                final Record record = createSingleFieldRecord(expectedType);

                final FieldValue initialFieldValue = evaluateSingleFieldValue("/field", record);
                assertDoesNotThrow(() -> initialFieldValue.updateValue(null));

                final FieldValue updatedFieldValue = evaluateSingleFieldValue("/field", record);
                assertAll(
                        () -> assertEquals(record, updatedFieldValue.getParentRecord().orElseThrow()),
                        () -> assertEquals(expectedType, updatedFieldValue.getField().getDataType()),
                        () -> assertEquals("field", updatedFieldValue.getField().getFieldName()),
                        () -> assertNull(updatedFieldValue.getValue())
                );
            };

            Stream<Executable> valueChecks = expectedValues.map(originalAndExpectedValue -> () -> {
                final Record record = createSingleFieldRecord(expectedType);

                final FieldValue initialFieldValue = evaluateSingleFieldValue("/field", record);
                assertDoesNotThrow(() -> initialFieldValue.updateValue(originalAndExpectedValue.getKey()));
                final FieldValue fieldValue = evaluateSingleFieldValue("/field", record);

                assertAll(
                        () -> assertEquals(record, fieldValue.getParentRecord().orElseThrow()),
                        () -> assertEquals(expectedType, fieldValue.getField().getDataType()),
                        () -> assertEquals("field", fieldValue.getField().getFieldName()),
                        () -> assertEquals(originalAndExpectedValue.getValue(), fieldValue.getValue())
                );
            });

            assertAll(Stream.concat(Stream.of(nullCheck), valueChecks));
        }

        private static Record createSingleFieldRecord(DataType expectedType) {
            final RecordSchema schema = recordSchemaOf(recordFieldOf("field", expectedType));
            return new MapRecord(schema, new HashMap<>(), true, true);
        }
    }

    @Nested
    class StandaloneFunctions {

        @Test
        public void standaloneFunctionsCanBeUsedAsRecordPath() {
            final FieldValue fieldValue = evaluateSingleFieldValue("substringBefore(/name, ' ')", record);
            assertEquals("John", fieldValue.getValue());
        }

        @Test
        public void standaloneFunctionsCanBeChainedTogether() {
            final FieldValue fieldValue = evaluateSingleFieldValue("substringAfter(substringBefore(/name, 'n'), 'J')", record);
            assertEquals("oh", fieldValue.getValue());
        }

        @Test
        public void supportsUsingStandaloneFunctionAsPartOfPredicate() {
            final FieldValue fieldValue = evaluateSingleFieldValue("/name[substring(., 1, 2) = 'o']", record);
            assertFieldValue(record, "name", "John Doe", fieldValue);
        }

        @Test
        public void throwsRecordPathExceptionWhenUsingStandaloneFunctionAsPredicate() {
            assertThrows(RecordPathException.class, () -> RecordPath.compile("/name[substring(., 1, 2)]"));
        }

        @Test
        public void canReferenceRecordRootInStandaloneFunction() {
            final Record record = reduceRecord(TestRecordPath.this.record, "id", "name", "missing");

            final FieldValue singleArgumentFieldValue = evaluateSingleFieldValue("escapeJson(/)", record);
            assertEquals("{\"id\":48,\"name\":\"John Doe\",\"missing\":null}", singleArgumentFieldValue.getValue());

            final FieldValue multipleArgumentsFieldValue = evaluateSingleFieldValue("mapOf(\"copy\",/)", record);
            assertEquals(Map.of("copy", record.toString()), multipleArgumentsFieldValue.getValue());
        }

        @Nested
        class ArrayOf {

            @Test
            public void testSimpleArrayOfValues() {
                final RecordPath recordPath = RecordPath.compile("arrayOf( 'a', 'b', 'c' )");
                final RecordPathResult result = recordPath.evaluate(record);
                final Object resultValue = result.getSelectedFields().findFirst().orElseThrow().getValue();

                assertInstanceOf(Object[].class, resultValue);
                assertArrayEquals(new Object[] {"a", "b", "c"}, (Object[]) resultValue);

            }

            @Test
            public void testAppendString() {
                final RecordPath recordPath = RecordPath.compile("arrayOf( /friends[*], 'Junior' )");
                final RecordPathResult result = recordPath.evaluate(record);
                final Object resultValue = result.getSelectedFields().findFirst().orElseThrow().getValue();

                assertInstanceOf(Object[].class, resultValue);
                assertArrayEquals(new Object[] {"John", "Jane", "Jacob", "Judy", "Junior"}, (Object[]) resultValue);
            }

            @Test
            public void testAppendSingleRecord() {
                final RecordPath recordPath = RecordPath.compile("arrayOf( /accounts[*], recordOf('id', '5555', 'balance', '123.45') )");
                final RecordPathResult result = recordPath.evaluate(record);
                final Object resultValue = result.getSelectedFields().findFirst().orElseThrow().getValue();

                assertInstanceOf(Object[].class, resultValue);
                final Object[] values = (Object[]) resultValue;
                assertEquals(3, values.length);

                assertInstanceOf(Record.class, values[2]);
                final Record added = (Record) values[2];
                assertEquals("5555", added.getValue("id"));
                assertEquals("123.45", added.getValue("balance"));
            }

            @Test
            public void testPrependSingleRecord() {
                final RecordPath recordPath = RecordPath.compile("arrayOf( recordOf('id', '5555', 'balance', '123.45'), /accounts[*] )");
                final RecordPathResult result = recordPath.evaluate(record);
                final Object resultValue = result.getSelectedFields().findFirst().orElseThrow().getValue();

                assertInstanceOf(Object[].class, resultValue);
                final Object[] values = (Object[]) resultValue;
                assertEquals(3, values.length);

                assertInstanceOf(Record.class, values[0]);
                final Record added = (Record) values[0];
                assertEquals("5555", added.getValue("id"));
                assertEquals("123.45", added.getValue("balance"));
            }


            @Test
            public void testAppendMultipleValues() {
                final RecordPath recordPath = RecordPath.compile("arrayOf( /accounts[*], recordOf('id', '5555', 'balance', '123.45'), /accounts[0] )");
                final RecordPathResult result = recordPath.evaluate(record);
                final Object resultValue = result.getSelectedFields().findFirst().orElseThrow().getValue();

                assertInstanceOf(Object[].class, resultValue);
                final Object[] values = (Object[]) resultValue;
                assertEquals(4, values.length);

                assertInstanceOf(Record.class, values[2]);
                final Record added = (Record) values[2];
                assertEquals("5555", added.getValue("id"));
                assertEquals("123.45", added.getValue("balance"));

                assertSame(values[0], values[3]);
            }

            @Test
            public void testWithUnescapeJson() {
                final RecordPath recordPath = RecordPath.compile("arrayOf( /accounts[*], unescapeJson('{\"id\": 5555, \"balance\": 123.45}', 'true') )");
                final RecordPathResult result = recordPath.evaluate(record);
                final Object resultValue = result.getSelectedFields().findFirst().orElseThrow().getValue();

                assertInstanceOf(Object[].class, resultValue);
                final Object[] values = (Object[]) resultValue;
                assertEquals(3, values.length);

                assertInstanceOf(Record.class, values[2]);
                final Record added = (Record) values[2];
                assertEquals(5555, added.getValue("id"));
                assertEquals(123.45, added.getValue("balance"));
            }
        }

        @Nested
        class Anchored {
            @Test
            public void allowsAnchoringRootContextOnAChildRecord() {
                final RecordPath recordPath = assertDoesNotThrow(
                        () -> RecordPath.compile("anchored(/mainAccount, concat(/id, '->', /balance))")
                );

                final FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
                assertEquals("1->123.45", fieldValue.getValue());
            }

            @Test
            public void allowsAnchoringRootContextOnAnArray() {
                final RecordPath recordPath = assertDoesNotThrow(
                        () -> RecordPath.compile("anchored(/accounts, concat(/id, '->', /balance))")
                );

                final List<FieldValue> fieldValues = evaluateMultiFieldValue(recordPath, record);
                assertAll(
                        () -> assertEquals(2, fieldValues.size()),
                        () -> assertEquals("6->10000.0", fieldValues.getFirst().getValue()),
                        () -> assertEquals("9->48.02", fieldValues.get(1).getValue())
                );
            }
        }

        @Nested
        class Base64Decode {
            private final Base64.Encoder encoder = Base64.getEncoder();

            @Test
            public void allowsToDecodeBase64EncodedByteArray() {
                final byte[] expectedBytes = "My bytes".getBytes(StandardCharsets.UTF_8);
                record.setValue("bytes", encoder.encode(expectedBytes));

                final FieldValue fieldValue = evaluateSingleFieldValue("base64Decode(/bytes)", record);
                assertArrayEquals(expectedBytes, (byte[]) fieldValue.getValue());
            }

            @Test
            public void allowsToDecodeBase64EncodedUtf8String() {
                final String expectedString = "My string";
                record.setValue("name", encoder.encodeToString(expectedString.getBytes(StandardCharsets.UTF_8)));

                final FieldValue fieldValue = evaluateSingleFieldValue("base64Decode(/name)", record);
                assertEquals(expectedString, fieldValue.getValue());
            }
        }

        @Nested
        class Base64Encode {
            private final Base64.Encoder encoder = Base64.getEncoder();

            @Test
            public void allowsToBase64EncodeByteArray() {
                final byte[] exampleBytes = "My bytes".getBytes(StandardCharsets.UTF_8);
                record.setValue("bytes", exampleBytes);

                final FieldValue fieldValue = evaluateSingleFieldValue("base64Encode(/bytes)", record);
                assertArrayEquals(encoder.encode(exampleBytes), (byte[]) fieldValue.getValue());
            }

            @Test
            public void allowsToBase64EncodeUtf8String() {
                final String exampleString = "My string";
                record.setValue("name", "My string");

                final FieldValue fieldValue = evaluateSingleFieldValue("base64Encode(/name)", record);
                assertEquals(encoder.encodeToString(exampleString.getBytes(StandardCharsets.UTF_8)), fieldValue.getValue());
            }
        }

        @Nested
        class Coalesce {
            @Test
            public void resolvesToFirstNonNullValueAmongNullValues() {
                record.setValue("name", null);
                record.setValue("firstName", null);
                record.setValue("lastName", "Eve");

                final FieldValue fieldValue = evaluateSingleFieldValue("coalesce(/name, /firstName, /lastName)", record);
                assertEquals("Eve", fieldValue.getValue());
            }

            @Test
            public void resolvesToFirstValueAmongNonNullValues() {
                record.setValue("name", "Alice");
                record.setValue("firstName", "Bob");
                record.setValue("lastName", "Eve");

                final FieldValue fieldValue = evaluateSingleFieldValue("coalesce(/name, /firstName, /lastName)", record);
                assertEquals("Alice", fieldValue.getValue());
            }

            @Test
            public void resolvesToNullWhenAllValuesAreNull() {
                record.setValue("name", null);
                record.setValue("firstName", null);
                record.setValue("lastName", null);

                final List<FieldValue> fieldValues =
                        evaluateMultiFieldValue("coalesce(/name, /firstName, /lastName)", record);
                assertEquals(List.of(), fieldValues);
            }

            @Test
            public void supportsLiteralValues() {
                record.setValue("name", null);
                record.setValue("firstName", "other");

                final FieldValue fieldValue = evaluateSingleFieldValue("coalesce(/name, 'default', /firstName)", record);
                assertEquals("default", fieldValue.getValue());
            }

            @Test
            public void supportsVariableNumberOfArguments() {
                final FieldValue singleArgumentFieldValue = evaluateSingleFieldValue("coalesce('single')", record);
                assertEquals("single", singleArgumentFieldValue.getValue());

                final String multiVariableRecordPath = Stream.concat(
                        Stream.generate(() -> "/missing").limit(1_000),
                        Stream.of("'multiple'")
                ).collect(Collectors.joining(", ", "coalesce(", ")"));
                final FieldValue multipleArgumentsFieldValue = evaluateSingleFieldValue(multiVariableRecordPath, record);
                assertEquals("multiple", multipleArgumentsFieldValue.getValue());
            }
        }

        @Nested
        class Concat {
            @Test
            public void concatenatesArgumentsIntoAString() {
                final FieldValue fieldValue = evaluateSingleFieldValue("concat(/firstName, /attributes['state'])", record);
                assertEquals("JohnNY", fieldValue.getValue());
            }

            @Test
            public void supportsNumericalValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("concat(/id, /mainAccount/balance)", record);
                assertEquals("48123.45", fieldValue.getValue());
            }

            @Test
            public void usesStringLiteralNullForNullValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("concat(/firstName, /missing)", record);
                assertEquals("Johnnull", fieldValue.getValue());
            }

            @Test
            public void supportsLiteralValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("concat('Hello NiFi', ' ', 2)", record);
                assertEquals("Hello NiFi 2", fieldValue.getValue());

            }
        }

        @Nested
        class Count {
            @Test
            public void countsTheNumberOfResultsOfARecordPath() {
                assertEquals(2L, evaluateSingleFieldValue("count(/attributes[*])", record).getValue());
                assertEquals(3L, evaluateSingleFieldValue("count(/friends[0, 1, 3])", record).getValue());
                assertEquals(1L, evaluateSingleFieldValue("count(/*[fieldName(.) = 'bytes'])", record).getValue());
            }

            @Test
            public void yieldsOneForReferencesToASingleFieldRegardlessOfItsValue() {
                assertAll(Stream.of("id", "name", "missing", "attributes", "friends", "mainAccount")
                        .map(fieldName -> () -> {
                                    FieldValue fieldValue = evaluateSingleFieldValue("count(/%s)".formatted(fieldName), record);
                                    assertEquals(1L, fieldValue.getValue());
                                }
                        ));
            }

            @Test
            public void yieldsOneForLiteralValues() {
                assertEquals(1L, evaluateSingleFieldValue("count('hello')", record).getValue());
                assertEquals(1L, evaluateSingleFieldValue("count(56)", record).getValue());
            }
        }

        @Nested
        class EscapeJson {
            @Test
            public void escapesReferencedStringValueAsStringLiteral() {
                assertEquals("\"John Doe\"", evaluateSingleFieldValue("escapeJson(/name)", record).getValue());
            }

            @Test
            public void escapesReferencedNumericValueAsNumberLiteral() {
                assertEquals("48", evaluateSingleFieldValue("escapeJson(/id)", record).getValue());
                assertEquals("123.45", evaluateSingleFieldValue("escapeJson(/mainAccount/balance)", record).getValue());
            }

            @Test
            public void escapesReferencedArrayValueAsArray() {
                assertEquals("[0,1,2,3,4,5,6,7,8,9]", evaluateSingleFieldValue("escapeJson(/numbers)", record).getValue());
                assertEquals("[\"John\",\"Jane\",\"Jacob\",\"Judy\"]", evaluateSingleFieldValue("escapeJson(/friends)", record).getValue());
            }

            @Test
            public void escapesReferencedMapValueAsObject() {
                assertEquals("{\"city\":\"New York\",\"state\":\"NY\"}", evaluateSingleFieldValue("escapeJson(/attributes)", record).getValue());
            }

            @Test
            public void escapesReferencedRecordValueAsObject() {
                assertEquals("{\"id\":1,\"balance\":123.45,\"address\":{\"city\":\"Boston\",\"state\":\"Massachusetts\"}}", evaluateSingleFieldValue("escapeJson(/mainAccount)", record).getValue());
            }

            @Test
            public void supportsEscapingWholeRecord() {
                final Record record = reduceRecord(TestRecordPath.this.record, "id", "attributes", "mainAccount", "numbers");

                assertEquals(
                        "{\"id\":48,\"attributes\":{\"city\":\"New York\",\"state\":\"NY\"}," +
                                "\"mainAccount\":{\"id\":1,\"balance\":123.45,\"address\":{\"city\":\"Boston\",\"state\":\"Massachusetts\"}}," +
                                "\"numbers\":[0,1,2,3,4,5,6,7,8,9]}",
                        evaluateSingleFieldValue("escapeJson(/)", record).getValue()
                );
            }
        }

        @Nested
        class FieldName {
            @Test
            public void yieldsNameOfReferencedFieldRegardlessOfItsValue() {
                assertEquals("id", evaluateSingleFieldValue("fieldName(/id)", record).getValue());
                assertEquals("missing", evaluateSingleFieldValue("fieldName(/missing)", record).getValue());
            }

            @Test
            public void supportsRecordPathsWithNoResults() {
                final List<FieldValue> fieldValues = evaluateMultiFieldValue("fieldName(/name[/id != /id])", record);
                assertEquals(List.of(), fieldValues);
            }

            @Test
            public void supportsRecordPathsWithMultipleResults() {
                final List<FieldValue> fieldValues = evaluateMultiFieldValue("fieldName(/mainAccount/*)", record);
                assertEquals(List.of("id", "balance", "address"), fieldValues.stream().map(FieldValue::getValue).toList());
            }

            @Test
            public void yieldsNameOfFunctionWhenPassedAFunction() {
                assertEquals("concat", evaluateSingleFieldValue("fieldName(concat('enate'))", record).getValue());
            }

            @Test
            public void throwsExceptionWhenPassedLiteralValue() {
                assertThrows(Exception.class, () -> evaluateSingleFieldValue("fieldName('whoops')", record));
            }
        }

        @Nested
        class Format {
            @Test
            public void supportsFormattingLongAsDateString() {
                final String localDate = "2017-10-20";
                final String instantFormatted = String.format("%sT12:30:45Z", localDate);

                record.setValue("id", Instant.parse(instantFormatted).toEpochMilli());

                assertEquals(localDate, evaluateSingleFieldValue("format(/id, 'yyyy-MM-dd')", record).getValue());
                assertEquals(instantFormatted, evaluateSingleFieldValue("format(/id, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", 'GMT')", record).getValue());
            }

            @Test
            public void supportsFormattingDateAsDateString() {
                final String localDate = "2017-10-20";
                final String instantFormatted = String.format("%sT12:30:45Z", localDate);

                record.setValue("id", new Date(Instant.parse(instantFormatted).toEpochMilli()));

                assertEquals(localDate, evaluateSingleFieldValue("format(/id, 'yyyy-MM-dd')", record).getValue());
                assertEquals(instantFormatted, evaluateSingleFieldValue("format(/id, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", 'GMT')", record).getValue());
            }

            @Test
            public void supportsDefiningFormatAsPathReference() {
                record.setValue("id", Date.valueOf("2024-08-18"));
                record.setValue("name", "yyyy-MM-dd");

                assertEquals("2024-08-18", evaluateSingleFieldValue("format(/id, /name)", record).getValue());
            }

            @Test
            public void supportsDefiningTimezoneAsPathReference() {
                final String instantFormatted = "2017-10-20T12:30:45Z";
                record.setValue("id", new Date(Instant.parse(instantFormatted).toEpochMilli()));
                record.setValue("name", "GMT");

                assertEquals(instantFormatted, evaluateSingleFieldValue("format(/id, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", /name)", record).getValue());
            }

            @Test
            public void yieldsValueUnchangedWhenFormatIsInvalid() {
                final Date originalValue = Date.valueOf("2024-08-18");
                record.setValue("id", originalValue);

                assertEquals(originalValue, evaluateSingleFieldValue("format(/id, 'INVALID')", record).getValue());
            }

            @Test
            public void yieldsValueUnchangedWhenAppliedOnAFieldWithNeitherLongNorDateCompatibleValue() {
                final List<String> nonLongOrDateFields = List.of("date", "attributes", "mainAccount", "numbers", "bytes");

                assertAll(nonLongOrDateFields.stream().map(fieldName -> () -> {
                    final FieldValue fieldValue = evaluateSingleFieldValue("format(/%s, 'yyyy-MM-dd')".formatted(fieldName), record);
                    assertEquals(record.getValue(fieldName), fieldValue.getValue());
                }));
            }
        }

        @Nested
        class Hash {
            @Test
            public void canCalculateSha512HashValues() {
                assertEquals(
                        "1fcb45d41a91df3139cb682a7895cf39636bab30d7f464943ca4f2287f72c06f4c34b10d203b26ccca06e9051c024252657302dd8ad3b2086c6bfd9bd34fa407",
                        evaluateSingleFieldValue("hash(/name, 'SHA-512')", record).getValue()
                );
            }

            @Test
            public void canCalculateMd5HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'MD5')", record);
                assertEquals("4c2a904bafba06591225113ad17b5cec", fieldValue.getValue());
            }

            @Test
            public void canCalculateSha384HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'SHA-384')", record);
                assertEquals("d0e24ff9f82e2a6b35409aec172e64b363f2dd26d8881d19f63214d5552357a40e32ac874a587d3fcf43ec86299eb001", fieldValue.getValue());
            }

            @Test
            public void canCalculateSha224HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'SHA-224')", record);
                assertEquals("20b058bc065abdbbd674123ed539286fa2765589424c38cd9e27b748", fieldValue.getValue());
            }

            @Test
            public void canCalculateSha256HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'SHA-256')", record);
                assertEquals("6cea57c2fb6cbc2a40411135005760f241fffc3e5e67ab99882726431037f908", fieldValue.getValue());
            }

            @Test
            public void canCalculateMd2HashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'MD2')", record);
                assertEquals("bb4d5a2fb65820445e54f00d629e1127", fieldValue.getValue());
            }

            @Test
            public void canCalculateShaHashValues() {
                final FieldValue fieldValue = evaluateSingleFieldValue("hash(/name, 'SHA')", record);
                assertEquals("ae6e4d1209f17b460503904fad297b31e9cf6362", fieldValue.getValue());
            }

            @Test
            public void throwsRecordPathExceptionOnUnsupportedAlgorithm() {
                assertThrows(RecordPathException.class, () -> evaluateSingleFieldValue("hash(/name, 'NOT_A_ALGO')", record));
            }

            @Test
            public void supportsProvidingAlgorithmAsReference() {
                record.setValue("name", "MD5");
                assertEquals("7f138a09169b250e9dcb378140907378", evaluateSingleFieldValue("hash(/name, /name)", record).getValue());
            }
        }

        @Nested
        class Join {
            @Test
            public void yieldsStringConsistingOfPassedArgumentsJoinedByDeclaredSeparator() {
                assertEquals("John, Doe, John Doe", evaluateSingleFieldValue("join(', ', /firstName, /lastName, /name)", record).getValue());
            }

            @Test
            public void yieldsArgumentsAsIsWhenGivenSingleArgumentBesidesSeparator() {
                assertEquals("John Doe", evaluateSingleFieldValue("join('---', /name)", record).getValue());
            }

            @Test
            public void supportsLiteralValuesAsArguments() {
                assertEquals("Nyan Cat", evaluateSingleFieldValue("join(' ', 'Nyan', 'Cat')", record).getValue());
            }

            @Test
            public void usesItemsAsArgumentsWhenGivenAnArray() {
                assertEquals("John Jane Jacob Doe", evaluateSingleFieldValue("join(' ', /firstName, /friends[1..2], /lastName)", record).getValue());
            }
        }

        @Nested
        class MapOf {
            @Test
            public void generatesMapOfStringFromProvidedArguments() {
                Map<String, String> expectedResult = Map.of(
                        "id", "48",
                        "fullName", "John Doe",
                        "money", "123.45",
                        "nullStringLiteral", "null"
                );

                final FieldValue fieldValue = evaluateSingleFieldValue(
                        "mapOf('id', /id, 'fullName', /name, 'money', /mainAccount/balance, 'nullStringLiteral', /missing)", record
                );

                assertEquals(expectedResult, fieldValue.getValue());
            }

            @Test
            public void throwsRecordPathExceptionWhenPassedAnOddAmountOfArguments() {
                assertThrows(RecordPathException.class, () -> RecordPath.compile("mapOf('firstName', /firstName, 'lastName')").evaluate(record));
            }
        }

        @Nested
        class PadLeft {
            @Test
            public void padsLeftOfStringWithUnderscoresUpToDesiredLength() {
                assertEquals("___John Doe", evaluateSingleFieldValue("padLeft(/name, 11)", record).getValue());
            }

            @Test
            public void supportsDefiningSingleCharacterPadValue() {
                assertEquals("@@John Doe", evaluateSingleFieldValue("padLeft(/name, 10, '@')", record).getValue());
            }

            @Test
            public void supportsDefiningMultiCharacterPadValue() {
                assertEquals("abcJohn Doe", evaluateSingleFieldValue("padLeft(/name, 11, 'abc')", record).getValue());
                assertEquals("aJohn Doe", evaluateSingleFieldValue("padLeft(/name, 9, 'abc')", record).getValue());
                assertEquals("abcabJohn Doe", evaluateSingleFieldValue("padLeft(/name, 13, 'abc')", record).getValue());
            }

            @Test
            public void supportsDefiningPadValueByPathReference() {
                assertEquals("JohnDoe", evaluateSingleFieldValue("padLeft(/lastName, 7, /firstName)", record).getValue());
            }

            @Test
            public void usesDefaultPadValueWhenProvidedEmptyPadValue() {
                assertEquals("___John Doe", evaluateSingleFieldValue("padLeft(/name, 11, '')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenAppliedOnAFieldWithStringValueOfDesiredLength() {
                assertEquals("John Doe", evaluateSingleFieldValue("padLeft(/name, 8)", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenAppliedOnAFieldWithStringValueLongerDesiredLength() {
                assertEquals("John Doe", evaluateSingleFieldValue("padLeft(/name, 3)", record).getValue());
            }

            @Test
            public void yieldsNullWhenAppliedOnAFieldWithValueOfNull() {
                assertNull(evaluateSingleFieldValue("padLeft(/missing, 10)", record).getValue());
            }
        }

        @Nested
        class PadRight {
            @Test
            public void padsRightOfStringWithUnderscoresUpToDesiredLength() {
                assertEquals("John Doe___", evaluateSingleFieldValue("padRight(/name, 11)", record).getValue());
            }

            @Test
            public void supportsDefiningSingleCharacterPadValue() {
                assertEquals("John Doe@@", evaluateSingleFieldValue("padRight(/name, 10, '@')", record).getValue());
            }

            @Test
            public void supportsDefiningMultiCharacterPadValue() {
                assertEquals("John Doeabc", evaluateSingleFieldValue("padRight(/name, 11, 'abc')", record).getValue());
                assertEquals("John Doea", evaluateSingleFieldValue("padRight(/name, 9, 'abc')", record).getValue());
                assertEquals("John Doeabcab", evaluateSingleFieldValue("padRight(/name, 13, 'abc')", record).getValue());
            }

            @Test
            public void supportsDefiningPadValueByPathReference() {
                assertEquals("DoeJohn", evaluateSingleFieldValue("padRight(/lastName, 7, /firstName)", record).getValue());
            }

            @Test
            public void usesDefaultPadValueWhenProvidedEmptyPadValue() {
                assertEquals("John Doe___", evaluateSingleFieldValue("padRight(/name, 11, '')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenAppliedOnAFieldWithStringValueOfDesiredLength() {
                assertEquals("John Doe", evaluateSingleFieldValue("padRight(/name, 8)", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenAppliedOnAFieldWithStringValueLongerDesiredLength() {
                assertEquals("John Doe", evaluateSingleFieldValue("padRight(/name, 3)", record).getValue());
            }

            @Test
            public void yieldsNullWhenAppliedOnAFieldWithValueOfNull() {
                assertNull(evaluateSingleFieldValue("padRight(/missing, 10)", record).getValue());
            }
        }

        @Nested
        class RecordOf {
            @Test
            public void createsRecordFromReferencedFields() {
                assertRecordOf(
                        "recordOf('mappedLong', /id, 'mappedString', /name)",
                        Map.of("id", "mappedLong", "name", "mappedString")
                );
            }

            @Test
            public void throwsRecordPathExceptionWhenPassedAnOddAmountOfArguments() {
                assertThrows(RecordPathException.class, () -> RecordPath.compile("recordOf('firstName', /firstName, 'lastName')").evaluate(record));
            }

            @Test
            public void supportsReferencesToFieldsOfTypeMap() {
                assertRecordOf(
                        "recordOf('mappedMap', /attributes)",
                        Map.of("attributes", "mappedMap")
                );
            }

            @Test
            public void supportsReferencesToFieldsOfTypeArray() {
                assertRecordOf(
                        "recordOf('mappedArray', /bytes)",
                        Map.of("bytes", "mappedArray")
                );
            }

            @Test
            public void supportsReferencesToFieldsOfTypeRecord() {
                assertRecordOf(
                        "recordOf('mappedRecord', /mainAccount)",
                        Map.of("mainAccount", "mappedRecord")
                );
            }

            @Test
            public void supportsPathReferenceToMissingValue() {
                final Map<String, DataType> expectedFieldTypes = Map.of(
                        "missingValue", record.getSchema().getDataType("missing").orElseThrow(),
                        "nonExisting", choiceTypeOf(RecordFieldType.STRING, RecordFieldType.RECORD) // fallback used when field is not defined in source
                );
                final Map<String, Object> expectedFieldValues = new HashMap<>();
                expectedFieldValues.put("nonExisting", null);

                assertRecordOf(
                        "recordOf('missingValue', /missing, 'nonExisting', /nonExistingField)",
                        expectedFieldTypes,
                        expectedFieldValues
                );
            }

            @Test
            public void supportsCreatingRecordWithFieldNameFromPathReference() {
                final Map<String, DataType> expectedFieldTypes = Map.of(
                        "John", RecordFieldType.STRING.getDataType()
                );
                final Map<String, Object> expectedFieldValues = Map.of(
                        "John", "Doe"
                );

                assertRecordOf(
                        "recordOf(/firstName, /lastName)",
                        expectedFieldTypes,
                        expectedFieldValues
                );
            }

            @Test
            public void supportsCreatingRecordFromLiteralValue() {
                final Map<String, DataType> expectedFieldTypes = Map.of(
                        "aNumber", RecordFieldType.INT.getDataType(),
                        "aString", RecordFieldType.STRING.getDataType()
                );
                final Map<String, Object> expectedFieldValues = Map.of(
                        "aNumber", 2012,
                        "aString", "aValue"
                );

                assertRecordOf(
                        "recordOf('aNumber', 2012, 'aString', 'aValue')",
                        expectedFieldTypes,
                        expectedFieldValues
                );
            }

            private void assertRecordOf(final String path, final Map<String, String> originalToMappedFieldNames) {
                final Map<String, DataType> expectedFieldTypes = originalToMappedFieldNames.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getValue,
                                originalToMappedFieldName -> {
                                    final String originalFieldName = originalToMappedFieldName.getKey();
                                    return record.getSchema().getDataType(originalFieldName).orElseThrow();
                                }
                        ));
                final Map<String, Object> expectedFieldValues = originalToMappedFieldNames.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getValue,
                                originalToMappedFieldName -> {
                                    final String originalFieldName = originalToMappedFieldName.getKey();
                                    return record.getValue(originalFieldName);
                                }
                        ));

                assertRecordOf(path, expectedFieldTypes, expectedFieldValues);
            }

            private void assertRecordOf(
                    final String path,
                    final Map<String, DataType> expectedFieldTypes,
                    final Map<String, Object> expectedFieldValues
            ) {
                final FieldValue result = evaluateSingleFieldValue(path, record);

                assertEquals(RecordFieldType.RECORD, result.getField().getDataType().getFieldType());

                final Object fieldValue = result.getValue();
                assertInstanceOf(Record.class, fieldValue);
                final Record recordValue = (Record) fieldValue;

                assertAll(Stream.concat(
                        expectedFieldTypes.entrySet().stream().map(expectation -> () -> {
                            final DataType expectedFieldType = expectation.getValue();
                            final RecordField actualRecordField =
                                    recordValue.getSchema().getField(expectation.getKey()).orElseThrow();

                            assertEquals(expectedFieldType, actualRecordField.getDataType());
                        }),
                        expectedFieldValues.entrySet().stream().map(expectation ->
                                () -> assertEquals(expectation.getValue(), recordValue.getValue(expectation.getKey()))
                        )
                ));
            }
        }

        @Nested
        class Replace {
            @Test
            public void replacesSearchTermInsideStringWithReplacementValue() {
                assertEquals("Jane Doe", evaluateSingleFieldValue("replace(/name, 'ohn', 'ane')", record).getValue());
            }

            @Test
            public void replacesAllOccurrencesOfTheSearchTerm() {
                assertEquals("JXhn DXe", evaluateSingleFieldValue("replace(/name, 'o', 'X')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenItDoesNotContainSearchTerm() {
                assertEquals("John Doe", evaluateSingleFieldValue("replace(/name, 'nomatch', 'X')", record).getValue());
            }
        }

        @Nested
        class ReplaceNull {
            @Test
            public void yieldsReplacementValueWhenAppliedOnAFieldWithValueOfNull() {
                assertEquals(14, evaluateSingleFieldValue("replaceNull(/missing, 14)", record).getValue());
            }

            @Test
            public void yieldsValueUnchangedWhenAppliedOnAFieldWithNonNullValue() {
                assertEquals("John", evaluateSingleFieldValue("replaceNull(/firstName, 17)", record).getValue());
            }

            @Test
            public void supportsDefiningReplacementValueByPathReference() {
                assertEquals(48, evaluateSingleFieldValue("replaceNull(/missing, /id)", record).getValue());
            }
        }

        @Nested
        class ReplaceRegex {
            @Test
            public void supportsReplacementOfLiteralValue() {
                assertEquals("Jane Doe", evaluateSingleFieldValue("replaceRegex(/name, 'ohn', 'ane')", record).getValue());
            }

            @Test
            public void replacesAllMatches() {
                assertEquals("JXhn DXe", evaluateSingleFieldValue("replaceRegex(/name, 'o', 'X')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenPatternDoesNotMatch() {
                assertEquals("John Doe", evaluateSingleFieldValue("replaceRegex(/name, 'nomatch', 'X')", record).getValue());
            }

            @Test
            public void supportsReplacementOfCharacterSet() {
                assertEquals("Jxhn Dxx", evaluateSingleFieldValue("replaceRegex(/name, '[aeiou]', 'x')", record).getValue());
            }

            @Test
            public void supportsReplacementOfPreDefinedCharacterSet() {
                assertEquals("xxxx xxx", evaluateSingleFieldValue("replaceRegex(/name, '\\w', 'x')", record).getValue());
            }

            @Test
            public void supportsReplacementUsingOrCondition() {
                assertEquals("Xohn Xoe", evaluateSingleFieldValue("replaceRegex(/name, 'J|D', 'X')", record).getValue());
            }

            @Test
            public void supportsReplacementUsingDotWildcard() {
                record.setValue("name", "Joe");
                assertEquals("XXX", evaluateSingleFieldValue("replaceRegex(/name, '.', 'X')", record).getValue());
            }

            @Test
            public void dotWildcardMatchesSpaceAndTabButNotCarriageReturnAndNewlineCharacters() {
                record.setValue("name", " \n\r\t");
                assertEquals("W\n\rW", evaluateSingleFieldValue("replaceRegex(/name, '.', 'W')", record).getValue());
            }

            @Test
            public void supportsMatchingFromStart() {
                record.setValue("name", "Bob Bobby");
                assertEquals("Hello Bobby", evaluateSingleFieldValue("replaceRegex(/name, '^Bob', 'Hello')", record).getValue());
            }

            @Test
            public void supportsMatchingToEnd() {
                record.setValue("name", "Bobby Bob");
                assertEquals("Bobby Bobson", evaluateSingleFieldValue("replaceRegex(/name, 'Bob$', 'Bobson')", record).getValue());
            }

            @Test
            public void supportsUsingValueOfCaptureGroup() {
                assertEquals("Jxohn Dxoe", evaluateSingleFieldValue("replaceRegex(/name, '([JD])', '$1x')", record).getValue());
            }

            @Test
            public void supportsUsingValueOfNamedCaptureGroup() {
                assertEquals("Jxohn Dxoe", evaluateSingleFieldValue("replaceRegex(/name, '(?<hello>[JD])', '${hello}x')", record).getValue());
            }

            @Test
            public void supportsToEscapeRegexReservedCharacters() {
                record.setValue("name", ".^$*+?()[{\\|");
                // NOTE: At Java code, a single back-slash needs to be escaped with another-back slash, but needn't do so at NiFi UI.
                //       The test record path is equivalent to replaceRegex(/name, '\.', 'x')
                assertEquals("x^$*+?()[{\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\.', 'x')", record).getValue());
                assertEquals(".x$*+?()[{\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\^', 'x')", record).getValue());
                assertEquals(".^x*+?()[{\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\$', 'x')", record).getValue());
                assertEquals(".^$x+?()[{\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\*', 'x')", record).getValue());
                assertEquals(".^$*x?()[{\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\+', 'x')", record).getValue());
                assertEquals(".^$*+x()[{\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\?', 'x')", record).getValue());
                assertEquals(".^$*+?x)[{\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\(', 'x')", record).getValue());
                assertEquals(".^$*+?(x[{\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\)', 'x')", record).getValue());
                assertEquals(".^$*+?()x{\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\[', 'x')", record).getValue());
                assertEquals(".^$*+?()[x\\|", evaluateSingleFieldValue("replaceRegex(/name, '\\{', 'x')", record).getValue());
                assertEquals(".^$*+?()[{x|", evaluateSingleFieldValue("replaceRegex(/name, '\\\\', 'x')", record).getValue());
                assertEquals(".^$*+?()[{\\x", evaluateSingleFieldValue("replaceRegex(/name, '\\|', 'x')", record).getValue());
            }

            @Test
            public void supportsDefiningTargetValueAsLiteralValue() {
                assertEquals("Jane Doe", evaluateSingleFieldValue("replaceRegex('John Doe', 'John', 'Jane')", record).getValue());
            }

            @Test
            public void supportsDefiningPatternByPathReference() {
                record.setValue("firstName", "ohn");
                assertEquals("Jane Doe", evaluateSingleFieldValue("replaceRegex(/name, /firstName, 'ane')", record).getValue());
            }

            @Test
            public void supportsDefiningReplacementValueByPathReference() {
                assertEquals("J48n Doe", evaluateSingleFieldValue("replaceRegex(/name, 'oh', /id)", record).getValue());
            }
        }

        @Nested
        class Substring {
            @Test
            public void yieldsTheSubstringBetweenTheDefinedStartIndexInclusiveAndEndIndexExclusive() {
                assertEquals("hn D", evaluateSingleFieldValue("substring(/name, 2, 6)", record).getValue());
            }

            @Test
            public void coercesEndIndexToStringLengthWhenDefinedEndIndexIsGreaterThanStringLength() {
                assertEquals(" Doe", evaluateSingleFieldValue("substring(/name, 4, 10000)", record).getValue());
            }

            @Test
            public void supportsIndexBasedOnEndOfString() {
                assertEquals("hn D", evaluateSingleFieldValue("substring(/name, -7, -3)", record).getValue());
            }

            @Test
            public void yieldsEmptyStringWhenStartIndexIsGreaterThanOrEqualToStringLength() {
                assertEquals("", evaluateSingleFieldValue("substring(/name, 8, 10)", record).getValue());
            }

            @Test
            public void yieldsEmptyStringWhenStartIndexEqualsEndIndex() {
                assertEquals("", evaluateSingleFieldValue("substring(/name, 3, 3)", record).getValue());
            }

            @Test
            public void yieldsNoResultWhenAppliedOnAFieldWithValueOfNull() {
                final List<FieldValue> fieldValues = evaluateMultiFieldValue("substring(/missing, 1, 2)", record);
                assertEquals(List.of(), fieldValues);
            }

            @Test
            public void supportsLiteralAsTargetValue() {
                assertEquals("Bobby", evaluateSingleFieldValue("substring('Bob Bobby Bobson', 4, 9)", record).getValue());
            }

            @Test
            public void supportsPathReferencesAsIndices() {
                record.setValue("id", 2);
                final Record mainAccountRecord = TestRecordPath.this.mainAccountRecord;
                mainAccountRecord.setValue("id", 6);

                assertEquals("hn D", evaluateSingleFieldValue("substring(/name, /id, /mainAccount/id)", record).getValue());
            }
        }

        @Nested
        class SubstringAfter {
            @Test
            public void yieldsSubstringAfterFirstOccurrenceOfSearchTerm() {
                assertEquals("hn Doe", evaluateSingleFieldValue("substringAfter(/name, 'o')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenSearchTermIsNotPartOfString() {
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfter(/name, 'x')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenSearchTermIsEmpty() {
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfter(/name, '')", record).getValue());
            }

            @Test
            public void yieldsEmptyStringWhenFirstOccurrenceOfSearchTermIsAtEndOfString() {
                assertEquals("", evaluateSingleFieldValue("substringAfter(/name, 'e')", record).getValue());
            }

            @Test
            public void supportsMultiCharacterSearchTerms() {
                assertEquals("n Doe", evaluateSingleFieldValue("substringAfter(/name, 'oh')", record).getValue());
            }

            @Test
            public void supportsLiteralAsTargetValue() {
                assertEquals(" Bobson", evaluateSingleFieldValue("substringAfter('Bob Bobby Bobson', 'y')", record).getValue());
            }

            @Test
            public void supportsPathReferencesAsSearchTerm() {
                assertEquals(" Doe", evaluateSingleFieldValue("substringAfter(/name, /firstName)", record).getValue());
            }
        }

        @Nested
        class SubstringAfterLast {
            @Test
            public void yieldsSubstringAfterLastOccurrenceOfSearchTerm() {
                assertEquals("e", evaluateSingleFieldValue("substringAfterLast(/name, 'o')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenSearchTermIsNotPartOfString() {
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfterLast(/name, 'x')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenSearchTermIsEmpty() {
                assertEquals("John Doe", evaluateSingleFieldValue("substringAfterLast(/name, '')", record).getValue());
            }

            @Test
            public void yieldsEmptyStringWhenLastOccurrenceOfSearchTermIsAtEndOfString() {
                assertEquals("", evaluateSingleFieldValue("substringAfterLast(/name, 'e')", record).getValue());
            }

            @Test
            public void supportsMultiCharacterSearchTerms() {
                assertEquals("n Doe", evaluateSingleFieldValue("substringAfterLast(/name, 'oh')", record).getValue());
            }

            @Test
            public void supportsLiteralAsTargetValue() {
                assertEquals(" Bobson", evaluateSingleFieldValue("substringAfterLast('Bob Bobby Bobson', 'y')", record).getValue());
            }

            @Test
            public void supportsPathReferencesAsSearchTerm() {
                assertEquals(" Doe", evaluateSingleFieldValue("substringAfterLast(/name, /firstName)", record).getValue());
            }
        }

        @Nested
        class SubstringBefore {
            @Test
            public void yieldsSubstringBeforeFirstOccurrenceOfSearchTerm() {
                assertEquals("John", evaluateSingleFieldValue("substringBefore(/name, ' ')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenSearchTermIsNotPartOfString() {
                assertEquals("John Doe", evaluateSingleFieldValue("substringBefore(/name, 'x')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenSearchTermIsEmpty() {
                assertEquals("John Doe", evaluateSingleFieldValue("substringBefore(/name, '')", record).getValue());
            }

            @Test
            public void yieldsEmptyStringWhenFirstOccurrenceOfSearchTermIsAtStartOfString() {
                assertEquals("", evaluateSingleFieldValue("substringBefore(/name, 'J')", record).getValue());
            }

            @Test
            public void supportsMultiCharacterSearchTerms() {
                assertEquals("John ", evaluateSingleFieldValue("substringBefore(/name, 'Do')", record).getValue());
            }

            @Test
            public void supportsLiteralAsTargetValue() {
                assertEquals("Bob Bobb", evaluateSingleFieldValue("substringBefore('Bob Bobby Bobson', 'y')", record).getValue());
            }

            @Test
            public void supportsPathReferencesAsSearchTerm() {
                assertEquals("John ", evaluateSingleFieldValue("substringBefore(/name, /lastName)", record).getValue());
            }
        }

        @Nested
        class SubstringBeforeLast {
            @Test
            public void yieldsSubstringBeforeLastOccurrenceOfSearchTerm() {
                assertEquals("John D", evaluateSingleFieldValue("substringBeforeLast(/name, 'o')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenSearchTermIsNotPartOfString() {
                assertEquals("John Doe", evaluateSingleFieldValue("substringBeforeLast(/name, 'x')", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenSearchTermIsEmpty() {
                assertEquals("John Doe", evaluateSingleFieldValue("substringBeforeLast(/name, '')", record).getValue());
            }

            @Test
            public void yieldsEmptyStringWhenLastOccurrenceOfSearchTermIsAtStartOfString() {
                assertEquals("", evaluateSingleFieldValue("substringBeforeLast(/name, 'J')", record).getValue());
            }

            @Test
            public void supportsMultiCharacterSearchTerms() {
                assertEquals("John ", evaluateSingleFieldValue("substringBeforeLast(/name, 'Do')", record).getValue());
            }

            @Test
            public void supportsLiteralAsTargetValue() {
                assertEquals("Bob Bobb", evaluateSingleFieldValue("substringBeforeLast('Bob Bobby Bobson', 'y')", record).getValue());
            }

            @Test
            public void supportsPathReferencesAsSearchTerm() {
                assertEquals("John ", evaluateSingleFieldValue("substringBeforeLast(/name, /lastName)", record).getValue());
            }
        }

        @Nested
        class ToBytes {
            @Test
            public void encodesStringAsBytesUsingTheDefinedCharset() {
                final String originalValue = "Hello World!";
                record.setValue("name", originalValue);

                assertArrayEquals(originalValue.getBytes(StandardCharsets.UTF_16LE), (byte[]) evaluateSingleFieldValue("toBytes(/name, 'UTF-16LE')", record).getValue());
            }

            @Test
            public void supportsDefiningCharsetAsPathReference() {
                final String originalValue = "Hello World!";
                record.setValue("name", originalValue);
                record.setValue("firstName", "UTF-8");

                assertArrayEquals(originalValue.getBytes(StandardCharsets.UTF_8), (byte[]) evaluateSingleFieldValue("toBytes(/name, /firstName)", record).getValue());
            }

            @Test
            public void throwsExceptionWhenPassedAnNonExistingCharset() {
                assertThrows(IllegalCharsetNameException.class, () -> evaluateSingleFieldValue("toBytes(/name, 'NOT A REAL CHARSET')", record));
            }
        }

        @Nested
        class ToDate {
            @Test
            public void supportsParsingStringToDateUsingSystemTimezone() {
                final String localDateString = "2017-10-20T12:30:45";
                final Date expectedValue = new Date(LocalDateTime.parse(localDateString).atZone(TEST_ZONE_ID).toInstant().toEpochMilli());

                record.setValue("date", localDateString);

                final FieldValue fieldValue = evaluateSingleFieldValue("toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\")", record);
                assertEquals(expectedValue, fieldValue.getValue());
            }

            @Test
            public void supportsParsingStringToDateUsingDefinedTimezone() {
                final String localDateTimeString = "2017-10-20T12:30:45";
                final ZonedDateTime expectedZonedDateTime = LocalDateTime.parse(localDateTimeString)
                        .atZone(TimeZone.getTimeZone("GMT+8:00").toZoneId());
                final Date expectedValue = new Date(expectedZonedDateTime.toInstant().toEpochMilli());

                record.setValue("date", localDateTimeString);

                final FieldValue fieldValue = evaluateSingleFieldValue("toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\", 'GMT+8:00')", record);
                assertEquals(expectedValue, fieldValue.getValue());
            }

            @Test
            public void supportsDefiningFormatAsPathReference() {
                final String localDateString = "2017-10-20T12:30:45";
                final Date expectedValue = new Date(LocalDateTime.parse(localDateString).atZone(TEST_ZONE_ID).toInstant().toEpochMilli());

                record.setValue("date", localDateString);
                record.setValue("name", "yyyy-MM-dd'T'HH:mm:ss");

                final FieldValue fieldValue = evaluateSingleFieldValue("toDate(/date, /name)", record);
                assertEquals(expectedValue, fieldValue.getValue());
            }

            @Test
            public void supportsDefiningTimezoneAsPathReference() {
                final String localDateTimeString = "2017-10-20T12:30:45";
                final String timeZone = "GMT+8:00";
                final ZonedDateTime expectedZonedDateTime = LocalDateTime.parse(localDateTimeString)
                        .atZone(TimeZone.getTimeZone(timeZone).toZoneId());
                final Date expectedValue = new Date(expectedZonedDateTime.toInstant().toEpochMilli());

                record.setValue("date", localDateTimeString);
                record.setValue("name", timeZone);

                final FieldValue fieldValue = evaluateSingleFieldValue("toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\", /name)", record);
                assertEquals(expectedValue, fieldValue.getValue());
            }

            @Test
            public void yieldsValueUnchangedWhenValueDoesNotMatchFormat() {
                final String originalValue = record.getAsString("date");

                assertEquals(originalValue, evaluateSingleFieldValue("toDate(/date, 'yyyy-MM-dd')", record).getValue());
            }

            @Test
            public void yieldsValueUnchangedWhenFormatIsInvalid() {
                final String originalValue = record.getAsString("date");

                assertEquals(originalValue, evaluateSingleFieldValue("toDate(/date, 'INVALID')", record).getValue());
            }

            @Test
            public void yieldsValueUnchangedWhenAppliedOnAFieldWithNonStringValue() {
                final List<String> nonStringFields = List.of("id", "attributes", "mainAccount", "numbers", "bytes");

                assertAll(nonStringFields.stream().map(fieldName -> () -> {
                    final FieldValue fieldValue = evaluateSingleFieldValue("toDate(/%s, 'yyyy-MM-dd')".formatted(fieldName), record);
                    assertEquals(record.getValue(fieldName), fieldValue.getValue());
                }));
            }
        }

        @Nested
        class ToLowerCase {
            @Test
            public void yieldsLowercaseRepresentationOfString() {
                assertEquals("john doe", evaluateSingleFieldValue("toLowerCase(/name)", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenStringIsLowercaseAlready() {
                record.setValue("name", "lower");

                assertEquals("lower", evaluateSingleFieldValue("toLowerCase(/name)", record).getValue());
            }

            @Test
            public void yieldsEmptyStringWhenAppliedOnAFieldWithValueOfNull() {
                assertEquals("", evaluateSingleFieldValue("toLowerCase(/missing)", record).getValue());
            }

            @Test
            public void supportsLiteralValue() {
                assertEquals("upper lower", evaluateSingleFieldValue("toLowerCase('UPPER lower')", record).getValue());
            }

            @Test
            public void canBeAppliedToReferenceResolvingMultipleFields() {
                record.setValue("friends", new String[]{"John Doe", "Jane Smith"});

                final List<FieldValue> results = evaluateMultiFieldValue("toLowerCase(/friends[*])", record);
                assertEquals("john doe", results.get(0).getValue());
                assertEquals("jane smith", results.get(1).getValue());
            }
        }

        @Nested
        class ToString {
            @Test
            public void decodesBytesAsStringUsingTheDefinedCharset() {
                record.setValue("bytes", "Hello World!".getBytes(StandardCharsets.UTF_16));

                assertEquals("Hello World!", evaluateSingleFieldValue("toString(/bytes, 'UTF-16')", record).getValue());
            }

            @Test
            public void supportsDefiningCharsetAsPathReference() {
                record.setValue("bytes", "Hello World!".getBytes(StandardCharsets.UTF_8));
                record.setValue("name", "UTF-8");

                assertEquals("Hello World!", evaluateSingleFieldValue("toString(/bytes, /name)", record).getValue());
            }

            @Test
            public void throwsExceptionWhenPassedAnNonExistingCharset() {
                assertThrows(IllegalCharsetNameException.class, () -> evaluateSingleFieldValue("toString(/bytes, 'NOT A REAL CHARSET')", record));
            }
        }

        @Nested
        class ToUpperCase {
            @Test
            public void yieldsUppercaseRepresentationOfString() {
                assertEquals("JOHN DOE", evaluateSingleFieldValue("toUpperCase(/name)", record).getValue());
            }

            @Test
            public void yieldsStringUnchangedWhenStringIsUppercaseAlready() {
                record.setValue("name", "UPPER");
                assertEquals("UPPER", evaluateSingleFieldValue("toUpperCase(/name)", record).getValue());
            }

            @Test
            public void yieldsEmptyStringWhenAppliedOnAFieldWithValueOfNull() {
                assertEquals("", evaluateSingleFieldValue("toUpperCase(/missing)", record).getValue());
            }

            @Test
            public void supportsLiteralValue() {
                assertEquals("UPPER LOWER", evaluateSingleFieldValue("toUpperCase('UPPER lower')", record).getValue());
            }

            @Test
            public void canBeAppliedToReferenceResolvingMultipleFields() {
                record.setValue("friends", new String[]{"John Doe", "Jane Smith"});

                final List<FieldValue> results = evaluateMultiFieldValue("toUpperCase(/friends[*])", record);
                assertEquals("JOHN DOE", results.get(0).getValue());
                assertEquals("JANE SMITH", results.get(1).getValue());
            }
        }

        @Nested
        class Trim {
            @Test
            public void removesWhitespaceFromStartOfString() {
                record.setValue("name", " \n\r\tJohn");

                assertEquals("John", evaluateSingleFieldValue("trim(/name)", record).getValue());
            }

            @Test
            public void removesWhitespaceFromEndOfString() {
                record.setValue("name", "John \n\r\t");

                assertEquals("John", evaluateSingleFieldValue("trim(/name)", record).getValue());
            }

            @Test
            public void keepsWhitespaceInBetweenNonWhitespaceCharacters() {
                record.setValue("name", " \n\r\tJohn Smith \n\r\t");

                assertEquals("John Smith", evaluateSingleFieldValue("trim(/name)", record).getValue());
            }

            @Test
            public void yieldsEmptyStringForMissingField() {
                assertEquals("", evaluateSingleFieldValue("trim(/missing)", record).getValue());
            }

            @Test
            public void supportsLiteralValue() {
                assertEquals("Bonjour", evaluateSingleFieldValue("trim(' Bonjour  ')", record).getValue());
            }

            @Test
            public void canBeAppliedToReferenceResolvingMultipleFields() {
                record.setValue("friends", new String[]{"   John Smith     ", "   Jane Smith     "});

                final List<FieldValue> results = evaluateMultiFieldValue("trim(/friends[*])", record);
                assertEquals("John Smith", results.get(0).getValue());
                assertEquals("Jane Smith", results.get(1).getValue());
            }
        }

        @Nested
        class UnescapeJson {
            @Test
            public void unescapesReferencedStringLiteralAsStringValue() {
                record.setValue("name", "\"John Doe\"");
                assertEquals("John Doe", evaluateSingleFieldValue("unescapeJson(/name)", record).getValue());
            }

            @Test
            public void unescapesReferencedNumberLiteralAsNumericValue() {
                record.setValue("id", "48");
                assertEquals(48, evaluateSingleFieldValue("unescapeJson(/id)", record).getValue());

                record.setValue("id", "123.45");
                assertEquals(123.45, evaluateSingleFieldValue("unescapeJson(/id)", record).getValue());
            }

            @Test
            public void unescapesReferencedArrayAsArrayValue() {
                record.setValue("numbers", "[0,1,2,3,4,5,6,7,8,9]");
                assertArrayEquals(new Integer[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, (Object[]) evaluateSingleFieldValue("unescapeJson(/numbers)", record).getValue());

                record.setValue("friends", "[\"John\",\"Jane\",\"Jacob\",\"Judy\"]");
                assertArrayEquals(new String[]{"John", "Jane", "Jacob", "Judy"}, (Object[]) evaluateSingleFieldValue("unescapeJson(/friends)", record).getValue());
            }

            @Test
            public void unescapesReferencedObjectAsMapValue() {
                record.setValue("name", "{\"id\":1,\"balance\":2.3,\"address\":{\"city\":\"New York\",\"state\":\"NY\"}}");
                final Map<String, Object> expectedMap = Map.of(
                        "id", 1,
                        "balance", 2.3,
                        "address", Map.of(
                                "city", "New York",
                                "state", "NY"
                        )
                );

                assertEquals(expectedMap, evaluateSingleFieldValue("unescapeJson(/name, 'false')", record).getValue());
                assertEquals(expectedMap, evaluateSingleFieldValue("unescapeJson(/name)", record).getValue());
            }

            @Test
            public void supportsToUnescapeReferencedObjectAsRecordValue() {
                record.setValue("name", "{\"id\":1,\"balance\":2.3,\"address\":{\"city\":\"New York\",\"state\":\"NY\"}}");
                final Record expectedRecord = createAccountRecord(1, 2.3, "New York", "NY");

                final FieldValue fieldValue = evaluateSingleFieldValue("unescapeJson(/name, 'true')", record);
                assertEquals(expectedRecord, fieldValue.getValue());
            }

            @Test
            public void supportsToUnescapeReferencedObjectAsRecordValueAndNestedObjectsAsMapValue() {
                record.setValue("name", "{\"id\":1,\"balance\":2.3,\"nested\":[{\"city\":\"New York\",\"state\":\"NY\"}]}");
                final Object[] expectedNestedValue = {Map.of("city", "New York", "state", "NY")};

                final FieldValue fieldValue = evaluateSingleFieldValue("unescapeJson(/name, 'true', 'false')", record);
                assertInstanceOf(Record.class, fieldValue.getValue());
                final Record actualRecord = (Record) fieldValue.getValue();
                assertEquals(1, actualRecord.getValue("id"));
                assertEquals(2.3, actualRecord.getValue("balance"));
                assertArrayEquals(expectedNestedValue, actualRecord.getAsArray("nested"));
            }

            @Test
            public void supportsToUnescapeReferencedObjectAndNestedObjectsAsRecordValue() {
                record.setValue("name", "{\"id\":1,\"balance\":2.3,\"nested\":[{\"city\":\"New York\",\"state\":\"NY\"}]}");
                Record[] expectedNestedValue = {createAddressRecord("New York", "NY")};

                final FieldValue fieldValue = evaluateSingleFieldValue("unescapeJson(/name, 'true', 'true')", record);
                assertInstanceOf(Record.class, fieldValue.getValue());
                final Record actualRecord = (Record) fieldValue.getValue();
                assertEquals(1, actualRecord.getValue("id"));
                assertEquals(2.3, actualRecord.getValue("balance"));
                assertArrayEquals(expectedNestedValue, actualRecord.getAsArray("nested"));
            }

            @Test
            public void throwsExceptionWhenAppliedToNonStringValue() {
                final List<String> nonStringFields = List.of("id", "attributes", "mainAccount", "numbers", "bytes");

                assertAll(nonStringFields.stream().map(fieldName -> () -> {
                    Exception exception =
                            assertThrows(Exception.class, () -> evaluateSingleFieldValue("unescapeJson(/%s)".formatted(fieldName), record));
                    assertEquals("Argument supplied to unescapeJson must be a String", exception.getMessage());
                }));
            }

            @Test
            public void throwsExceptionWhenAppliedToNonJsonStringValue() {
                record.setValue("name", "<xml>value</xml>");
                Exception exception =
                        assertThrows(Exception.class, () -> evaluateSingleFieldValue("unescapeJson(/name)", record));
                assertEquals("Unable to deserialise JSON String into Record Path value", exception.getMessage());
            }
        }

        @Nested
        class UUID5 {

            @Test
            public void supportsGenerationWithoutExplicitNamespace() {
                final String input = "testing NiFi functionality";

                record.setValue("firstName", input);

                final FieldValue fieldValue = evaluateSingleFieldValue("uuid5(/firstName)", record);

                final String value = fieldValue.getValue().toString();
                assertEquals(Uuid5Util.fromString(input, null), value);
            }

            @Test
            public void supportsGenerationWithExplicitNamespace() {
                final UUID namespace = UUID.fromString("67eb2232-f06e-406a-b934-e17f5fa31ae4");
                final String input = "testing NiFi functionality";

                record.setValue("firstName", input);
                record.setValue("lastName", namespace.toString());

                final FieldValue fieldValue = evaluateSingleFieldValue("uuid5(/firstName, /lastName)", record);

                final String value = fieldValue.getValue().toString();
                assertEquals(Uuid5Util.fromString(input, namespace.toString()), value);
            }
        }
    }

    @Nested
    class FilterFunctions {
        @Test
        public void singleResultRecordPathCanBeFiltered() {
            assertMatches("/name[contains(., 'John')]", record);
            assertNotMatches("/name[contains(., 'Jane')]", record);
        }

        @Test
        public void multiResultRecordPathCanBeFiltered() {
            final Record record = reduceRecord(TestRecordPath.this.record, "firstName", "lastName", "name");
            List<FieldValue> fieldValues = evaluateMultiFieldValue("/*[contains(., 'John')]", record);
            assertAll(
                    () -> assertEquals(2, fieldValues.size()),
                    () -> assertEquals("John", fieldValues.getFirst().getValue()),
                    () -> assertEquals("John Doe", fieldValues.get(1).getValue())
            );
            assertNotMatches("/*[contains(., 'Friedrich')]", record);
        }

        @Test
        public void filterCanBeAppliedOnRelativePath() {
            assertMatches("/mainAccount[./balance > 100]", record);
            assertNotMatches("/mainAccount[./balance <= 100]", record);
        }

        @Test
        public void filterReceivesNullForNonExistingRelativePath() {
            assertMatches("/name[isBlank(./nonExisting)]", record);
            assertMatches("/mainAccount[isBlank(./nonExisting)]", record);
            assertNotMatches("/mainAccount[isBlank(./balance)]", record);
        }

        @Test
        public void filterCanBeAppliedOnAbsolutePath() {
            assertMatches("/lastName[contains(/firstName, 'Jo')]", record);
            assertNotMatches("/lastName[contains(/firstName, 'Do')]", record);
        }

        @Test
        public void recordPathCanBuiltUponFilteredPath() {
            final FieldValue fieldValue = evaluateSingleFieldValue("/mainAccount[./balance > 100]/id", record);
            assertFieldValue(mainAccountRecord, "id", 1, fieldValue);

            assertNotMatches("/mainAccount[./balance < 100]/id", record);
        }

        @Test
        public void filterFunctionsCanBeUsedStandalone() {
            record.setValue("name", null);

            assertEquals(Boolean.TRUE, evaluateSingleFieldValue("isEmpty(/name)", record).getValue());
            assertEquals(Boolean.FALSE, evaluateSingleFieldValue("isEmpty(/id)", record).getValue());

            assertEquals(Boolean.TRUE, evaluateSingleFieldValue("/id = 48", record).getValue());
            assertEquals(Boolean.FALSE, evaluateSingleFieldValue("/id > 48", record).getValue());

            assertEquals(Boolean.FALSE, evaluateSingleFieldValue("not(/id = 48)", record).getValue());
        }

        @Test
        public void filterFunctionsCanBeChained() {
            assertMatches("/name[startsWith(contains(., 'John'), 't')]", record);
            assertNotMatches("/name[startsWith(contains(., 'Jane'), 't')]", record);
        }

        @Nested
        class Contains {
            @Test
            public void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[contains(., 'o')]", record);
                assertNotMatches("/name[contains(., 'x')]", record);
            }

            @Test
            public void supportsComparingWithStringReference() {
                assertMatches("/name[contains(., /friends[0])]", record);
                assertNotMatches("/name[contains(., /friends[1])]", record);
            }

            @Test
            public void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[contains(., /friends[2])]", record);
            }
        }

        @Nested
        class ContainsRegex {
            @Test
            public void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[containsRegex(., 'o[gh]n')]", record);
                assertNotMatches("/name[containsRegex(., 'o[xy]n')]", record);
            }

            @Test
            public void supportsComparingWithStringReference() {
                record.setValue("friends", new String[]{"o[gh]n", "o[xy]n"});
                assertMatches("/name[containsRegex(., /friends[0])]", record);
                assertNotMatches("/name[containsRegex(., /friends[1])]", record);
            }

            @Test
            public void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[containsRegex(., /friends[2])]", record);
            }
        }

        @Nested
        class EndsWith {
            @Test
            public void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[endsWith(., 'n Doe')]", record);
                assertNotMatches("/name[endsWith(., 'n Dont')]", record);
            }

            @Test
            public void supportsComparingWithStringReference() {
                record.setValue("friends", new String[]{"n Doe", "n Dont"});
                assertMatches("/name[endsWith(., /friends[0])]", record);
                assertNotMatches("/name[endsWith(., /friends[1])]", record);
            }

            @Test
            public void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[endsWith(., /friends[2])]", record);
            }

            @Test
            public void matchesWhenSearchValueIsEmpty() {
                assertMatches("/name[endsWith(., '')]", record);
            }
        }

        @Nested
        class Equals {
            @Test
            public void supportsArrayValuesByReference() {
                assertMatches("/friends[. = /friends]", record);
                assertNotMatches("/friends[. = /bytes]", record);
            }

            @Test
            public void supportsBigIntValuesByReference() {
                record.setValue("firstName", BigInteger.valueOf(42));
                record.setValue("lastName", BigInteger.valueOf(43));
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            @Test
            public void supportsBooleanValuesByReference() {
                record.setValue("firstName", true);
                record.setValue("lastName", false);
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
                assertNotMatches("/lastName[. = /firstName]", record);
                assertMatches("/lastName[. = /lastName]", record);
            }

            @Test
            public void supportsByteValuesByReference() {
                assertMatches("/bytes[0][. = /bytes[0]]", record);
                assertNotMatches("/bytes[0][. = /bytes[1]]", record);
            }

            @Test
            public void supportsCharValuesByReference() {
                record.setValue("firstName", 'k');
                record.setValue("lastName", 'o');
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            @Test
            public void supportsChoiceValuesByReference() {
                final DataType choiceType = choiceTypeOf(RecordFieldType.INT, RecordFieldType.STRING);
                final RecordSchema schema = recordSchemaOf(
                        recordFieldOf("firstChoice", choiceType),
                        recordFieldOf("secondChoice", choiceType)
                );
                final Record record = new MapRecord(schema, new HashMap<>(Map.of(
                        "firstChoice", "text",
                        "secondChoice", 4911
                )));

                assertMatches("/firstChoice[. = /firstChoice]", record);
                assertNotMatches("/firstChoice[. = /secondChoice]", record);
            }

            @Test
            public void supportsDateValuesByReference() {
                record.setValue("firstName", Date.valueOf("1998-04-29"));
                record.setValue("lastName", Date.valueOf("2001-12-06"));
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            @Test
            public void supportsEnumValuesByReference() {
                record.setValue("firstName", RecordFieldType.ENUM);
                record.setValue("lastName", RecordFieldType.BOOLEAN);
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            @Test
            public void supportsMapValuesByReference() {
                record.setValue("friends", new HashMap<>(Map.of("different", "entries")));
                assertMatches("/attributes[. = /attributes]", record);
                assertNotMatches("/attributes[. = /friends]", record);
            }

            @Test
            public void supportsNumericalValuesByLiteralValues() {
                assertMatches("/id[. = 48]", record);
                assertNotMatches("/id[. = 49]", record);
                assertMatches("/mainAccount/balance[. = '123.45']", record);
                assertNotMatches("/mainAccount/balance[. = '123.46']", record);
            }

            @Test
            public void supportsNumericalValuesByReference() {
                record.setValue("numbers", new Integer[]{48, 49});
                assertMatches("/id[. = /numbers[0]]", record);
                assertNotMatches("/id[. = /numbers[1]]", record);
                record.setValue("numbers", new Double[]{123.45, 123.46});
                assertMatches("/mainAccount/balance[. = /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. = /numbers[1]]", record);
            }

            @Test
            public void supportsRecordValuesByReference() {
                assertMatches("/mainAccount[. = /mainAccount]", record);
                assertNotMatches("/mainAccount[. = /accounts[1]]", record);
            }

            @Test
            public void supportsStringValuesByLiteralValues() {
                assertMatches("/name[. = 'John Doe']", record);
                assertNotMatches("/name[. = 'Jane Doe']", record);
            }

            @Test
            public void supportsStringValuesByReference() {
                record.setValue("name", record.getAsArray("friends")[0]);
                assertMatches("/name[. = /friends[0]]", record);
                assertNotMatches("/name[. = /friends[1]]", record);
            }

            @Test
            public void supportsTimeValuesByReference() {
                record.setValue("firstName", Time.valueOf("22:56:37"));
                record.setValue("lastName", Time.valueOf("20:53:14"));
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            @Test
            public void supportsTimestampValuesByReference() {
                record.setValue("firstName", Timestamp.valueOf("1998-04-29 22:56:37"));
                record.setValue("lastName", Timestamp.valueOf("2001-12-06 20:53:14"));
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }

            @Test
            public void supportsUUIDValuesByReference() {
                record.setValue("firstName", UUID.fromString("d0fb6ab5-20e6-4823-8190-1ab9f6173d12"));
                record.setValue("lastName", UUID.fromString("01234567-9012-3456-7890-123456789012"));
                assertMatches("/firstName[. = /firstName]", record);
                assertNotMatches("/firstName[. = /lastName]", record);
            }
        }

        @Nested
        class GreaterThan {
            @Test
            public void supportsComparingWithLongCompatibleLiteralValues() {
                assertMatches("/id[. > 47]", record);
                assertNotMatches("/id[. > 48]", record);
            }

            @Test
            public void supportsComparingWithLongCompatibleReference() {
                record.setValue("numbers", new Integer[]{47, 48});
                assertMatches("/id[. > /numbers[0]]", record);
                assertNotMatches("/id[. > /numbers[1]]", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleLiteralValues() {
                assertMatches("/mainAccount/balance[. > '122.99']", record);
                assertNotMatches("/mainAccount/balance[. > '123.45']", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleReference() {
                record.setValue("numbers", new Double[]{122.99d, 123.45d});
                assertMatches("/mainAccount/balance[. > /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. > /numbers[1]]", record);
            }

            @Test
            public void doesNotMatchOnNonNumberComparisons() {
                assertNotMatches("/name[. > 'Jane']", record);
                assertNotMatches("/name['Jane' > .]", record);
            }
        }

        @Nested
        class GreaterThanOrEqual {
            @Test
            public void supportsComparingWithLongCompatibleLiteralValues() {
                assertMatches("/id[. >= 48]", record);
                assertNotMatches("/id[. >= 49]", record);
            }

            @Test
            public void supportsComparingWithLongCompatibleReference() {
                record.setValue("numbers", new Integer[]{48, 49});
                assertMatches("/id[. >= /numbers[0]]", record);
                assertNotMatches("/id[. >= /numbers[1]]", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleLiteralValues() {
                assertMatches("/mainAccount/balance[. >= '122.99']", record);
                assertNotMatches("/mainAccount/balance[. >= '123.46']", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleReference() {
                record.setValue("numbers", new Double[]{122.99d, 124.00d});
                assertMatches("/mainAccount/balance[. >= /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. >= /numbers[1]]", record);
            }

            @Test
            public void doesNotMatchOnNonNumberComparisons() {
                assertNotMatches("/name[. >= 'Jane']", record);
                assertNotMatches("/name['Jane' >= .]", record);
            }
        }

        @Nested
        class IsBlank {
            @Test
            public void supportsStringLiteralValues() {
                assertMatches("/name[isBlank('')]", record);
            }

            @Test
            public void supportsStringReferenceValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isBlank(/firstName)]", record);
            }

            @Test
            public void matchesOnNullValues() {
                assertMatches("/name[isBlank(/missing)]", record);
            }

            @Test
            public void matchesOnEmptyStringValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isBlank(/firstName)]", record);
            }

            @Test
            public void matchesOnBlankStringValues() {
                record.setValue("firstName", " \r\n\t");
                assertMatches("/name[isBlank(/firstName)]", record);
            }

            @Test
            public void doesNotMatchOnStringContainingNonWhitespace() {
                record.setValue("firstName", " u ");
                assertNotMatches("/name[isBlank(/firstName)]", record);
            }
        }

        @Nested
        class IsEmpty {
            @Test
            public void supportsStringLiteralValues() {
                assertMatches("/name[isEmpty('')]", record);
            }

            @Test
            public void supportsStringReferenceValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isEmpty(/firstName)]", record);
            }

            @Test
            public void matchesOnNullValues() {
                assertMatches("/name[isEmpty(/missing)]", record);
            }

            @Test
            public void matchesOnEmptyStringValues() {
                record.setValue("firstName", "");
                assertMatches("/name[isEmpty(/firstName)]", record);
            }

            @Test
            public void doesNotMatchesOnBlankStringValues() {
                record.setValue("firstName", " \r\n\t");
                assertNotMatches("/name[isEmpty(/firstName)]", record);
            }

            @Test
            public void doesNotMatchOnStringContainingNonWhitespace() {
                record.setValue("firstName", "u");
                assertNotMatches("/name[isEmpty(/firstName)]", record);
            }
        }

        @Nested
        class LessThan {
            @Test
            public void supportsComparingWithLongCompatibleLiteralValues() {
                assertMatches("/id[. < 49]", record);
                assertNotMatches("/id[. < 48]", record);
            }

            @Test
            public void supportsComparingWithLongCompatibleReference() {
                record.setValue("numbers", new Integer[]{49, 48});
                assertMatches("/id[. < /numbers[0]]", record);
                assertNotMatches("/id[. < /numbers[1]]", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleLiteralValues() {
                assertMatches("/mainAccount/balance[. < '123.46']", record);
                assertNotMatches("/mainAccount/balance[. < '122.99']", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleReference() {
                record.setValue("numbers", new Double[]{124.00d, 122.99d});
                assertMatches("/mainAccount/balance[. < /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. < /numbers[1]]", record);
            }

            @Test
            public void doesNotMatchOnNonNumberComparisons() {
                assertNotMatches("/name[. < 'Jane']", record);
                assertNotMatches("/name['Jane' < .]", record);
            }
        }

        @Nested
        class LessThanOrEqual {
            @Test
            public void supportsComparingWithLongCompatibleLiteralValues() {
                assertMatches("/id[. <= 48]", record);
                assertNotMatches("/id[. <= 47]", record);
            }

            @Test
            public void supportsComparingWithLongCompatibleReference() {
                record.setValue("numbers", new Integer[]{48, 47});
                assertMatches("/id[. <= /numbers[0]]", record);
                assertNotMatches("/id[. <= /numbers[1]]", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleLiteralValues() {
                assertMatches("/mainAccount/balance[. <= '123.45']", record);
                assertNotMatches("/mainAccount/balance[. <= '122.99']", record);
            }

            @Test
            public void supportsComparingWithDoubleCompatibleReference() {
                record.setValue("numbers", new Double[]{123.45d, 122.99d});
                assertMatches("/mainAccount/balance[. <= /numbers[0]]", record);
                assertNotMatches("/mainAccount/balance[. <= /numbers[1]]", record);
            }

            @Test
            public void doesNotMatchOnNonNumberComparisons() {
                assertNotMatches("/name[. <= 'Jane']", record);
                assertNotMatches("/name['Jane' <= .]", record);
            }
        }

        @Nested
        class MatchesRegex {
            @Test
            public void matchesOnComparisonWithExactLiteralValue() {
                assertMatches("/name[matchesRegex(., 'John Doe')]", record);
                assertNotMatches("/name[matchesRegex(., 'Jon Doe')]", record);
            }

            @Test
            public void supportsCharacterSetInPattern() {
                assertMatches("/name[matchesRegex(., 'John D[aeiou]{2}')]", record);
            }

            @Test
            public void supportsPreDefinedCharacterSetInPattern() {
                assertMatches("/name[matchesRegex(., '\\w+ \\w+')]", record);
            }

            @Test
            public void supportsUsingOrConditionInPattern() {
                assertMatches("/name[matchesRegex(., '(J|D)ohn (J|D)oe')]", record);
            }

            @Test
            public void supportsUsingDotWildcardInPattern() {
                assertMatches("/name[matchesRegex(., 'J.*n Doe')]", record);
            }

            @Test
            public void dotWildcardMatchesSpaceAndTabButNotCarriageReturnAndNewlineCharacters() {
                record.setValue("name", " \t");
                assertMatches("/name[matchesRegex(., '.+')]", record);
                record.setValue("name", "\n\r");
                assertNotMatches("/name[matchesRegex(., '.+')]", record);
            }

            @Test
            public void supportsToEscapeRegexReservedCharacters() {
                record.setValue("name", ".^$*+?()[{\\|");
                // NOTE: At Java code, a single back-slash needs to be escaped with another-back slash, but needn't do so at NiFi UI.
                //       The test record path is equivalent to /name[matchesRegex(., '\.\^\$\*\+\?\(\)\[\{\\\|')]
                assertMatches("/name[matchesRegex(., '\\.\\^\\$\\*\\+\\?\\(\\)\\[\\{\\\\\\|')]", record);
            }

            @Test
            public void supportsDefiningPatternByPathReference() {
                record.setValue("firstName", ".+");
                assertMatches("/name[matchesRegex(., /firstName)]", record);
            }
        }

        @Nested
        class NotEquals {

            @Test
            public void supportsArrayValuesByReference() {
                assertNotMatches("/friends[. != /friends]", record);
                assertMatches("/friends[. != /bytes]", record);
            }

            @Test
            public void supportsBigIntValuesByReference() {
                record.setValue("firstName", BigInteger.valueOf(42));
                record.setValue("lastName", BigInteger.valueOf(43));
                assertNotMatches("/firstName[. != /firstName]", record);
                assertMatches("/firstName[. != /lastName]", record);
            }

            @Test
            public void supportsBooleanValuesByReference() {
                record.setValue("firstName", true);
                record.setValue("lastName", false);
                assertNotMatches("/firstName[. != /firstName]", record);
                assertMatches("/firstName[. != /lastName]", record);
                assertMatches("/lastName[. != /firstName]", record);
                assertNotMatches("/lastName[. != /lastName]", record);
            }

            @Test
            public void supportsByteValuesByReference() {
                assertNotMatches("/bytes[0][. != /bytes[0]]", record);
                assertMatches("/bytes[0][. != /bytes[1]]", record);
            }

            @Test
            public void supportsCharValuesByReference() {
                record.setValue("firstName", 'k');
                record.setValue("lastName", 'o');
                assertNotMatches("/firstName[. != /firstName]", record);
                assertMatches("/firstName[. != /lastName]", record);
            }

            @Test
            public void supportsDateValuesByReference() {
                record.setValue("firstName", Date.valueOf("1998-04-29"));
                record.setValue("lastName", Date.valueOf("2001-12-06"));
                assertNotMatches("/firstName[. != /firstName]", record);
                assertMatches("/firstName[. != /lastName]", record);
            }

            @Test
            public void supportsEnumValuesByReference() {
                record.setValue("firstName", RecordFieldType.ENUM);
                record.setValue("lastName", RecordFieldType.BOOLEAN);
                assertNotMatches("/firstName[. != /firstName]", record);
                assertMatches("/firstName[. != /lastName]", record);
            }

            @Test
            public void supportsMapValuesByReference() {
                record.setValue("friends", new HashMap<>(Map.of("different", "entries")));
                assertNotMatches("/attributes[. != /attributes]", record);
                assertMatches("/attributes[. != /friends]", record);
            }

            @Test
            public void supportsNumericalValuesByReference() {
                record.setValue("numbers", new Integer[]{48, 49});
                assertNotMatches("/id[. != /numbers[0]]", record);
                assertMatches("/id[. != /numbers[1]]", record);
                record.setValue("numbers", new Double[]{123.45, 123.46});
                assertNotMatches("/mainAccount/balance[. != /numbers[0]]", record);
                assertMatches("/mainAccount/balance[. != /numbers[1]]", record);
            }

            @Test
            public void supportsRecordValuesByReference() {
                assertNotMatches("/mainAccount[. != /mainAccount]", record);
                assertMatches("/mainAccount[. != /accounts[1]]", record);
            }

            @Test
            public void supportsStringValuesByLiteralValues() {
                assertNotMatches("/name[. != 'John Doe']", record);
                assertMatches("/name[. != 'Jane Doe']", record);
            }

            @Test
            public void supportsStringValuesByReference() {
                record.setValue("name", record.getAsArray("friends")[0]);
                assertNotMatches("/name[. != /friends[0]]", record);
                assertMatches("/name[. != /friends[1]]", record);
            }

            @Test
            public void supportsTimeValuesByReference() {
                record.setValue("firstName", Time.valueOf("22:56:37"));
                record.setValue("lastName", Time.valueOf("20:53:14"));
                assertNotMatches("/firstName[. != /firstName]", record);
                assertMatches("/firstName[. != /lastName]", record);
            }

            @Test
            public void supportsTimestampValuesByReference() {
                record.setValue("firstName", Timestamp.valueOf("1998-04-29 22:56:37"));
                record.setValue("lastName", Timestamp.valueOf("2001-12-06 20:53:14"));
                assertNotMatches("/firstName[. != /firstName]", record);
                assertMatches("/firstName[. != /lastName]", record);
            }

            @Test
            public void supportsUUIDValuesByReference() {
                record.setValue("firstName", UUID.fromString("d0fb6ab5-20e6-4823-8190-1ab9f6173d12"));
                record.setValue("lastName", UUID.fromString("01234567-9012-3456-7890-123456789012"));
                assertNotMatches("/firstName[. != /firstName]", record);
                assertMatches("/firstName[. != /lastName]", record);
            }
        }

        @Nested
        class Not {
            @Test
            public void invertsOperatorResults() {
                assertMatches("/name[not(. = 'other')]", record);
                assertNotMatches("/name[not(. = /name)]", record);
            }

            @Test
            public void invertsFilterResults() {
                assertMatches("/name[not(contains(., 'other'))]", record);
                assertNotMatches("/name[not(contains(., /name))]", record);
            }
        }

        @Nested
        class StartsWith {
            @Test
            public void supportsComparingWithStringLiteralValues() {
                assertMatches("/name[startsWith(., 'John D')]", record);
                assertNotMatches("/name[startsWith(., 'Jonn N')]", record);
            }

            @Test
            public void supportsComparingWithStringReference() {
                record.setValue("friends", new String[]{"John D", "John N"});
                assertMatches("/name[startsWith(., /friends[0])]", record);
                assertNotMatches("/name[startsWith(., /friends[1])]", record);
            }

            @Test
            public void matchesWhenTargetMatchesSearchValue() {
                record.setArrayValue("friends", 2, record.getValue("name"));
                assertMatches("/name[startsWith(., /friends[2])]", record);
            }

            @Test
            public void matchesWhenSearchValueIsEmpty() {
                assertMatches("/name[startsWith(., '')]", record);
            }
        }

        private static void assertMatches(final String path, final Record record) {
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(path, record);

            assertEquals(
                    1,
                    fieldValues.size(),
                    () -> "Expected \"" + path + "\" to match a single field on record " + record + " but got: " + fieldValues
            );
        }

        private static void assertNotMatches(final String path, final Record record) {
            final List<FieldValue> fieldValues = evaluateMultiFieldValue(path, record);

            assertEquals(
                    0,
                    fieldValues.size(),
                    () -> "Expected \"" + path + "\" to not match any fields on record " + record + " but got: " + fieldValues
            );
        }
    }

    private static <T> void assertFieldValue(
            final Record expectedParent,
            final String expectedFieldName,
            final T expectedValue,
            final FieldValue actualFieldValue
    ) {
        if (expectedParent == null) {
            assertFalse(actualFieldValue.getParent().isPresent());
        } else {
            assertEquals(expectedParent, actualFieldValue.getParentRecord().orElseThrow());
        }
        assertEquals(expectedFieldName, actualFieldValue.getField().getFieldName());
        assertEquals(expectedValue, actualFieldValue.getValue());
    }

    private static <T> void assertSingleFieldMultipleValueResult(
            final Record expectedParent,
            final String expectedFieldName,
            final T[] expectedValues,
            final List<FieldValue> fieldValues
    ) {
        assertAll(Stream.concat(
                Stream.of(() -> assertEquals(expectedValues.length, fieldValues.size())),
                IntStream.range(0, expectedValues.length).mapToObj(index ->
                        () -> assertFieldValue(expectedParent, expectedFieldName, expectedValues[index], fieldValues.get(index))
                )
        ));
    }

    private static RecordSchema getExampleSchema() {
        final DataType accountDataType = recordTypeOf(getAccountSchema());

        return recordSchemaOf(
                recordFieldOf("id", RecordFieldType.INT),
                recordFieldOf("firstName", RecordFieldType.STRING),
                recordFieldOf("lastName", RecordFieldType.STRING),
                recordFieldOf("name", RecordFieldType.STRING),
                recordFieldOf("missing", RecordFieldType.STRING),
                recordFieldOf("date", RecordFieldType.DATE),
                recordFieldOf("attributes", mapTypeOf(RecordFieldType.STRING)),
                recordFieldOf("mainAccount", recordTypeOf(getAccountSchema())),
                recordFieldOf("accounts", arrayTypeOf(accountDataType)),
                recordFieldOf("numbers", arrayTypeOf(RecordFieldType.INT)),
                recordFieldOf("friends", arrayTypeOf(RecordFieldType.STRING)),
                recordFieldOf("bytes", arrayTypeOf(RecordFieldType.BYTE))
        );
    }

    private static RecordSchema getAccountSchema() {
        return recordSchemaOf(
                recordFieldOf("id", RecordFieldType.INT),
                recordFieldOf("balance", RecordFieldType.DOUBLE),
                recordFieldOf("address", recordTypeOf(getAddressSchema()))
        );
    }

    private static RecordSchema getAddressSchema() {
        return recordSchemaOf(
                recordFieldOf("city", RecordFieldType.STRING),
                recordFieldOf("state", RecordFieldType.STRING)
        );
    }

    private static Record createExampleRecord() {
        final Map<String, Object> values = Map.ofEntries(
                entry("id", 48),
                entry("firstName", "John"),
                entry("lastName", "Doe"),
                entry("name", "John Doe"),
                // field "missing" is missing purposely
                entry("date", "2017-10-20T11:00:00Z"),
                entry("attributes", new HashMap<>(Map.of(
                        "city", "New York",
                        "state", "NY"
                ))),
                entry("mainAccount", createAccountRecord()),
                entry("accounts", new Record[]{
                        createAccountRecord(6, 10_000.00D, "Las Vegas", "Nevada"),
                        createAccountRecord(9, 48.02D, "Austin", "Texas")
                }),
                entry("numbers", new Integer[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
                entry("friends", new String[]{"John", "Jane", "Jacob", "Judy"}),
                entry("bytes", boxBytes("Hello World!".getBytes(StandardCharsets.UTF_8)))
        );

        return new MapRecord(getExampleSchema(), new HashMap<>(values));
    }

    private static Record createAccountRecord() {
        return createAccountRecord(1);
    }

    private static Record createAccountRecord(final int id) {
        return createAccountRecord(id, 123.45D);
    }

    private static Record createAccountRecord(final int id, final Double balance) {
        return createAccountRecord(id, balance, "Boston", "Massachusetts");
    }

    private static Record createAccountRecord(final int id, final Double balance, final String city, final String state) {
        return new MapRecord(getAccountSchema(), new HashMap<>(Map.of(
                "id", id,
                "balance", balance,
                "address", createAddressRecord(city, state)
        )));
    }

    private static Record createAddressRecord(String city, String state) {
        return new MapRecord(getAddressSchema(), new HashMap<>(Map.of(
                "city", city,
                "state", state
        )));
    }

    private static Record reduceRecord(final Record record, String... fieldsToRetain) {
        final RecordSchema schema = record.getSchema();

        final RecordField[] retainedFields = Arrays.stream(fieldsToRetain)
                .map(fieldName -> schema.getField(fieldName).orElseThrow())
                .toArray(RecordField[]::new);
        final RecordSchema reducedSchema = recordSchemaOf(retainedFields);

        return new MapRecord(reducedSchema, new HashMap<>(record.toMap()), false, true);
    }

    private static Record getAddressRecord(final Record parentRecord) {
        return parentRecord.getAsRecord("address", getAddressSchema());
    }

    private static FieldValue evaluateSingleFieldValue(final RecordPath path, final Record record, final FieldValue contextNode) {
        return path.evaluate(record, contextNode).getSelectedFields().findFirst().orElseThrow(AssertionError::new);
    }

    private static FieldValue evaluateSingleFieldValue(final String path, final Record record, final FieldValue contextNode) {
        return evaluateSingleFieldValue(RecordPath.compile(path), record, contextNode);
    }

    private static FieldValue evaluateSingleFieldValue(final RecordPath path, final Record record) {
        return evaluateSingleFieldValue(path, record, null);
    }

    private static FieldValue evaluateSingleFieldValue(final String path, final Record record) {
        return evaluateSingleFieldValue(path, record, null);
    }

    private static List<FieldValue> evaluateMultiFieldValue(final RecordPath path, final Record record, final FieldValue contextNode) {
        return path.evaluate(record, contextNode).getSelectedFields().toList();
    }

    private static List<FieldValue> evaluateMultiFieldValue(final String path, final Record record, final FieldValue contextNode) {
        return evaluateMultiFieldValue(RecordPath.compile(path), record, contextNode);
    }

    private static List<FieldValue> evaluateMultiFieldValue(final RecordPath path, final Record record) {
        return evaluateMultiFieldValue(path, record, null);
    }

    private static List<FieldValue> evaluateMultiFieldValue(final String path, final Record record) {
        return evaluateMultiFieldValue(path, record, null);
    }

    private static RecordSchema recordSchemaOf(RecordField... fields) {
        return new SimpleRecordSchema(Arrays.asList(fields));
    }

    private static RecordField recordFieldOf(final String fieldName, final DataType fieldType) {
        return new RecordField(fieldName, fieldType);
    }

    private static RecordField recordFieldOf(final String fieldName, final RecordFieldType fieldType) {
        return recordFieldOf(fieldName, fieldType.getDataType());
    }

    private static DataType mapTypeOf(final RecordFieldType fieldType) {
        return RecordFieldType.MAP.getMapDataType(fieldType.getDataType());
    }

    private static DataType arrayTypeOf(final DataType fieldType) {
        return RecordFieldType.ARRAY.getArrayDataType(fieldType);
    }

    private static DataType arrayTypeOf(final RecordFieldType fieldType) {
        return arrayTypeOf(fieldType.getDataType());
    }

    private static DataType choiceTypeOf(final Object... fieldTypes) {
        final List<DataType> typedFieldTypes = Arrays.stream(fieldTypes).map(rawFieldType -> {
            if (rawFieldType instanceof RecordFieldType recordFieldType) {
                return recordFieldType.getDataType();
            } else if (rawFieldType instanceof DataType dataType) {
                return dataType;
            }
            throw new IllegalArgumentException("fieldTypes passed to choiceTypeOf must be either RecordFieldType or DataType");
        }).toList();

        return RecordFieldType.CHOICE.getChoiceDataType(typedFieldTypes);
    }

    private static DataType recordTypeOf(final RecordSchema childSchema) {
        return RecordFieldType.RECORD.getRecordDataType(childSchema);
    }

    private static Byte[] boxBytes(final byte[] bytes) {
        Byte[] boxedBytes = new Byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            boxedBytes[i] = bytes[i];
        }
        return boxedBytes;
    }
}
