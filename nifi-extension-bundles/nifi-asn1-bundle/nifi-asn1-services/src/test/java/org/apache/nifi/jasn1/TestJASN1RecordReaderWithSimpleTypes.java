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

import com.beanit.asn1bean.ber.types.BerBitString;
import com.beanit.asn1bean.ber.types.BerBoolean;
import com.beanit.asn1bean.ber.types.BerDate;
import com.beanit.asn1bean.ber.types.BerDateTime;
import com.beanit.asn1bean.ber.types.BerEnum;
import com.beanit.asn1bean.ber.types.BerInteger;
import com.beanit.asn1bean.ber.types.BerOctetString;
import com.beanit.asn1bean.ber.types.BerReal;
import com.beanit.asn1bean.ber.types.BerTimeOfDay;
import com.beanit.asn1bean.ber.types.string.BerBMPString;
import com.beanit.asn1bean.ber.types.string.BerUTF8String;
import org.apache.nifi.jasn1.simple.BMPStringWrapper;
import org.apache.nifi.jasn1.simple.BitStringWrapper;
import org.apache.nifi.jasn1.simple.BooleanWrapper;
import org.apache.nifi.jasn1.simple.DateTimeWrapper;
import org.apache.nifi.jasn1.simple.DateWrapper;
import org.apache.nifi.jasn1.simple.EnumeratedWrapper;
import org.apache.nifi.jasn1.simple.IntegerWrapper;
import org.apache.nifi.jasn1.simple.OctetStringWrapper;
import org.apache.nifi.jasn1.simple.RealWrapper;
import org.apache.nifi.jasn1.simple.TimeOfDayWrapper;
import org.apache.nifi.jasn1.simple.UTF8StringWrapper;
import org.apache.nifi.jasn1.util.JASN1ReadRecordTester;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Depends on generated test classes
 */
public class TestJASN1RecordReaderWithSimpleTypes implements JASN1ReadRecordTester {
    @Test
    public void testBoolean() throws Exception {
        final String dataFile = "target/boolean_wrapper.dat";

        final BooleanWrapper berValue = new BooleanWrapper();
        berValue.setValue(new BerBoolean(true));

        final Map<String, Object> expectedValues = Map.of("value", true);

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.BOOLEAN.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testInteger() throws Exception {
        final String dataFile = "target/integer_wrapper.dat";

        final IntegerWrapper berValue = new IntegerWrapper();
        berValue.setValue(new BerInteger(4321234));

        final Map<String, Object> expectedValues = Map.of("value", BigInteger.valueOf(4321234));

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.BIGINT.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testBitString() throws Exception {
        final String dataFile = "target/bit_string_wrapper.dat";

        final BitStringWrapper berValue = new BitStringWrapper();
        berValue.setValue(new BerBitString(new boolean[]{false, true, false, false, true, true, true, true, false, true, false, false}));

        final Map<String, Object> expectedValues = Map.of("value", "010011110100");

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.STRING.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testOctetString() throws Exception {
        final String dataFile = "target/octet_string_wrapper.dat";

        final OctetStringWrapper berValue = new OctetStringWrapper();
        berValue.setValue(new BerOctetString("0123456789ABCDEFGHIJKLMNopqrstuvwxyz".getBytes()));

        final Map<String, Object> expectedValues =
                Map.of("value", octetStringExpectedValueConverter("0123456789ABCDEFGHIJKLMNopqrstuvwxyz".getBytes()));

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.STRING.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testUTF8StringString() throws Exception {
        final String dataFile = "target/utf8string_wrapper.dat";

        final UTF8StringWrapper berValue = new UTF8StringWrapper();
        berValue.setValue(new BerUTF8String("Some UTF-8 String. こんにちは世界。"));

        final Map<String, Object> expectedValues = Map.of("value", "Some UTF-8 String. こんにちは世界。");

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.STRING.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testBMPString() throws Exception {
        final String dataFile = "target/bmpstring_wrapper.dat";

        final BMPStringWrapper berValue = new BMPStringWrapper();
        berValue.setValue(new BerBMPString("Some UTF-8 String. こんにちは世界。".getBytes(StandardCharsets.UTF_8)));

        final Map<String, Object> expectedValues = Map.of("value", "Some UTF-8 String. こんにちは世界。");

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.STRING.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testDate() throws Exception {
        final String dataFile = "target/date_wrapper.dat";

        final DateWrapper berValue = new DateWrapper();
        berValue.setValue(new BerDate("2019-10-16"));

        final Map<String, Object> expectedValues = Map.of("value", LocalDate.parse("2019-10-16"));

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.DATE.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testDateInvalidValue() throws Exception {
        final String dataFile = "target/date_invalid_wrapper.dat";

        final DateWrapper berValue = new DateWrapper();
        berValue.setValue(new BerDate("2019_10-16"));

        try {
            testReadRecord(dataFile, berValue, (Map) null, null);
            fail();
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("Text '2019_10-16' could not be parsed at index 4"));
        }
    }

    @Test
    public void testTimeOfDay() throws Exception {
        final String dataFile = "target/time_of_day_wrapper.dat";

        final TimeOfDayWrapper berValue = new TimeOfDayWrapper();
        berValue.setValue(new BerTimeOfDay("16:13:12"));

        final Map<String, Object> expectedValues = Map.of("value", LocalTime.parse("16:13:12"));

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.TIME.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testTimeOfDayInvalidValue() {
        final String dataFile = "target/time_of_day_invalid_wrapper.dat";

        final TimeOfDayWrapper berValue = new TimeOfDayWrapper();
        berValue.setValue(new BerTimeOfDay("16.13:12"));

        try {
            testReadRecord(dataFile, berValue, (Map) null, null);
            fail();
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("Text '16.13:12' could not be parsed at index 2"));
        }
    }

    @Test
    public void testDateTime() throws Exception {
        final String dataFile = "target/date_time_wrapper.dat";

        final DateTimeWrapper berValue = new DateTimeWrapper();
        berValue.setValue(new BerDateTime("2019-10-16T16:18:20"));

        final Map<String, Object> expectedValues = Map.of("value", LocalDateTime.parse("2019-10-16T16:18:20"));

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.TIMESTAMP.getDataType("yyyy-MM-ddTHH:mm:ss")))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testDateTimeInvalid() {
        final String dataFile = "target/date_time_invalid_wrapper.dat";

        final DateTimeWrapper berValue = new DateTimeWrapper();
        berValue.setValue(new BerDateTime("2019-10-16 16:18:20"));

        try {
            testReadRecord(dataFile, berValue, (Map) null, null);
            fail();
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("Text '2019-10-16 16:18:20' could not be parsed at index 10"));
        }
    }

    @Test
    public void testReal() throws Exception {
        final String dataFile = "target/real_wrapper.dat";

        final RealWrapper berValue = new RealWrapper();
        berValue.setValue(new BerReal(176.34D));

        final Map<String, Object> expectedValues = Map.of("value", 176.34D);

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.DOUBLE.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testEnumerated() throws Exception {
        final String dataFile = "target/enumerated_wrapper.dat";

        final EnumeratedWrapper berValue = new EnumeratedWrapper();
        berValue.setValue(new BerEnum(0));

        final Map<String, Object> expectedValues = Map.of("value", 0);

        final RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.INT.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }
}
