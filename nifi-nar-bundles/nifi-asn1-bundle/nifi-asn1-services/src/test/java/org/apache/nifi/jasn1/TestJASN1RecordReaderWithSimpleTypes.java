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
import org.junit.Test;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Depends on generated test classes
 */
public class TestJASN1RecordReaderWithSimpleTypes implements JASN1ReadRecordTester {
    @Test
    public void testBoolean() throws Exception {
        String dataFile = "target/boolean_wrapper.dat";

        BooleanWrapper berValue = new BooleanWrapper();
        berValue.setValue(new BerBoolean(true));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", true);
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.BOOLEAN.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testInteger() throws Exception {
        String dataFile = "target/integer_wrapper.dat";

        IntegerWrapper berValue = new IntegerWrapper();
        berValue.setValue(new BerInteger(4321234));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", BigInteger.valueOf(4321234));
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.BIGINT.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testBitString() throws Exception {
        String dataFile = "target/bit_string_wrapper.dat";

        BitStringWrapper berValue = new BitStringWrapper();
        berValue.setValue(new BerBitString(new boolean[]{false, true, false, false, true, true, true, true, false, true, false, false}));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", new boolean[]{false, true, false, false, true, true, true, true, false, true, false, false});
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BOOLEAN.getDataType())))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testOctetString() throws Exception {
        String dataFile = "target/octet_string_wrapper.dat";

        OctetStringWrapper berValue = new OctetStringWrapper();
        berValue.setValue(new BerOctetString("0123456789ABCDEFGHIJKLMNopqrstuvwxyz".getBytes()));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", octetStringExpectedValueConverter("0123456789ABCDEFGHIJKLMNopqrstuvwxyz".getBytes()));
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.STRING.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testUTF8StringString() throws Exception {
        String dataFile = "target/utf8string_wrapper.dat";

        UTF8StringWrapper berValue = new UTF8StringWrapper();
        berValue.setValue(new BerUTF8String("Some UTF-8 String. こんにちは世界。"));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", "Some UTF-8 String. こんにちは世界。");
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.STRING.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testBMPString() throws Exception {
        String dataFile = "target/bmpstring_wrapper.dat";

        BMPStringWrapper berValue = new BMPStringWrapper();
        berValue.setValue(new BerBMPString("Some UTF-8 String. こんにちは世界。".getBytes()));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", "Some UTF-8 String. こんにちは世界。");
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.STRING.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testDate() throws Exception {
        String dataFile = "target/date_wrapper.dat";

        DateWrapper berValue = new DateWrapper();
        berValue.setValue(new BerDate("2019-10-16"));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", LocalDate.parse("2019-10-16"));
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.DATE.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testDateInvalidValue() throws Exception {
        String dataFile = "target/date_invalid_wrapper.dat";

        DateWrapper berValue = new DateWrapper();
        berValue.setValue(new BerDate("2019_10-16"));

        try {
            testReadRecord(dataFile, berValue, (Map) null, null);
            fail();
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Text '2019_10-16' could not be parsed at index 4"));
        }
    }

    @Test
    public void testTimeOfDay() throws Exception {
        String dataFile = "target/time_of_day_wrapper.dat";

        TimeOfDayWrapper berValue = new TimeOfDayWrapper();
        berValue.setValue(new BerTimeOfDay("16:13:12"));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", LocalTime.parse("16:13:12"));
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.TIME.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testTimeOfDayInvalidValue() throws Exception {
        String dataFile = "target/time_of_day_invalid_wrapper.dat";

        TimeOfDayWrapper berValue = new TimeOfDayWrapper();
        berValue.setValue(new BerTimeOfDay("16.13:12"));

        try {
            testReadRecord(dataFile, berValue, (Map) null, null);
            fail();
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Text '16.13:12' could not be parsed at index 2"));
        }
    }

    @Test
    public void testDateTime() throws Exception {
        String dataFile = "target/date_time_wrapper.dat";

        DateTimeWrapper berValue = new DateTimeWrapper();
        berValue.setValue(new BerDateTime("2019-10-16T16:18:20"));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", LocalDateTime.parse("2019-10-16T16:18:20"));
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.TIMESTAMP.getDataType("yyyy-MM-ddTHH:mm:ss")))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testDateTimeInvalid() throws Exception {
        String dataFile = "target/date_time_invalid_wrapper.dat";

        DateTimeWrapper berValue = new DateTimeWrapper();
        berValue.setValue(new BerDateTime("2019-10-16 16:18:20"));

        try {
            testReadRecord(dataFile, berValue, (Map) null, null);
            fail();
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Text '2019-10-16 16:18:20' could not be parsed at index 10"));
        }
    }

    @Test
    public void testReal() throws Exception {
        String dataFile = "target/real_wrapper.dat";

        RealWrapper berValue = new RealWrapper();
        berValue.setValue(new BerReal(176.34D));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", 176.34D);
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.DOUBLE.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }

    @Test
    public void testEnumerated() throws Exception {
        String dataFile = "target/enumerated_wrapper.dat";

        EnumeratedWrapper berValue = new EnumeratedWrapper();
        berValue.setValue(new BerEnum(0));

        Map<String, Object> expectedValues = new HashMap<String, Object>() {{
            put("value", 0);
        }};

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("value", RecordFieldType.INT.getDataType()))
        );

        testReadRecord(dataFile, berValue, expectedValues, expectedSchema);
    }
}
