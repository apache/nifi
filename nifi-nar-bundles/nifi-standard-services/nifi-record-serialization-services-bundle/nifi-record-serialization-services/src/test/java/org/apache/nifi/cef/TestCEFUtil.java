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
package org.apache.nifi.cef;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCEFUtil {
    static final String RAW_FIELD = "raw";
    static final String RAW_VALUE = "Oct 12 04:16:11 localhost CEF:0|Company|Product|1.2.3|audit-login|Successful login|3|";

    static final String INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY = "src/test/resources/cef/single-row-header-fields-only.txt";
    static final String INPUT_SINGLE_ROW_WITH_EXTENSIONS = "src/test/resources/cef/single-row-with-extensions.txt";
    static final String INPUT_SINGLE_ROW_WITH_EMPTY_EXTENSION = "src/test/resources/cef/single-row-with-empty-extension.txt";
    static final String INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS = "src/test/resources/cef/single-row-with-custom-extensions.txt";
    static final String INPUT_SINGLE_ROW_WITH_EMPTY_CUSTOM_EXTENSIONS = "src/test/resources/cef/single-row-with-empty-custom-extensions.txt";
    static final String INPUT_SINGLE_ROW_WITH_INCORRECT_HEADER_FIELD = "src/test/resources/cef/single-row-with-incorrect-header-field.txt";
    static final String INPUT_SINGLE_ROW_WITH_INCORRECT_CUSTOM_EXTENSIONS = "src/test/resources/cef/single-row-with-incorrect-custom-extensions.txt";
    static final String INPUT_EMPTY_ROW = "src/test/resources/cef/empty-row.txt";
    static final String INPUT_MISFORMATTED_ROW = "src/test/resources/cef/misformatted-row.txt";
    static final String INPUT_MULTIPLE_IDENTICAL_ROWS = "src/test/resources/cef/multiple-rows.txt";
    static final String INPUT_MULTIPLE_ROWS_WITH_DIFFERENT_CUSTOM_TYPES = "src/test/resources/cef/multiple-rows-with-different-custom-types.txt";
    static final String INPUT_MULTIPLE_ROWS_STARTING_WITH_EMPTY_ROW = "src/test/resources/cef/multiple-rows-starting-with-empty-row.txt";
    static final String INPUT_MULTIPLE_ROWS_WITH_EMPTY_ROWS = "src/test/resources/cef/multiple-rows-with-empty-rows.txt";
    static final String INPUT_MULTIPLE_ROWS_WITH_DECREASING_NUMBER_OF_EXTENSIONS = "src/test/resources/cef/multiple-rows-decreasing-number-of-extensions.txt";
    static final String INPUT_MULTIPLE_ROWS_WITH_INCREASING_NUMBER_OF_EXTENSIONS = "src/test/resources/cef/multiple-rows-increasing-number-of-extensions.txt";

    static final Map<String, Object> EXPECTED_HEADER_VALUES = new HashMap<>();
    static final Map<String, Object> EXPECTED_EXTENSION_VALUES = new HashMap<>();

    static final String CUSTOM_EXTENSION_FIELD_NAME = "loginsequence";
    static final RecordField CUSTOM_EXTENSION_FIELD = new RecordField(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, RecordFieldType.INT.getDataType());
    static final RecordField CUSTOM_EXTENSION_FIELD_AS_STRING = new RecordField(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, RecordFieldType.STRING.getDataType());
    static final RecordField CUSTOM_EXTENSION_FIELD_AS_CHOICE = new RecordField(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, RecordFieldType.CHOICE.getChoiceDataType(
            RecordFieldType.FLOAT.getDataType(), RecordFieldType.STRING.getDataType()
    ));

    static {
        EXPECTED_HEADER_VALUES.put("version", 0);
        EXPECTED_HEADER_VALUES.put("deviceVendor", "Company");
        EXPECTED_HEADER_VALUES.put("deviceProduct", "Product");
        EXPECTED_HEADER_VALUES.put("deviceVersion", "1.2.3");
        EXPECTED_HEADER_VALUES.put("deviceEventClassId", "audit-login");
        EXPECTED_HEADER_VALUES.put("name", "Successful login");
        EXPECTED_HEADER_VALUES.put("severity", "3");

        EXPECTED_EXTENSION_VALUES.put("cn1Label", "userid");
        EXPECTED_EXTENSION_VALUES.put("spt", 46117);
        EXPECTED_EXTENSION_VALUES.put("cn1", 99999L);
        EXPECTED_EXTENSION_VALUES.put("cfp1", 1.23F);
        EXPECTED_EXTENSION_VALUES.put("dst", "127.0.0.1");
        EXPECTED_EXTENSION_VALUES.put("c6a1", "2345:425:2ca1:0:0:567:5673:23b5"); // Lower case representation is expected and values with leading zeroes shortened
        EXPECTED_EXTENSION_VALUES.put("dmac", "00:0d:60:af:1b:61"); // Lower case representation is expected

        EXPECTED_EXTENSION_VALUES.put("start", new Timestamp(1479152665000L));
        EXPECTED_EXTENSION_VALUES.put("end", Timestamp.valueOf("2017-01-12 12:23:45"));

        EXPECTED_EXTENSION_VALUES.put("dlat", 456.789D);
    }

    /**
     * The list of custom fields in the schema are expected to be generated based on the first event in the flow file. Because of this, it will not contain all the
     * possible extension fields. Also because of this, the following rows are handled based on this schema as well.
     */
    static List<RecordField> getFieldWithExtensions() {
        final List<RecordField> result = new ArrayList<>(CEFSchemaUtil.getHeaderFields());
        result.add(new RecordField("cn1Label", RecordFieldType.STRING.getDataType()));
        result.add(new RecordField("spt", RecordFieldType.INT.getDataType()));
        result.add(new RecordField("cn1", RecordFieldType.LONG.getDataType()));
        result.add(new RecordField("cfp1", RecordFieldType.FLOAT.getDataType()));
        result.add(new RecordField("dst", RecordFieldType.STRING.getDataType()));
        result.add(new RecordField("c6a1", RecordFieldType.STRING.getDataType()));
        result.add(new RecordField("dmac", RecordFieldType.STRING.getDataType()));
        result.add(new RecordField("start", RecordFieldType.TIMESTAMP.getDataType()));
        result.add(new RecordField("end", RecordFieldType.TIMESTAMP.getDataType()));
        result.add(new RecordField("dlat", RecordFieldType.DOUBLE.getDataType()));
        return result;
    }

    static List<RecordField> getFieldsWithCustomExtensions(RecordField... customExtensions) {
        final List<RecordField> result = TestCEFUtil.getFieldWithExtensions();

        Collections.addAll(result, customExtensions);

        return result;
    }

    /*
     * Record internally keeps the unknown fields even if they are marked for dropping. Thus direct comparison will not work as expected
     */
    static void assertFieldsAre(final Record record, final Map<String, Object> expectedValues) {
        assertEquals(expectedValues.size(), record.getValues().length);
        expectedValues.forEach((key, value) -> assertEquals(value, record.getValue(key), "Field " + key + " is incorrect"));
    }
}
