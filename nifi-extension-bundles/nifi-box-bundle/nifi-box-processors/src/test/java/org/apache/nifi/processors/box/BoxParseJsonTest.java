/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.box;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import org.apache.nifi.processors.box.utils.BoxMetadataUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BoxParseJsonTest {

    @Test
    void testParseString() {
        String expected = "test string";
        Object result = BoxMetadataUtils.parseJsonValue(Json.value(expected));
        assertEquals(expected, result);

        // Empty string
        expected = "";
        result = BoxMetadataUtils.parseJsonValue(Json.value(expected));
        assertEquals(expected, result);
    }

    @Test
    void testParseBoolean() {
        // Test true
        boolean expected = true;
        Object result = BoxMetadataUtils.parseJsonValue(Json.value(expected));
        assertEquals(expected, result);

        // Test false
        expected = false;
        result = BoxMetadataUtils.parseJsonValue(Json.value(expected));
        assertEquals(expected, result);
    }

    @Test
    void testParseIntegerNumber() {
        // Integer value
        long expected = 42;
        Object result = BoxMetadataUtils.parseJsonValue(Json.value(expected));
        assertEquals(expected, result);

        // Max long value
        expected = Long.MAX_VALUE;
        result = BoxMetadataUtils.parseJsonValue(Json.value(expected));
        assertEquals(expected, result);

        // Min long value
        expected = Long.MIN_VALUE;
        result = BoxMetadataUtils.parseJsonValue(Json.value(expected));
        assertEquals(expected, result);
    }

    @Test
    void testParseDecimalNumber() {
        // Double without exponent
        String input = "3.14159";
        JsonValue jsonValue = Json.parse(input);
        Object result = BoxMetadataUtils.parseJsonValue(jsonValue);
        assertEquals(input, result);

        // Very small number that should be preserved
        input = "0.0000000001";
        jsonValue = Json.parse(input);
        result = BoxMetadataUtils.parseJsonValue(jsonValue);
        assertEquals(input, result);

        // Very large number that should be preserved
        input = "9999999999999999.9999";
        jsonValue = Json.parse(input);
        result = BoxMetadataUtils.parseJsonValue(jsonValue);
        assertEquals(input, result);
    }

    @Test
    void testParseExponentialNumber() {
        // Scientific notation is converted to plain string format
        String input = "1.234e5";
        JsonValue jsonValue = Json.parse(input);
        Object result = BoxMetadataUtils.parseJsonValue(jsonValue);
        assertEquals("123400", result);

        // large exponent
        input = "1.234e20";
        jsonValue = Json.parse(input);
        result = BoxMetadataUtils.parseJsonValue(jsonValue);
        assertEquals("123400000000000000000", result);

        // Negative exponent
        input = "1.234e-5";
        jsonValue = Json.parse(input);
        result = BoxMetadataUtils.parseJsonValue(jsonValue);
        assertEquals("0.00001234", result);
    }

    @Test
    void testParseObjectAndArray() {
        // JSON objects return their string representation
        JsonObject jsonObject = Json.object().add("key", "value");
        Object result = BoxMetadataUtils.parseJsonValue(jsonObject);
        assertEquals(jsonObject.toString(), result);

        // JSON arrays return their string representation
        JsonArray jsonArray = Json.array().add("item1").add("item2");
        result = BoxMetadataUtils.parseJsonValue(jsonArray);
        assertEquals(jsonArray.toString(), result);
    }

    @Test
    void testParseNumberFormatException() {
        String largeIntegerString = "9999999999999999999"; // Beyond Long.MAX_VALUE
        JsonValue jsonValue = Json.parse(largeIntegerString);
        Object result = BoxMetadataUtils.parseJsonValue(jsonValue);
        assertEquals(largeIntegerString, result);

        double doubleValue = 123.456;
        result = BoxMetadataUtils.parseJsonValue(Json.value(doubleValue));
        assertEquals(String.valueOf(doubleValue), result);
    }
}
