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

import org.apache.nifi.processors.box.utils.BoxMetadataUtils;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BoxParseJsonTest {

    @Test
    void testParseString() {
        String expected = "test string";
        Object result = BoxMetadataUtils.parseValue(expected);
        assertEquals(expected, result);

        // Empty string
        expected = "";
        result = BoxMetadataUtils.parseValue(expected);
        assertEquals(expected, result);
    }

    @Test
    void testParseBoolean() {
        // Test true
        boolean expected = true;
        Object result = BoxMetadataUtils.parseValue(expected);
        assertEquals(expected, result);

        // Test false
        expected = false;
        result = BoxMetadataUtils.parseValue(expected);
        assertEquals(expected, result);
    }

    @Test
    void testParseIntegerNumber() {
        // Integer value
        long expected = 42;
        Object result = BoxMetadataUtils.parseValue(expected);
        assertEquals(expected, result);

        // Max long value
        expected = Long.MAX_VALUE;
        result = BoxMetadataUtils.parseValue(expected);
        assertEquals(expected, result);

        // Min long value
        expected = Long.MIN_VALUE;
        result = BoxMetadataUtils.parseValue(expected);
        assertEquals(expected, result);
    }

    @Test
    void testParseDecimalNumber() {
        // Double without exponent
        Double input = 3.14159;
        Object result = BoxMetadataUtils.parseValue(input);
        assertEquals("3.14159", result);

        // Very small number - toPlainString() will expand the decimal
        input = 0.0000000001;
        result = BoxMetadataUtils.parseValue(input);
        assertEquals("0.00000000010", result);

        // Very large number that should be preserved
        input = 9999999999999999.9999;
        result = BoxMetadataUtils.parseValue(input);
        // Note: doubles have precision limits
        assertEquals("10000000000000000", result);
    }

    @Test
    void testParseExponentialNumber() {
        // Scientific notation is converted to plain string format
        Double input = 1.234e5;
        Object result = BoxMetadataUtils.parseValue(input);
        // Note: Double 1.234e5 = 123400.0 when converted to BigDecimal
        assertEquals("123400.0", result);

        // large exponent
        input = 1.234e20;
        result = BoxMetadataUtils.parseValue(input);
        assertEquals("123400000000000000000", result);

        // Negative exponent
        input = 1.234e-5;
        result = BoxMetadataUtils.parseValue(input);
        assertEquals("0.00001234", result);
    }

    @Test
    void testParseNull() {
        Object result = BoxMetadataUtils.parseValue(null);
        assertEquals(null, result);
    }

    @Test
    void testParseObjectAndArray() {
        // Collections return their string representation
        List<String> list = List.of("item1", "item2");
        Object result = BoxMetadataUtils.parseValue(list);
        assertEquals(list.toString(), result);

        // Maps return their string representation
        Map<String, String> map = Map.of("key", "value");
        result = BoxMetadataUtils.parseValue(map);
        assertEquals(map.toString(), result);
    }
}
