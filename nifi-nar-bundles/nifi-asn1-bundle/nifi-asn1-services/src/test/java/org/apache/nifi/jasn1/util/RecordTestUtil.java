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
package org.apache.nifi.jasn1.util;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.Tuple;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class RecordTestUtil {
    public static Object convertValue(Object value) {
        Object converted;
        if (value != null && value.getClass().isArray()) {
            converted = convertArray(value);
        } else if (value != null && value instanceof Record) {
            converted = convertRecord((Record) value);
        } else {
            converted = value;
        }
        return converted;
    }

    public static Object convertArray(Object value) {
        List<Object> converted = new ArrayList<>();

        int length = Array.getLength(value);
        for (int index = 0; index < length; index++) {
            Object itemValue = convertValue(Array.get(value, index));
            converted.add(itemValue);
        }

        return converted;
    }

    public static Map<String, Object> convertRecord(Record record) {
        Map<String, Object> converted = record.toMap().entrySet().stream()
                .map(nameAndValue -> {
                    Tuple<String, Object> result;

                    String name = nameAndValue.getKey();
                    Object value = nameAndValue.getValue();

                    Object convertedValue = convertValue(value);

                    result = new Tuple<>(name, convertedValue);

                    return result;
                })
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));


        return converted;
    }

    public static void assertRecordsEqual(Record record1, Record record2) {
        Map<String, Object> map1 = convertRecord(record1);
        Map<String, Object> map2 = convertRecord(record2);

        assertMapsEqual("ROOT", map1, map2);

        assertEquals(map1, map2);
    }

    public static void assertValuesEqual(String prefix, Object value1, Object value2) {
        if (value1 instanceof Map && value2 instanceof Map) {
            assertMapsEqual(prefix, (Map) value1, (Map) value2);
        } else if (value1 instanceof List && value2 instanceof List) {
            assertListsEqual(prefix, (List) value1, (List) value2);
            assertListsEqual(prefix, (List) value2, (List) value1);
        } else {
            assertEquals(prefix, value1, value2);
        }
    }

    public static void assertMapsEqual(String prefix, Map<String, Object> map1, Map<String, Object> map2) {
        map1.entrySet().stream().forEach(entry -> {
            String key = entry.getKey();
            Object value1 = entry.getValue();
            Object value2 = map2.get(key);

            assertValuesEqual(prefix + "." + key, value1, value2);
        });
        map2.entrySet().stream().forEach(entry -> {
            String key = entry.getKey();
            Object value1 = entry.getValue();
            Object value2 = map1.get(key);

            assertValuesEqual(prefix + "." + key, value2, value1);
        });
    }

    public static void assertListsEqual(String prefix, List list1, List list2) {
        int length1 = list1.size();
        int length2 = list2.size();

        if (length1 == length2) {
            for (int index = 0; index < length1; index++) {
                Object item1 = list1.get(index);
                Object item2 = list2.get(index);

                assertValuesEqual(prefix + "[" + index + "]", item1, item2);
            }
        } else {
            assertEquals(prefix, list1, list2);
        }
    }
}
