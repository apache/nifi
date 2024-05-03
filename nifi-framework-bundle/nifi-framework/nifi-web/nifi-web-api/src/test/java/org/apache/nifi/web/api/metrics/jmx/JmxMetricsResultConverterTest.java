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
package org.apache.nifi.web.api.metrics.jmx;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JmxMetricsResultConverterTest {
    private static final String COMPOSITE_DATA_KEY = "CompositeData%s";
    private static final String TABLE_DATA_KEY = "Test String %s";
    private static final String CONVERTED_TABLE_DATA_KEY = "[Test String %s]";
    private static final String ATTRIBUTE_NAME_STRING = "string";
    private static final String ATTRIBUTE_NAME_INT = "int";
    private static final String ATTRIBUTE_NAME_BOOLEAN = "boolean";
    private static final String TABLE_DESCRIPTION = "Table for all tests";
    private static final String TABLE_NAME = "Test table";
    private static final String METRIC_TYPE_DESCRIPTION = "Metric type for testing";
    private static final String METRIC_TYPE_NAME = "Metric type";
    private static JmxMetricsResultConverter metricsResultConverter;
    private static CompositeData compositeDataOne;
    private static CompositeData compositeDataTwo;
    private static CompositeData compositeDataThree;
    private static TabularType tableType;
    @BeforeAll
    public static void init() throws OpenDataException {
        metricsResultConverter = new JmxMetricsResultConverter();

        final CompositeType compositeType = new CompositeType(METRIC_TYPE_NAME,
                METRIC_TYPE_DESCRIPTION,
                new String[]{ATTRIBUTE_NAME_STRING, ATTRIBUTE_NAME_INT, ATTRIBUTE_NAME_BOOLEAN},
                new String[]{ATTRIBUTE_NAME_STRING, ATTRIBUTE_NAME_INT, ATTRIBUTE_NAME_BOOLEAN},
                new OpenType[]{SimpleType.STRING, SimpleType.INTEGER, SimpleType.BOOLEAN});

        tableType = new TabularType(TABLE_NAME,
                TABLE_DESCRIPTION,
                compositeType,
                new String[] {ATTRIBUTE_NAME_STRING});

        compositeDataOne = new CompositeDataSupport(compositeType,
                new String[] {ATTRIBUTE_NAME_STRING, ATTRIBUTE_NAME_INT, ATTRIBUTE_NAME_BOOLEAN},
                new Object[] {"Test String 1", 1, Boolean.FALSE}
        );

        compositeDataTwo = new CompositeDataSupport(compositeType,
                new String[] {ATTRIBUTE_NAME_STRING, ATTRIBUTE_NAME_INT, ATTRIBUTE_NAME_BOOLEAN},
                new Object[] {"Test String 2", 2, Boolean.TRUE}
        );

        compositeDataThree = new CompositeDataSupport(compositeType,
                new String[] {ATTRIBUTE_NAME_STRING, ATTRIBUTE_NAME_INT, ATTRIBUTE_NAME_BOOLEAN},
                new Object[] {"Test String 3", 3, Boolean.FALSE}
        );
    }

    @Test
    public void testSimpleTypeKeptOriginalType() {
        final String expectedString = "Test String";
        final int expectedInt = 1;
        final boolean expectedBoolean = Boolean.TRUE;

        final Object actualString = metricsResultConverter.convert(expectedString);
        final Object actualInt = metricsResultConverter.convert(expectedInt);
        final Object actualBoolean = metricsResultConverter.convert(expectedBoolean);

        assertEquals(expectedString, actualString);
        assertEquals(expectedInt, actualInt);
        assertEquals(expectedBoolean, actualBoolean);
        assertEquals(SimpleType.STRING.getTypeName(), actualString.getClass().getName());
        assertEquals(SimpleType.INTEGER.getTypeName(), actualInt.getClass().getName());
        assertEquals(SimpleType.BOOLEAN.getTypeName(), actualBoolean.getClass().getName());
    }

    @Test
    public void testCompositeDataConvertedToMap() {
        //CompositeData consists of a compositeType and a content which is a Map of attribute name and value pairs.
        //The content will be concerted to a LinkedHashMap.
        final CompositeData expected = compositeDataOne;

        final Map<String, Object> actual = castToMap(metricsResultConverter.convert(expected));

        assertEquals(expected.get(ATTRIBUTE_NAME_STRING), actual.get(ATTRIBUTE_NAME_STRING));
        assertEquals(expected.get(ATTRIBUTE_NAME_STRING).getClass().getName(), actual.get(ATTRIBUTE_NAME_STRING).getClass().getName());
        assertEquals(expected.get(ATTRIBUTE_NAME_INT), actual.get(ATTRIBUTE_NAME_INT));
        assertEquals(expected.get(ATTRIBUTE_NAME_INT).getClass().getName(), actual.get(ATTRIBUTE_NAME_INT).getClass().getName());
        assertEquals(expected.get(ATTRIBUTE_NAME_BOOLEAN), actual.get(ATTRIBUTE_NAME_BOOLEAN));
        assertEquals(expected.get(ATTRIBUTE_NAME_BOOLEAN).getClass().getName(), actual.get(ATTRIBUTE_NAME_BOOLEAN).getClass().getName());
    }

    @Test
    public void testCompositeDataListConvertedToMaps() {
        //A CompositeData array consists of a collection of CompositeData objects.
        //The content will be concerted to a LinkedHashMap where the key is 'CompositeData<array_index>'
        // and the value is a LinkedHashmap of the CompositeData content.
        final CompositeData[] expected = new CompositeData[] {
                    compositeDataOne,
                    compositeDataTwo,
                    compositeDataThree
                };

        final Map<String, Object> actual = castToMap(metricsResultConverter.convert(expected));

        assertTrue(expected[0].values().containsAll(castToMap(actual.get(String.format(COMPOSITE_DATA_KEY, 0))).values()));
        assertTrue(expected[1].values().containsAll(castToMap(actual.get(String.format(COMPOSITE_DATA_KEY, 1))).values()));
        assertTrue(expected[2].values().containsAll(castToMap(actual.get(String.format(COMPOSITE_DATA_KEY, 2))).values()));
        assertEquals(expected.length, actual.size());
    }

    @Test
    public void testTabularDataConvertedToMaps() {
        //A TabularData consists of a Collection where key is a String array of attribute names and value is a CompositeData.
        //The content will be concerted to a LinkedHashMap where the key is String output of the Tabular key array
        // and the value is a LinkedHashmap of the CompositeData content.
        final TabularData expected = new TabularDataSupport(tableType);
        expected.put(compositeDataOne);
        expected.put(compositeDataTwo);
        expected.put(compositeDataThree);

        final Map<String, Object> actual = castToMap(metricsResultConverter.convert(expected));

        assertTrue(expected.get(new String[]{String.format(TABLE_DATA_KEY, 1)}).values().containsAll(castToMap(actual.get(String.format(CONVERTED_TABLE_DATA_KEY, 1))).values()));
        assertTrue(expected.get(new String[]{String.format(TABLE_DATA_KEY, 2)}).values().containsAll(castToMap(actual.get(String.format(CONVERTED_TABLE_DATA_KEY, 2))).values()));
        assertTrue(expected.get(new String[]{String.format(TABLE_DATA_KEY, 3)}).values().containsAll(castToMap(actual.get(String.format(CONVERTED_TABLE_DATA_KEY, 3))).values()));
        assertEquals(expected.values().size(), actual.size());
    }

    private Map<String, Object> castToMap(final Object object) {
        return (Map<String, Object>) object;
    }

}