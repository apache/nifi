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
package org.apache.nifi.web.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.metrics.jmx.JmxMetricsResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestJmxMetricsResource {
    private static final String JMX_METRICS_NIFI_PROPERTY = "nifi.jmx.metrics.blacklisting.filter";
    private static final String TEST_BEAN_NAME_ONE = "testBean1";
    private static final String TEST_BEAN_NAME_TWO = "testBean2";
    private static final String OBJECT_NAME_PREFIX = "org.apache.nifi.web.api:type=test,name=%s";
    private static final String INVALID_REGEX = "(";
    private static final String NAME_KEY = "Name";
    private static final String DATA_KEY = "Data";
    private static final String DATA_LIST_KEY = "DataList";
    private static final String TABLE_KEY = "Table";
    private static final String ATTRIBUTE_NAME_STRING = "string";
    private static final String ATTRIBUTE_NAME_INT = "int";
    private static final String ATTRIBUTE_NAME_BOOLEAN = "boolean";
    private static final String TEST_TABULAR_STRING_VALUE = "Test tabular string";
    private static final String STRING_VALUE = "Test string";
    private static final String BEAN_NAME_FILTER = "%s; %s";
    private static final String STRING_ATTRIBUTE_VALUE = "%s%s";
    private static final String COMPOSITE_DATA_KEY = "CompositeData%s";
    private static final String TABULAR_DATA_KEY = "[%s%s]";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @InjectMocks
    private final static JmxMetricsResource resource = new JmxMetricsResource();

    @Mock
    private NiFiServiceFacade serviceFacade;

    private static MBeanServer mBeanServer;
    private static ObjectName objectNameForTestBeanOne;
    private static ObjectName objectNameForTestBeanTwo;

    @BeforeAll
    public static void init() throws MalformedObjectNameException, OpenDataException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException, IOException {
            objectNameForTestBeanOne = new ObjectName(String.format(OBJECT_NAME_PREFIX, TEST_BEAN_NAME_ONE));
            objectNameForTestBeanTwo = new ObjectName(String.format(OBJECT_NAME_PREFIX, TEST_BEAN_NAME_TWO));
            mBeanServer = ManagementFactory.getPlatformMBeanServer();
            mBeanServer.registerMBean(new Metric(TEST_BEAN_NAME_ONE), objectNameForTestBeanOne);
            mBeanServer.registerMBean(new Metric(TEST_BEAN_NAME_TWO), objectNameForTestBeanTwo);
    }

    @AfterAll
    public static void tearDown() throws InstanceNotFoundException, MBeanRegistrationException {
        mBeanServer.unregisterMBean(objectNameForTestBeanOne);
        mBeanServer.unregisterMBean(objectNameForTestBeanTwo);
    }

    @Test
    public void testNotProvidingFiltersReturnsAllMBeans() throws IOException {
        Set<String> names = getFilteringResult("", "");
        assertTrue(names.size() > 2);
        assertTrue(names.containsAll(Arrays.asList(TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_TWO)));
    }

    @Test
    public void testBlackListingFiltersRemovesMBeanFromResult() throws IOException {
        Set<String> names = getFilteringResult(TEST_BEAN_NAME_ONE, "");
        assertTrue(names.size() > 2);
        assertFalse(names.contains(TEST_BEAN_NAME_ONE));
        assertTrue(names.contains(TEST_BEAN_NAME_TWO));
    }

    @Test
    public void testBeanNameFiltersReturnsTheSpecifiedMBeansOnly() throws IOException {
        Set<String> names = getFilteringResult("", String.format(BEAN_NAME_FILTER, TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_TWO));
        assertEquals(names.size(), 2);
        assertTrue(names.containsAll(Arrays.asList(TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_TWO)));
    }

    @Test
    public void testInvalidBlackListFilteringRevertingBackToDefaultFiltering() throws IOException {
        Set<String> names = getFilteringResult(INVALID_REGEX, "");
        assertTrue(names.size() > 2);
        assertTrue(names.containsAll(Arrays.asList(TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_TWO)));
    }

    @Test
    public void testInvalidBeanNameFilteringRevertingBackToDefaultFiltering() throws IOException {
        Set<String> names = getFilteringResult("", INVALID_REGEX);
        assertTrue(names.size() > 2);
        assertTrue(names.containsAll(Arrays.asList(TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_TWO)));
    }

    @Test
    public void testInvalidFiltersRevertingBackToDefaultFiltering() throws IOException {
        Set<String> names = getFilteringResult(INVALID_REGEX, INVALID_REGEX);
        assertTrue(names.size() > 2);
        assertTrue(names.containsAll(Arrays.asList(TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_TWO)));
    }

    @Test
    public void testBlackListingFilterHasPriority() throws IOException {
        Set<String> names = getFilteringResult(TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_ONE);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testBlackListingFilterHasPriorityWhenTheSameFiltersApplied() throws IOException {
        Set<String> names = getFilteringResult(TEST_BEAN_NAME_ONE, String.format(BEAN_NAME_FILTER, TEST_BEAN_NAME_ONE, TEST_BEAN_NAME_TWO));

        assertEquals(names.size(), 1);
        assertFalse(names.contains(TEST_BEAN_NAME_ONE));
        assertTrue(names.contains(TEST_BEAN_NAME_TWO));
    }

    @Test
    public void testSimpleTypeKeptOriginalType() throws IOException {
        final Map<String, Object> resultMap = getDataResult();

        assertEquals(TEST_BEAN_NAME_ONE, resultMap.get(NAME_KEY));
        assertEquals(SimpleType.STRING.getTypeName(), resultMap.get(NAME_KEY).getClass().getName());
    }

    @Test
    public void testCompositeDataConvertedToMap() throws IOException {
        final Map<String, Object> expectedResult = createCompositeDataResult();

        final Map<String, Object> resultMap = getDataResult();

        assertEquals(expectedResult, resultMap.get(DATA_KEY));
        assertEquals(SimpleType.BOOLEAN.getTypeName(), castToMap(resultMap.get(DATA_KEY)).get(ATTRIBUTE_NAME_BOOLEAN).getClass().getName());
        assertEquals(SimpleType.STRING.getTypeName(), castToMap(resultMap.get(DATA_KEY)).get(ATTRIBUTE_NAME_STRING).getClass().getName());
        assertEquals(SimpleType.INTEGER.getTypeName(), castToMap(resultMap.get(DATA_KEY)).get(ATTRIBUTE_NAME_INT).getClass().getName());
    }

    @Test
    public void testCompositeDataListConvertedToListOfMaps() throws IOException {
        final Map<String, Object> expectedResult = createCompositeDataListResult();

        final Map<String, Object> resultMap = getDataResult();

        assertTrue(expectedResult.values().containsAll(castToMap(resultMap.get(DATA_LIST_KEY)).values()));
        assertEquals(expectedResult.values().size(), castToMap(resultMap.get(DATA_LIST_KEY)).values().size());
    }

    @Test
    public void testTabularDataConvertedToListOfMaps() throws IOException {
        final Map<String, Object> expectedResult = createTabularDataResult();

        final Map<String, Object> resultMap = getDataResult();

        assertTrue(expectedResult.values().containsAll(castToMap(resultMap.get(TABLE_KEY)).values()));
        assertEquals(expectedResult.values().size(), castToMap(resultMap.get(TABLE_KEY)).values().size());
    }

    private Set<String> getFilteringResult(final String blackListingFilter, final String beanNameFilter) throws IOException {
        final List<JmxMetricsResult> resultList = getResult(blackListingFilter, beanNameFilter);
        final Set<String> names = new HashSet<>();
        for (JmxMetricsResult result : resultList) {
            if (result.getAttributeName().equals(NAME_KEY)) {
                names.add(result.getAttributeValue().toString());
            }
        }
        return names;
    }

    private Map<String, Object> castToMap(final Object object) {
        return (Map<String, Object>) object;
    }

    private Map<String, Object> getDataResult() throws IOException {
        final List<JmxMetricsResult> resultList = getResult("", TEST_BEAN_NAME_ONE);
        final Map<String, Object> resultMap = new HashMap<>();
        for (JmxMetricsResult result : resultList) {
            resultMap.put(result.getAttributeName(), result.getAttributeValue());
        }
        return resultMap;
    }

    private List<JmxMetricsResult> getResult(final String blackListingFilter, final String beanNameFilter) throws IOException {
        resource.setProperties(new NiFiProperties(Collections.singletonMap(JMX_METRICS_NIFI_PROPERTY, blackListingFilter)));
        final ByteArrayOutputStream o = new ByteArrayOutputStream();
        ((StreamingOutput) resource.getJmxMetrics(beanNameFilter).getEntity()).write(o);

        return MAPPER.readValue(o.toByteArray(), new TypeReference<List<JmxMetricsResult>>() {});
    }

    private Map<String, Object> createCompositeDataResult() {
        return createCompositeDataResult(false, STRING_VALUE, 0);
    }

    private Map<String, Object> createCompositeDataResult(final boolean booleanValue, final String stringValue, final int intValue) {
        final Map<String, Object> expectedResult = new HashMap<>();
        expectedResult.put(ATTRIBUTE_NAME_BOOLEAN, booleanValue);
        expectedResult.put(ATTRIBUTE_NAME_STRING, stringValue);
        expectedResult.put(ATTRIBUTE_NAME_INT, intValue);
        return expectedResult;
    }

    private Map<String, Object> createCompositeDataListResult() {
        final Map<String, Object> expectedResult = new HashMap<>();
        for (int i = 1; i < 4; i++) {
            final Map<String, Object> dataRepresentation = createCompositeDataResult(i % 2 != 0, String.format(STRING_ATTRIBUTE_VALUE, STRING_VALUE, i), i);
            expectedResult.put(String.format(COMPOSITE_DATA_KEY, i), dataRepresentation);
        }
        return expectedResult;
    }

    private Map<String, Object> createTabularDataResult() {
        final Map<String, Object> expectedResult = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            final Map<String, Object> dataRepresentation = createCompositeDataResult(i % 2 == 0, String.format(STRING_ATTRIBUTE_VALUE, TEST_TABULAR_STRING_VALUE, i), i);
            expectedResult.put(String.format(TABULAR_DATA_KEY, TEST_TABULAR_STRING_VALUE, i), dataRepresentation);
        }
        return expectedResult;
    }

    private static class Metric implements MetricMBean {
        private static final String TABLE_DESCRIPTION = "Table for all test";
        private static final String TABLE_NAME = "Test table";
        private static final String METRIC_TYPE_DESCRIPTION = "Metric type for testing";
        private static final String METRIC_TYPE_NAME = "Metric type";
        private final String name;
        private final CompositeData data;
        private final CompositeData[] dataList;
        private final TabularData table;
        private final CompositeType compositeType;

        public Metric(final String name) throws OpenDataException {
            this.name = name;
            this.compositeType = createCompositeType();
            this.data = createCompositeData(STRING_VALUE, 0, Boolean.FALSE);
            this.dataList = new CompositeData[]{createCompositeData(String.format(STRING_ATTRIBUTE_VALUE, STRING_VALUE, 1), 1, Boolean.TRUE),
                        createCompositeData(String.format(STRING_ATTRIBUTE_VALUE, STRING_VALUE, 2), 2, Boolean.FALSE),
                        createCompositeData(String.format(STRING_ATTRIBUTE_VALUE, STRING_VALUE, 3), 3, Boolean.TRUE)};
            this.table = createTabularData();
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public CompositeData getData() {
            return this.data;
        }

        @Override
        public CompositeData[] getDataList() {
            return this.dataList;
        }

        @Override
        public TabularData getTable() {
            return this.table;
        }

        private CompositeType createCompositeType() throws OpenDataException {
            return new CompositeType(METRIC_TYPE_NAME,
                    METRIC_TYPE_DESCRIPTION,
                    new String[] {ATTRIBUTE_NAME_STRING, ATTRIBUTE_NAME_INT, ATTRIBUTE_NAME_BOOLEAN},
                    new String[] {ATTRIBUTE_NAME_STRING, ATTRIBUTE_NAME_INT, ATTRIBUTE_NAME_BOOLEAN},
                    new OpenType[] {SimpleType.STRING, SimpleType.INTEGER, SimpleType.BOOLEAN});
        }

        private CompositeData createCompositeData(final String stringValue, final int intValue, final boolean booleanValue) throws OpenDataException {
            return new CompositeDataSupport(this.compositeType,
                    new String[] {ATTRIBUTE_NAME_STRING, ATTRIBUTE_NAME_INT, ATTRIBUTE_NAME_BOOLEAN},
                    new Object[] {stringValue, intValue, booleanValue}
                    );
        }

        private TabularData createTabularData() throws OpenDataException {
            final TabularType tableType = new TabularType(TABLE_NAME,
                    TABLE_DESCRIPTION,
                    this.compositeType,
                    new String[] {ATTRIBUTE_NAME_STRING});
            final TabularData table = new TabularDataSupport(tableType);

            for (int i = 0; i < 3; i++) {
                table.put(createCompositeData(String.format(STRING_ATTRIBUTE_VALUE, TEST_TABULAR_STRING_VALUE, i), i, i % 2 == 0));
            }
            return table;
        }
    }

     public interface MetricMBean {
        String getName();
        CompositeData getData();
        CompositeData[] getDataList();
        TabularData getTable();
    }
}
