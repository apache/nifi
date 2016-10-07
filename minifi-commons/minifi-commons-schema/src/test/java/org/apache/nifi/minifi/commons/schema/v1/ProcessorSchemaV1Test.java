/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.commons.schema.v1;

import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.CLASS_KEY;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.DEFAULT_AUTO_TERMINATED_RELATIONSHIPS_LIST;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.DEFAULT_MAX_CONCURRENT_TASKS;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.DEFAULT_PENALIZATION_PERIOD;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.DEFAULT_PROPERTIES;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.DEFAULT_RUN_DURATION_NANOS;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.DEFAULT_YIELD_DURATION;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.PENALIZATION_PERIOD_KEY;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.PROCESSOR_PROPS_KEY;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.RUN_DURATION_NANOS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_PERIOD_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_STRATEGY_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.YIELD_PERIOD_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ProcessorSchemaV1Test {
    private String testName;
    private String testProcessorClass;
    private String testSchedulingStrategy;
    private String testSchedulingPeriod;
    private int testMaxConcurrentTasks;
    private String testPenalizationPeriod;
    private String testYieldPeriod;
    private int testRunDurationNanos;
    private String testAutoTerminatedRelationship1;
    private String testAutoTerminatedRelationship2;
    private List<String> testAutoTerminatedRelationships;
    private String testKey1;
    private String testValue1;
    private String testKey2;
    private String testValue2;
    private Map<String, Object> testProperties;

    @Before
    public void setup() {
        testName = "testName";
        testProcessorClass = "testProcessorClass";
        testSchedulingStrategy = SchedulingStrategy.PRIMARY_NODE_ONLY.toString();
        testSchedulingPeriod = "testSchedulingPeriod";
        testMaxConcurrentTasks = 55;
        testPenalizationPeriod = "testPenalizationPeriod";
        testYieldPeriod = "testYieldPeriod";
        testRunDurationNanos = 125;
        testAutoTerminatedRelationship1 = "testAutoTerminatedRelationship1";
        testAutoTerminatedRelationship2 = "testAutoTerminatedRelationship2";
        testAutoTerminatedRelationships = new ArrayList<>(Arrays.asList(testAutoTerminatedRelationship1, testAutoTerminatedRelationship2));
        testKey1 = "testKey1";
        testValue1 = "testValue1";
        testKey2 = "testKey2";
        testValue2 = "testValue2";
        testProperties = new HashMap<>();
        testProperties.put(testKey1, testValue1);
        testProperties.put(testKey2, testValue2);
    }

    private ProcessorSchemaV1 createSchema(int expectedValidationIssues) {
        return createSchema(createMap(), expectedValidationIssues);
    }

    private ProcessorSchemaV1 createSchema(Map<String, Object> map, int expectedValidationIssues) {
        ProcessorSchemaV1 processorSchemaV1 = new ProcessorSchemaV1(map);
        assertEquals(expectedValidationIssues, processorSchemaV1.getValidationIssues().size());
        return processorSchemaV1;
    }

    private Map<String, Object> createMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(NAME_KEY, testName);
        map.put(CLASS_KEY, testProcessorClass);
        map.put(SCHEDULING_STRATEGY_KEY, testSchedulingStrategy);
        map.put(SCHEDULING_PERIOD_KEY, testSchedulingPeriod);
        map.put(MAX_CONCURRENT_TASKS_KEY, testMaxConcurrentTasks);
        map.put(PENALIZATION_PERIOD_KEY, testPenalizationPeriod);
        map.put(YIELD_PERIOD_KEY, testYieldPeriod);
        map.put(RUN_DURATION_NANOS_KEY, testRunDurationNanos);
        map.put(AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY, testAutoTerminatedRelationships);
        map.put(PROCESSOR_PROPS_KEY, testProperties);
        return map;
    }

    @Test
    public void testName() {
        assertEquals(testName, createSchema(0).convert().getName());
    }

    @Test
    public void testNoName() {
        Map<String, Object> map = createMap();
        map.remove(NAME_KEY);
        assertNull(createSchema(map, 1).getName());
    }

    @Test
    public void testProcessorClass() {
        assertEquals(testProcessorClass, createSchema(0).convert().getProcessorClass());
    }

    @Test
    public void testNoProcessorClass() {
        Map<String, Object> map = createMap();
        map.remove(CLASS_KEY);
        assertNull(createSchema(map, 1).convert().getProcessorClass());
    }

    @Test
    public void testSchedulingStrategy() {
        assertEquals(testSchedulingStrategy, createSchema(0).convert().getSchedulingStrategy());
    }

    @Test
    public void testNoSchedulingStrategy() {
        Map<String, Object> map = createMap();
        map.remove(SCHEDULING_STRATEGY_KEY);
        assertNull(createSchema(map, 1).convert().getSchedulingStrategy());
    }

    @Test
    public void testInvalidSchedulingStrategy() {
        testSchedulingStrategy = "fake strategy";
        assertEquals(testSchedulingStrategy, createSchema(1).convert().getSchedulingStrategy());
    }

    @Test
    public void testSchedulingPeriod() {
        assertEquals(testSchedulingPeriod, createSchema(0).convert().getSchedulingPeriod());
    }

    @Test
    public void testNoSchedulingPeriod() {
        Map<String, Object> map = createMap();
        map.remove(SCHEDULING_PERIOD_KEY);
        assertNull(createSchema(map, 1).convert().getSchedulingPeriod());
    }

    @Test
    public void testMaxConcurrentTasks() {
        assertEquals(testMaxConcurrentTasks, createSchema(0).convert().getMaxConcurrentTasks());
    }

    @Test
    public void testNoMaxConcurrentTasks() {
        Map<String, Object> map = createMap();
        map.remove(MAX_CONCURRENT_TASKS_KEY);
        assertEquals(DEFAULT_MAX_CONCURRENT_TASKS, createSchema(map, 0).convert().getMaxConcurrentTasks());
    }

    @Test
    public void testPenalizationPeriod() {
        assertEquals(testPenalizationPeriod, createSchema(0).convert().getPenalizationPeriod());
    }

    @Test
    public void testNoPenalizationPeriod() {
        Map<String, Object> map = createMap();
        map.remove(PENALIZATION_PERIOD_KEY);
        assertEquals(DEFAULT_PENALIZATION_PERIOD, createSchema(map, 0).convert().getPenalizationPeriod());
    }

    @Test
    public void testYieldPeriod() {
        assertEquals(testYieldPeriod, createSchema(0).convert().getYieldPeriod());
    }

    @Test
    public void testNoYieldPeriod() {
        Map<String, Object> map = createMap();
        map.remove(YIELD_PERIOD_KEY);
        assertEquals(DEFAULT_YIELD_DURATION, createSchema(map, 0).convert().getYieldPeriod());
    }

    @Test
    public void testRunDurationNanos() {
        assertEquals(testRunDurationNanos, createSchema(0).convert().getRunDurationNanos());
    }

    @Test
    public void testNoRunDurationNanos() {
        Map<String, Object> map = createMap();
        map.remove(RUN_DURATION_NANOS_KEY);
        assertEquals(DEFAULT_RUN_DURATION_NANOS, createSchema(map, 0).convert().getRunDurationNanos());
    }

    @Test
    public void testAutoTerminatedRelationships() {
        assertEquals(testAutoTerminatedRelationships, createSchema(0).convert().getAutoTerminatedRelationshipsList());
    }

    @Test
    public void testNoAutoTerminatedRelationships() {
        Map<String, Object> map = createMap();
        map.remove(AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY);
        assertEquals(DEFAULT_AUTO_TERMINATED_RELATIONSHIPS_LIST, createSchema(map, 0).convert().getAutoTerminatedRelationshipsList());
    }

    @Test
    public void testProperties() {
        assertEquals(testProperties, createSchema(0).convert().getProperties());
    }

    @Test
    public void testNoProperties() {
        Map<String, Object> map = createMap();
        map.remove(PROCESSOR_PROPS_KEY);
        assertEquals(DEFAULT_PROPERTIES, createSchema(map, 0).convert().getProperties());
    }
}
