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

package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RelationshipDTO;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProcessorSchemaTest extends BaseSchemaTester<ProcessorSchema, ProcessorDTO> {
    private String testName = "testName";
    private String testProcessorClass = "testProcessorClass";
    private String testSchedulingStrategy = SchedulingStrategy.PRIMARY_NODE_ONLY.name();
    private String testSchedulingPeriod = "10 s";
    private int testMaxConcurrentTasks = 101;
    private String testYieldDuration = "5 s";
    private long testRunDurationNanos = 1111000L;
    private String testRelationship = "testRelationship";
    private String testKey = "testKey";
    private String testValue = "testValue";
    private String testPenalizationPeriod = "55 s";
    private ProcessorConfigDTO config;

    public ProcessorSchemaTest() {
        super(new ProcessorSchemaFunction(), ProcessorSchema::new);
    }

    @Before
    public void setup() {
        config = new ProcessorConfigDTO();

        RelationshipDTO relationshipDTO = new RelationshipDTO();
        relationshipDTO.setName(testRelationship);
        relationshipDTO.setAutoTerminate(true);

        dto = new ProcessorDTO();
        dto.setConfig(config);
        dto.setName(testName);
        dto.setType(testProcessorClass);
        config.setSchedulingStrategy(testSchedulingStrategy);
        config.setSchedulingPeriod(testSchedulingPeriod);
        config.setConcurrentlySchedulableTaskCount(testMaxConcurrentTasks);
        config.setPenaltyDuration(testPenalizationPeriod);
        config.setYieldDuration(testYieldDuration);
        config.setRunDurationMillis(testRunDurationNanos / 1000);
        dto.setRelationships(Arrays.asList(relationshipDTO));
        Map<String, String> properties = new HashMap<>();
        properties.put(testKey, testValue);
        config.setProperties(properties);

        map = new HashMap<>();
        map.put(CommonPropertyKeys.NAME_KEY, testName);
        map.put(ProcessorSchema.CLASS_KEY, testProcessorClass);
        map.put(CommonPropertyKeys.SCHEDULING_STRATEGY_KEY, testSchedulingStrategy);
        map.put(CommonPropertyKeys.SCHEDULING_PERIOD_KEY, testSchedulingPeriod);
        map.put(CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY, testMaxConcurrentTasks);
        map.put(ProcessorSchema.PENALIZATION_PERIOD_KEY, testPenalizationPeriod);
        map.put(CommonPropertyKeys.YIELD_PERIOD_KEY, testYieldDuration);
        map.put(ProcessorSchema.RUN_DURATION_NANOS_KEY, testRunDurationNanos);
        map.put(ProcessorSchema.AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY, Arrays.asList(testRelationship));
        map.put(ProcessorSchema.PROCESSOR_PROPS_KEY, new HashMap<>(properties));
    }

    @Test
    public void testNoName() {
        dto.setName(null);
        map.remove(CommonPropertyKeys.NAME_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoProcessorClass() {
        dto.setType(null);
        map.remove(ProcessorSchema.CLASS_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoSchedulingStrategy() {
        config.setSchedulingStrategy(null);
        map.remove(CommonPropertyKeys.SCHEDULING_STRATEGY_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testInvalidSchedulingStrategy() {
        String fake = "fake";
        config.setSchedulingStrategy(fake);
        map.put(CommonPropertyKeys.SCHEDULING_STRATEGY_KEY, fake);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoSchedulingPeriod() {
        config.setSchedulingPeriod(null);
        map.remove(CommonPropertyKeys.SCHEDULING_PERIOD_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoMaxConcurrentTasks() {
        config.setConcurrentlySchedulableTaskCount(null);
        map.remove(CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoPenalizationPeriod() {
        config.setPenaltyDuration(null);
        map.remove(ProcessorSchema.PENALIZATION_PERIOD_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoYieldPeriod() {
        config.setYieldDuration(null);
        map.remove(CommonPropertyKeys.YIELD_PERIOD_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoRunDurationNanos() {
        config.setRunDurationMillis(null);
        map.remove(ProcessorSchema.RUN_DURATION_NANOS_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoAutoTerminateRelationships() {
        dto.setRelationships(null);
        map.remove(ProcessorSchema.AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoProperties() {
        config.setProperties(null);
        map.remove(ProcessorSchema.PROCESSOR_PROPS_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Override
    public void assertSchemaEquals(ProcessorSchema one, ProcessorSchema two) {
        assertEquals(one.getName(), two.getName());
        assertEquals(one.getProcessorClass(), two.getProcessorClass());
        assertEquals(one.getSchedulingStrategy(), two.getSchedulingStrategy());
        assertEquals(one.getSchedulingPeriod(), two.getSchedulingPeriod());
        assertEquals(one.getMaxConcurrentTasks(), two.getMaxConcurrentTasks());
        assertEquals(one.getPenalizationPeriod(), two.getPenalizationPeriod());
        assertEquals(one.getYieldPeriod(), two.getYieldPeriod());
        assertEquals(one.getRunDurationNanos(), two.getRunDurationNanos());
        assertEquals(one.getAutoTerminatedRelationshipsList(), two.getAutoTerminatedRelationshipsList());
        assertEquals(one.getProperties(), two.getProperties());
    }
}
