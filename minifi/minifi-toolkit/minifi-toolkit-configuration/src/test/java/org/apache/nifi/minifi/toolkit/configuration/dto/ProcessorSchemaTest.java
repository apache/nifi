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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ANNOTATION_DATA_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CLASS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROPERTIES_KEY;
import static org.junit.Assert.assertEquals;

public class ProcessorSchemaTest extends BaseSchemaTester<ProcessorSchema, ProcessorDTO> {
    private final String testName = "testName";
    private final String testId = UUID.nameUUIDFromBytes("testId".getBytes(StandardCharsets.UTF_8)).toString();
    private final String testProcessorClass = "testProcessorClass";
    private final String testSchedulingStrategy = SchedulingStrategy.PRIMARY_NODE_ONLY.name();
    private final String testSchedulingPeriod = "10 s";
    private final int testMaxConcurrentTasks = 101;
    private final String testYieldDuration = "5 s";
    private final long testRunDurationNanos = 1111000L;
    private final String testRelationship = "testRelationship";
    private final String testKey = "testKey";
    private final String testValue = "testValue";
    private final String testPenalizationPeriod = "55 s";
    private final String testAnnotationData = "&lt;criteria&gt;\n" +
            "    &lt;flowFilePolicy&gt;USE_ORIGINAL&lt;/flowFilePolicy&gt;\n" +
            "    &lt;rules&gt;\n" +
            "        &lt;actions&gt;\n" +
            "            &lt;attribute&gt;it's the one&lt;/attribute&gt;\n" +
            "            &lt;id&gt;364cdf1a-3ac8-46a7-9d5d-e83618d9dfde&lt;/id&gt;\n" +
            "            &lt;value&gt;true&lt;/value&gt;\n" +
            "        &lt;/actions&gt;\n" +
            "        &lt;conditions&gt;\n" +
            "            &lt;expression&gt;${uuid:equals(\"theUUID!\")}&lt;/expression&gt;\n" +
            "            &lt;id&gt;c0ceadcd-6de5-4994-bf76-f76a5fd1640c&lt;/id&gt;\n" +
            "        &lt;/conditions&gt;\n" +
            "        &lt;id&gt;e6fb8d10-49ac-494b-8f02-ced1109c9445&lt;/id&gt;\n" +
            "        &lt;name&gt;testRule&lt;/name&gt;\n" +
            "    &lt;/rules&gt;\n" +
            "    &lt;rules&gt;\n" +
            "        &lt;actions&gt;\n" +
            "            &lt;attribute&gt;the tenth&lt;/attribute&gt;\n" +
            "            &lt;id&gt;0bbe7bcd-e6b9-4801-b1e4-337da5f0532f&lt;/id&gt;\n" +
            "            &lt;value&gt;${hostname()}&lt;/value&gt;\n" +
            "        &lt;/actions&gt;\n" +
            "        &lt;conditions&gt;\n" +
            "            &lt;expression&gt;${random:mod(10):equals(0)}&lt;/expression&gt;\n" +
            "            &lt;id&gt;48735bfe-02ef-4634-8d99-f384ebcd8fa8&lt;/id&gt;\n" +
            "        &lt;/conditions&gt;\n" +
            "        &lt;id&gt;74b86ba2-4a8b-4952-8aef-ea672847c589&lt;/id&gt;\n" +
            "        &lt;name&gt;testRule3&lt;/name&gt;\n" +
            "    &lt;/rules&gt;\n" +
            "&lt;/criteria&gt;";
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
        dto.setId(testId);
        dto.setType(testProcessorClass);
        config.setSchedulingStrategy(testSchedulingStrategy);
        config.setSchedulingPeriod(testSchedulingPeriod);
        config.setConcurrentlySchedulableTaskCount(testMaxConcurrentTasks);
        config.setPenaltyDuration(testPenalizationPeriod);
        config.setYieldDuration(testYieldDuration);
        config.setRunDurationMillis(testRunDurationNanos / 1000);
        config.setAnnotationData(testAnnotationData);
        dto.setRelationships(Arrays.asList(relationshipDTO));
        Map<String, String> properties = new HashMap<>();
        properties.put(testKey, testValue);
        config.setProperties(properties);

        map = new HashMap<>();
        map.put(CommonPropertyKeys.NAME_KEY, testName);
        map.put(CommonPropertyKeys.ID_KEY, testId);
        map.put(CLASS_KEY, testProcessorClass);
        map.put(CommonPropertyKeys.SCHEDULING_STRATEGY_KEY, testSchedulingStrategy);
        map.put(CommonPropertyKeys.SCHEDULING_PERIOD_KEY, testSchedulingPeriod);
        map.put(CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY, testMaxConcurrentTasks);
        map.put(ProcessorSchema.PENALIZATION_PERIOD_KEY, testPenalizationPeriod);
        map.put(CommonPropertyKeys.YIELD_PERIOD_KEY, testYieldDuration);
        map.put(ProcessorSchema.RUN_DURATION_NANOS_KEY, testRunDurationNanos);
        map.put(ProcessorSchema.AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY, Arrays.asList(testRelationship));
        map.put(PROPERTIES_KEY, new HashMap<>(properties));
        map.put(ANNOTATION_DATA_KEY, testAnnotationData);
    }

    @Test
    public void testNoName() {
        dto.setName(null);
        map.remove(CommonPropertyKeys.NAME_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoId() {
        dto.setId(null);
        map.remove(CommonPropertyKeys.ID_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoProcessorClass() {
        dto.setType(null);
        map.remove(CLASS_KEY);
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
        map.remove(PROPERTIES_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoAnnotationData() {
        config.setAnnotationData(null);
        map.remove(ANNOTATION_DATA_KEY);
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
        assertEquals(one.getAnnotationData(), two.getAnnotationData());
    }
}
