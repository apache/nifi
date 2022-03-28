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
package org.apache.nifi.runtime.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.c2.protocol.component.api.BuildInfo;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.PropertyDependency;
import org.apache.nifi.c2.protocol.component.api.PropertyDescriptor;
import org.apache.nifi.c2.protocol.component.api.PropertyResourceDefinition;
import org.apache.nifi.c2.protocol.component.api.Relationship;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.c2.protocol.component.api.SchedulingDefaults;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRuntimeManifest {

    @Test
    public void testRuntimeManifest() throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();

        final RuntimeManifest runtimeManifest;
        try (final InputStream inputStream = new FileInputStream("target/nifi-runtime-manifest/nifi-runtime-manifest.json")) {
            runtimeManifest = objectMapper.readValue(inputStream, RuntimeManifest.class);
        }
        assertNotNull(runtimeManifest);
        assertEquals("apache-nifi", runtimeManifest.getIdentifier());
        assertEquals("nifi", runtimeManifest.getAgentType());
        assertNotNull(runtimeManifest.getVersion());

        final BuildInfo buildInfo = runtimeManifest.getBuildInfo();
        assertNotNull(buildInfo);
        assertNotNull(buildInfo.getCompiler());
        assertNotNull(buildInfo.getRevision());
        assertNotNull(buildInfo.getTimestamp());
        assertNotNull(buildInfo.getVersion());

        final SchedulingDefaults schedulingDefaults = runtimeManifest.getSchedulingDefaults();
        assertNotNull(schedulingDefaults);
        assertEquals(SchedulingStrategy.TIMER_DRIVEN, schedulingDefaults.getDefaultSchedulingStrategy());

        final Map<String, Integer> defaultConcurrentTasks = schedulingDefaults.getDefaultConcurrentTasksBySchedulingStrategy();
        assertNotNull(defaultConcurrentTasks);
        assertEquals(3, defaultConcurrentTasks.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks(), defaultConcurrentTasks.get(SchedulingStrategy.TIMER_DRIVEN.name()).intValue());
        assertEquals(SchedulingStrategy.EVENT_DRIVEN.getDefaultConcurrentTasks(), defaultConcurrentTasks.get(SchedulingStrategy.EVENT_DRIVEN.name()).intValue());
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks(), defaultConcurrentTasks.get(SchedulingStrategy.CRON_DRIVEN.name()).intValue());

        final Map<String, String> defaultSchedulingPeriods = schedulingDefaults.getDefaultSchedulingPeriodsBySchedulingStrategy();
        assertEquals(2, defaultSchedulingPeriods.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod(), defaultSchedulingPeriods.get(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod(), defaultSchedulingPeriods.get(SchedulingStrategy.CRON_DRIVEN.name()));

        final List<Bundle> bundles = runtimeManifest.getBundles();
        assertNotNull(bundles);
        assertTrue(bundles.size() > 0);

        // Verify ListHDFS definition
        final ProcessorDefinition listHdfsDefinition = getProcessorDefinition(bundles, "nifi-hadoop-nar", "org.apache.nifi.processors.hadoop.ListHDFS");
        assertNotNull(listHdfsDefinition);
        assertTrue(listHdfsDefinition.getPrimaryNodeOnly());
        assertTrue(listHdfsDefinition.getTriggerSerially());
        assertTrue(listHdfsDefinition.getTriggerWhenEmpty());
        assertFalse(listHdfsDefinition.getSupportsBatching());
        assertFalse(listHdfsDefinition.getSupportsEventDriven());
        assertFalse(listHdfsDefinition.getSideEffectFree());
        assertFalse(listHdfsDefinition.getTriggerWhenAnyDestinationAvailable());
        assertFalse(listHdfsDefinition.getSupportsDynamicProperties());
        assertFalse(listHdfsDefinition.getSupportsDynamicRelationships());
        assertEquals(InputRequirement.Requirement.INPUT_FORBIDDEN, listHdfsDefinition.getInputRequirement());

        assertEquals("30 sec", listHdfsDefinition.getDefaultPenaltyDuration());
        assertEquals("1 sec", listHdfsDefinition.getDefaultYieldDuration());
        assertEquals("WARN", listHdfsDefinition.getDefaultBulletinLevel());

        assertEquals(SchedulingStrategy.TIMER_DRIVEN.name(), listHdfsDefinition.getDefaultSchedulingStrategy());

        final List<String> listHdfsSchedulingStrategies = listHdfsDefinition.getSupportedSchedulingStrategies();
        assertNotNull(listHdfsSchedulingStrategies);
        assertEquals(2, listHdfsSchedulingStrategies.size());
        assertTrue(listHdfsSchedulingStrategies.contains(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertTrue(listHdfsSchedulingStrategies.contains(SchedulingStrategy.CRON_DRIVEN.name()));

        final Map<String, Integer> listHdfsDefaultConcurrentTasks = listHdfsDefinition.getDefaultConcurrentTasksBySchedulingStrategy();
        assertNotNull(listHdfsDefaultConcurrentTasks);
        assertEquals(2, listHdfsDefaultConcurrentTasks.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks(), listHdfsDefaultConcurrentTasks.get(SchedulingStrategy.TIMER_DRIVEN.name()).intValue());
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks(), listHdfsDefaultConcurrentTasks.get(SchedulingStrategy.CRON_DRIVEN.name()).intValue());

        final Map<String, String> listHdfsDefaultSchedulingPeriods = listHdfsDefinition.getDefaultSchedulingPeriodBySchedulingStrategy();
        assertNotNull(listHdfsDefaultSchedulingPeriods);
        assertEquals(2, listHdfsDefaultSchedulingPeriods.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod(), listHdfsDefaultSchedulingPeriods.get(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod(), listHdfsDefaultSchedulingPeriods.get(SchedulingStrategy.CRON_DRIVEN.name()));

        final List<Relationship> relationships = listHdfsDefinition.getSupportedRelationships();
        assertNotNull(relationships);
        assertEquals(1, relationships.size());
        assertEquals("success", relationships.get(0).getName());

        final PropertyDescriptor configResourcesProp = getPropertyDescriptor(listHdfsDefinition, "Hadoop Configuration Resources");

        final PropertyResourceDefinition resourceDefinition = configResourcesProp.getResourceDefinition();
        assertNotNull(resourceDefinition);
        assertEquals(ResourceCardinality.MULTIPLE, resourceDefinition.getCardinality());
        assertNotNull(resourceDefinition.getResourceTypes());
        assertEquals(1, resourceDefinition.getResourceTypes().size());
        assertEquals(ResourceType.FILE, resourceDefinition.getResourceTypes().stream().findFirst().get());

        // Verify ConsumeKafka_2_6 definition which has properties with dependencies
        final ProcessorDefinition consumeKafkaDefinition = getProcessorDefinition(bundles, "nifi-kafka-2-6-nar",
                "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6");

        final PropertyDescriptor maxUncommitProp = getPropertyDescriptor(consumeKafkaDefinition, "max-uncommit-offset-wait");
        final List<PropertyDependency> propertyDependencies = maxUncommitProp.getDependencies();
        assertNotNull(propertyDependencies);
        assertEquals(1, propertyDependencies.size());

        final PropertyDependency propertyMaxUncommitDependency = propertyDependencies.get(0);
        assertEquals("Commit Offsets", propertyMaxUncommitDependency.getPropertyName());
        assertNotNull(propertyMaxUncommitDependency.getDependentValues());
        assertEquals(1, propertyMaxUncommitDependency.getDependentValues().size());
        assertEquals("true", propertyMaxUncommitDependency.getDependentValues().get(0));

        // Verify AmbariReportingTask definition which also has @DefaultSchedule
        final ReportingTaskDefinition ambariReportingTaskDef = getReportingTaskDefinition(bundles, "nifi-ambari-nar",
                "org.apache.nifi.reporting.ambari.AmbariReportingTask");

        assertEquals(SchedulingStrategy.TIMER_DRIVEN.name(), ambariReportingTaskDef.getDefaultSchedulingStrategy());

        final List<String> ambariSchedulingStrategies = ambariReportingTaskDef.getSupportedSchedulingStrategies();
        assertNotNull(ambariSchedulingStrategies);
        assertEquals(2, ambariSchedulingStrategies.size());
        assertTrue(ambariSchedulingStrategies.contains(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertTrue(ambariSchedulingStrategies.contains(SchedulingStrategy.CRON_DRIVEN.name()));

        final Map<String, String> ambariDefaultSchedulingPeriods = ambariReportingTaskDef.getDefaultSchedulingPeriodBySchedulingStrategy();
        assertNotNull(ambariDefaultSchedulingPeriods);
        assertEquals(2, ambariDefaultSchedulingPeriods.size());
        // TIMER_DRIVEN period should come from the @DefaultSchedule annotation that overrides the default value
        assertEquals("1 min", ambariDefaultSchedulingPeriods.get(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod(), ambariDefaultSchedulingPeriods.get(SchedulingStrategy.CRON_DRIVEN.name()));

        // Verify JoltTransformRecord which has @EventDriven
        final ProcessorDefinition joltTransformDef = getProcessorDefinition(bundles, "nifi-jolt-record-nar",
                "org.apache.nifi.processors.jolt.record.JoltTransformRecord");

        assertEquals(SchedulingStrategy.TIMER_DRIVEN.name(), joltTransformDef.getDefaultSchedulingStrategy());

        final List<String> joltTransformSchedulingStrategies = joltTransformDef.getSupportedSchedulingStrategies();
        assertNotNull(joltTransformSchedulingStrategies);
        assertEquals(3, joltTransformSchedulingStrategies.size());
        assertTrue(joltTransformSchedulingStrategies.contains(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertTrue(joltTransformSchedulingStrategies.contains(SchedulingStrategy.CRON_DRIVEN.name()));
        assertTrue(joltTransformSchedulingStrategies.contains(SchedulingStrategy.EVENT_DRIVEN.name()));

        final Map<String, Integer> joltTransformDefaultConcurrentTasks = joltTransformDef.getDefaultConcurrentTasksBySchedulingStrategy();
        assertNotNull(joltTransformDefaultConcurrentTasks);
        assertEquals(3, joltTransformDefaultConcurrentTasks.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks(), joltTransformDefaultConcurrentTasks.get(SchedulingStrategy.TIMER_DRIVEN.name()).intValue());
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks(), joltTransformDefaultConcurrentTasks.get(SchedulingStrategy.CRON_DRIVEN.name()).intValue());
        assertEquals(SchedulingStrategy.EVENT_DRIVEN.getDefaultConcurrentTasks(), joltTransformDefaultConcurrentTasks.get(SchedulingStrategy.EVENT_DRIVEN.name()).intValue());

        final Map<String, String> joltTransformDefaultSchedulingPeriods = listHdfsDefinition.getDefaultSchedulingPeriodBySchedulingStrategy();
        assertNotNull(joltTransformDefaultSchedulingPeriods);
        assertEquals(2, joltTransformDefaultSchedulingPeriods.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod(), joltTransformDefaultSchedulingPeriods.get(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod(), joltTransformDefaultSchedulingPeriods.get(SchedulingStrategy.CRON_DRIVEN.name()));
    }

    private PropertyDescriptor getPropertyDescriptor(final ProcessorDefinition processorDefinition, final String propName) {
        final Map<String, PropertyDescriptor> propertyDescriptors = processorDefinition.getPropertyDescriptors();
        assertNotNull(propertyDescriptors);

        final PropertyDescriptor propertyDescriptor = propertyDescriptors.values().stream()
                .filter(p -> p.getName().equals(propName))
                .findFirst()
                .orElse(null);
        assertNotNull(propertyDescriptor);
        return propertyDescriptor;
    }

    private ProcessorDefinition getProcessorDefinition(final List<Bundle> bundles, final String artifactId, final String type) {
        final ComponentManifest componentManifest = getComponentManifest(bundles, artifactId);

        final List<ProcessorDefinition> processors = componentManifest.getProcessors();
        assertNotNull(processors);

        final ProcessorDefinition processorDefinition = processors.stream()
                .filter(p -> p.getType().equals(type))
                .findFirst()
                .orElse(null);
        assertNotNull(processorDefinition);
        return processorDefinition;
    }

    private ReportingTaskDefinition getReportingTaskDefinition(final List<Bundle> bundles, final String artifactId, final String type) {
        final ComponentManifest componentManifest = getComponentManifest(bundles, artifactId);

        final List<ReportingTaskDefinition> reportingTasks = componentManifest.getReportingTasks();
        assertNotNull(reportingTasks);

        final ReportingTaskDefinition reportingTaskDefinition = reportingTasks.stream()
                .filter(p -> p.getType().equals(type))
                .findFirst()
                .orElse(null);
        assertNotNull(reportingTaskDefinition);
        return reportingTaskDefinition;
    }

    private ComponentManifest getComponentManifest(final List<Bundle> bundles, final String artifactId) {
        final Bundle bundle = bundles.stream().filter(b -> b.getArtifact().equals(artifactId)).findFirst().orElse(null);
        assertNotNull(bundle);

        final ComponentManifest componentManifest = bundle.getComponentManifest();
        assertNotNull(componentManifest);
        return componentManifest;
    }
}
