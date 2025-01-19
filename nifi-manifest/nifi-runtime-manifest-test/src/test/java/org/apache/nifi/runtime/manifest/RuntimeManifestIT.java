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
import org.apache.nifi.c2.protocol.component.api.Relationship;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;
import org.apache.nifi.c2.protocol.component.api.Restriction;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.c2.protocol.component.api.SchedulingDefaults;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeManifestIT {

    @Test
    void testRuntimeManifest() throws IOException {
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
        assertEquals(2, defaultConcurrentTasks.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks(), defaultConcurrentTasks.get(SchedulingStrategy.TIMER_DRIVEN.name()).intValue());
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks(), defaultConcurrentTasks.get(SchedulingStrategy.CRON_DRIVEN.name()).intValue());

        final Map<String, String> defaultSchedulingPeriods = schedulingDefaults.getDefaultSchedulingPeriodsBySchedulingStrategy();
        assertEquals(2, defaultSchedulingPeriods.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod(), defaultSchedulingPeriods.get(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod(), defaultSchedulingPeriods.get(SchedulingStrategy.CRON_DRIVEN.name()));

        final List<Bundle> bundles = runtimeManifest.getBundles();
        assertNotNull(bundles);
        assertFalse(bundles.isEmpty());

        assertProcessorDefinitionFound(bundles);
        assertRestrictionsFound(bundles);

        // Verify TailFile definition which has properties with dependencies
        final ProcessorDefinition tailFileDefinition = getProcessorDefinition(bundles, "nifi-standard-nar",
                "org.apache.nifi.processors.standard.TailFile");
        assertTrue(tailFileDefinition.isAdditionalDetails());

        final PropertyDescriptor lineStartPattern = getPropertyDescriptor(tailFileDefinition, "Line Start Pattern");
        final List<PropertyDependency> propertyDependencies = lineStartPattern.getDependencies();
        assertNotNull(propertyDependencies);
        assertEquals(1, propertyDependencies.size());

        final PropertyDependency lineStartPatternDependency = propertyDependencies.get(0);
        assertEquals("tail-mode", lineStartPatternDependency.getPropertyName());
        assertNotNull(lineStartPatternDependency.getDependentValues());
        assertEquals(1, lineStartPatternDependency.getDependentValues().size());
        assertEquals("Single file", lineStartPatternDependency.getDependentValues().get(0));

        final ProcessorDefinition joltTransformDef = getProcessorDefinition(bundles, "nifi-jolt-nar",
                "org.apache.nifi.processors.jolt.JoltTransformRecord");
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.name(), joltTransformDef.getDefaultSchedulingStrategy());

        final List<String> joltTransformSchedulingStrategies = joltTransformDef.getSupportedSchedulingStrategies();
        assertNotNull(joltTransformSchedulingStrategies);
        assertEquals(2, joltTransformSchedulingStrategies.size());
        assertTrue(joltTransformSchedulingStrategies.contains(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertTrue(joltTransformSchedulingStrategies.contains(SchedulingStrategy.CRON_DRIVEN.name()));

        final Map<String, Integer> joltTransformDefaultConcurrentTasks = joltTransformDef.getDefaultConcurrentTasksBySchedulingStrategy();
        assertNotNull(joltTransformDefaultConcurrentTasks);
        assertEquals(2, joltTransformDefaultConcurrentTasks.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks(), joltTransformDefaultConcurrentTasks.get(SchedulingStrategy.TIMER_DRIVEN.name()).intValue());
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks(), joltTransformDefaultConcurrentTasks.get(SchedulingStrategy.CRON_DRIVEN.name()).intValue());

        final Map<String, String> joltTransformDefaultSchedulingPeriods = joltTransformDef.getDefaultSchedulingPeriodBySchedulingStrategy();
        assertNotNull(joltTransformDefaultSchedulingPeriods);
        assertEquals(2, joltTransformDefaultSchedulingPeriods.size());
        assertEquals("0 sec", joltTransformDefaultSchedulingPeriods.get(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod(), joltTransformDefaultSchedulingPeriods.get(SchedulingStrategy.CRON_DRIVEN.name()));

        // Verify ExecuteSQL has readsAttributes
        final ProcessorDefinition executeSqlDef = getProcessorDefinition(bundles, "nifi-standard-nar",
                "org.apache.nifi.processors.standard.ExecuteSQL");
        assertNotNull(executeSqlDef.getReadsAttributes());
        assertFalse(executeSqlDef.getReadsAttributes().isEmpty());
        assertNotNull(executeSqlDef.getReadsAttributes().get(0).getName());
        assertNotNull(executeSqlDef.getReadsAttributes().get(0).getDescription());
        assertTrue(executeSqlDef.getSupportsSensitiveDynamicProperties());

        // Verify RouteOnAttribute dynamic relationships and dynamic properties
        final ProcessorDefinition routeOnAttributeDef = getProcessorDefinition(bundles, "nifi-standard-nar",
                "org.apache.nifi.processors.standard.RouteOnAttribute");

        assertTrue(routeOnAttributeDef.getSupportsDynamicRelationships());
        assertNotNull(routeOnAttributeDef.getDynamicRelationship());
        assertNotNull(routeOnAttributeDef.getDynamicRelationship().getName());
        assertNotNull(routeOnAttributeDef.getDynamicRelationship().getDescription());

        assertTrue(routeOnAttributeDef.getSupportsDynamicProperties());
        assertFalse(routeOnAttributeDef.getSupportsSensitiveDynamicProperties());
        assertNotNull(routeOnAttributeDef.getDynamicProperties());
        assertFalse(routeOnAttributeDef.getDynamicProperties().isEmpty());
        assertNotNull(routeOnAttributeDef.getDynamicProperties().get(0).getName());
        assertNotNull(routeOnAttributeDef.getDynamicProperties().get(0).getDescription());
        assertNotNull(routeOnAttributeDef.getDynamicProperties().get(0).getValue());
        assertNotNull(routeOnAttributeDef.getDynamicProperties().get(0).getExpressionLanguageScope());

        // Verify SplitJson has @SystemResourceConsiderations
        final ProcessorDefinition splitJsonDef = getProcessorDefinition(bundles, "nifi-standard-nar",
                "org.apache.nifi.processors.standard.SplitJson");
        assertNotNull(splitJsonDef.getSystemResourceConsiderations());
        assertFalse(splitJsonDef.getSystemResourceConsiderations().isEmpty());
        assertNotNull(splitJsonDef.getSystemResourceConsiderations().get(0).getResource());
        assertNotNull(splitJsonDef.getSystemResourceConsiderations().get(0).getDescription());
    }

    private void assertProcessorDefinitionFound(final List<Bundle> bundles) {
        final ProcessorDefinition definition = getProcessorDefinition(bundles, "nifi-standard-nar", "org.apache.nifi.processors.standard.ListFile");
        assertNotNull(definition);
        assertFalse(definition.getPrimaryNodeOnly());
        assertTrue(definition.getTriggerSerially());
        assertFalse(definition.getTriggerWhenEmpty());
        assertFalse(definition.getSupportsBatching());
        assertFalse(definition.getSideEffectFree());
        assertFalse(definition.getTriggerWhenAnyDestinationAvailable());
        assertFalse(definition.getSupportsDynamicProperties());
        assertFalse(definition.getSupportsSensitiveDynamicProperties());
        assertNull(definition.getDynamicProperties());
        assertFalse(definition.getSupportsDynamicRelationships());
        assertNull(definition.getDynamicRelationship());
        assertEquals(InputRequirement.Requirement.INPUT_FORBIDDEN, definition.getInputRequirement());
        assertTrue(definition.isAdditionalDetails());
        assertNull(definition.getReadsAttributes());
        assertNotNull(definition.getWritesAttributes());
        assertFalse(definition.getWritesAttributes().isEmpty());
        assertNotNull(definition.getWritesAttributes().get(0).getName());
        assertNotNull(definition.getWritesAttributes().get(0).getDescription());
        assertNotNull(definition.getSeeAlso());
        assertFalse(definition.getSeeAlso().isEmpty());
        assertNull(definition.getSystemResourceConsiderations());
        assertNull(definition.getDeprecated());
        assertNull(definition.getDeprecationReason());
        assertNull(definition.getDeprecationAlternatives());

        assertEquals("30 sec", definition.getDefaultPenaltyDuration());
        assertEquals("1 sec", definition.getDefaultYieldDuration());
        assertEquals("WARN", definition.getDefaultBulletinLevel());

        assertEquals(SchedulingStrategy.TIMER_DRIVEN.name(), definition.getDefaultSchedulingStrategy());

        final List<String> schedulingStrategies = definition.getSupportedSchedulingStrategies();
        assertNotNull(schedulingStrategies);
        assertEquals(2, schedulingStrategies.size());
        assertTrue(schedulingStrategies.contains(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertTrue(schedulingStrategies.contains(SchedulingStrategy.CRON_DRIVEN.name()));

        final Map<String, Integer> defaultConcurrentTasks = definition.getDefaultConcurrentTasksBySchedulingStrategy();
        assertNotNull(defaultConcurrentTasks);
        assertEquals(2, defaultConcurrentTasks.size());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks(), defaultConcurrentTasks.get(SchedulingStrategy.TIMER_DRIVEN.name()).intValue());
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks(), defaultConcurrentTasks.get(SchedulingStrategy.CRON_DRIVEN.name()).intValue());

        final Map<String, String> defaultSchedulingPeriods = definition.getDefaultSchedulingPeriodBySchedulingStrategy();
        assertNotNull(defaultSchedulingPeriods);
        assertEquals(2, defaultSchedulingPeriods.size());
        assertEquals("1 min", defaultSchedulingPeriods.get(SchedulingStrategy.TIMER_DRIVEN.name()));
        assertEquals(SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod(), defaultSchedulingPeriods.get(SchedulingStrategy.CRON_DRIVEN.name()));

        final List<Relationship> relationships = definition.getSupportedRelationships();
        assertNotNull(relationships);
        assertEquals(1, relationships.size());
        assertEquals("success", relationships.get(0).getName());

        assertNull(definition.isRestricted());
        assertNull(definition.getRestrictedExplanation());
        assertNull(definition.getExplicitRestrictions());
        assertNotNull(definition.getStateful());
        assertNotNull(definition.getStateful().getDescription());
        assertNotNull(definition.getStateful().getScopes());
        assertEquals(Set.of(Scope.LOCAL, Scope.CLUSTER), definition.getStateful().getScopes());
    }

    private void assertRestrictionsFound(final List<Bundle> bundles) {
        final ProcessorDefinition processorDefinition = getProcessorDefinition(bundles, "nifi-standard-nar", "org.apache.nifi.processors.standard.GetFile");
        assertNotNull(processorDefinition.isRestricted());
        assertTrue(processorDefinition.isRestricted());
        assertFalse(processorDefinition.isAdditionalDetails());

        final Set<Restriction> restrictions = processorDefinition.getExplicitRestrictions();
        assertNotNull(restrictions);
        assertEquals(2, restrictions.size());

        final Restriction restriction = restrictions.stream().findFirst().orElse(null);
        assertEquals(RequiredPermission.READ_FILESYSTEM.getPermissionLabel(), restriction.getRequiredPermission());
        assertNotNull(restriction.getExplanation());
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
