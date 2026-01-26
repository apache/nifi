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
package org.apache.nifi.components.connector;

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardConnectorInitializationContext {


    @Test
    public void testCreateParameterValuesSingleContext() {
        final VersionedParameterContext context = new VersionedParameterContext();
        context.setName("ctx");
        context.setInheritedParameterContexts(List.of());

        final Set<VersionedParameter> parameters = new HashSet<>();
        parameters.add(createVersionedParameter("p1", "v1", false));
        parameters.add(createVersionedParameter("p2", "secret", true));
        context.setParameters(parameters);

        final List<ParameterValue> parameterValues = createParameterValues(List.of(context));
        assertEquals(2, parameterValues.size());

        final Map<String, ParameterValue> byName = indexByName(parameterValues);
        assertEquals("v1", byName.get("p1").getValue());
        assertFalse(byName.get("p1").isSensitive());
        assertEquals("secret", byName.get("p2").getValue());
        assertTrue(byName.get("p2").isSensitive());
    }

    @Test
    public void testCreateParameterValuesInheritedPrecedenceOrder() {
        // Inherited contexts listed highest precedence first; reverse traversal ensures last applied wins
        final VersionedParameterContext high = new VersionedParameterContext();
        high.setName("high");
        high.setInheritedParameterContexts(List.of());
        high.setParameters(Set.of(createVersionedParameter("p", "H", false)));

        final VersionedParameterContext low = new VersionedParameterContext();
        low.setName("low");
        low.setInheritedParameterContexts(List.of());
        low.setParameters(Set.of(createVersionedParameter("p", "L", false)));

        final VersionedParameterContext child = new VersionedParameterContext();
        child.setName("child");
        child.setInheritedParameterContexts(List.of("high", "low"));
        child.setParameters(Set.of());

        final Collection<VersionedParameterContext> contexts = List.of(child, high, low);
        final List<ParameterValue> parameterValues = createParameterValues(contexts);
        final Map<String, ParameterValue> byName = indexByName(parameterValues);
        assertEquals(1, byName.size());
        assertEquals("H", byName.get("p").getValue());
    }

    @Test
    public void testCreateParameterValuesLocalOverridesInherited() {
        final VersionedParameterContext base = new VersionedParameterContext();
        base.setName("base");
        base.setInheritedParameterContexts(List.of());
        base.setParameters(Set.of(
            createVersionedParameter("x", "1", false),
            createVersionedParameter("y", "Y", false)
        ));

        final VersionedParameterContext mid = new VersionedParameterContext();
        mid.setName("mid");
        mid.setInheritedParameterContexts(List.of("base"));
        mid.setParameters(Set.of(
            createVersionedParameter("x", "2", false)
        ));

        final VersionedParameterContext top = new VersionedParameterContext();
        top.setName("top");
        top.setInheritedParameterContexts(List.of("mid"));
        top.setParameters(Set.of(
            createVersionedParameter("x", "3", false),
            createVersionedParameter("z", "Z", true)
        ));

        final List<VersionedParameterContext> contexts = new ArrayList<>();
        contexts.add(top);
        contexts.add(mid);
        contexts.add(base);

        final List<ParameterValue> parameterValues = createParameterValues(contexts);
        final Map<String, ParameterValue> byName = indexByName(parameterValues);
        assertEquals(3, byName.size());
        assertEquals("3", byName.get("x").getValue());
        assertEquals("Y", byName.get("y").getValue());
        assertEquals("Z", byName.get("z").getValue());
        assertTrue(byName.get("z").isSensitive());
    }

    private VersionedParameter createVersionedParameter(final String name, final String value, final boolean sensitive) {
        final VersionedParameter parameter = new VersionedParameter();
        parameter.setName(name);
        parameter.setValue(value);
        parameter.setSensitive(sensitive);
        return parameter;
    }

    private Map<String, ParameterValue> indexByName(final List<ParameterValue> values) {
        final Map<String, ParameterValue> byName = new HashMap<>();
        for (final ParameterValue value : values) {
            byName.put(value.getName(), value);
        }
        return byName;
    }

    private List<ParameterValue> createParameterValues(final Collection<VersionedParameterContext> contexts) {
        final ConnectorParameterLookup parameterLookup = new ConnectorParameterLookup(contexts, null);
        return parameterLookup.getParameterValues();
    }

    @Test
    public void testResolveBundlesRequireExactBundle() {
        final Bundle unavailableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "0.0.0-NONEXISTENT");
        final Bundle availableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "2.0.0");

        final ComponentBundleLookup bundleLookup = mock(ComponentBundleLookup.class);
        when(bundleLookup.getAvailableBundles("org.apache.nifi.processors.standard.GenerateFlowFile")).thenReturn(List.of(availableBundle));

        final VersionedProcessGroup group = createProcessGroupWithProcessor("GenerateFlowFile",
            "org.apache.nifi.processors.standard.GenerateFlowFile", unavailableBundle);

        final VersionedExternalFlow externalFlow = new VersionedExternalFlow();
        externalFlow.setFlowContents(group);
        externalFlow.setParameterContexts(Map.of());

        final StandardConnectorInitializationContext context = createContext(bundleLookup);
        context.resolveBundles(group, BundleCompatibility.REQUIRE_EXACT_BUNDLE);

        final VersionedProcessor processor = group.getProcessors().iterator().next();
        assertEquals("0.0.0-NONEXISTENT", processor.getBundle().getVersion(), "REQUIRE_EXACT_BUNDLE should not change the bundle");
    }

    @Test
    public void testResolveBundlesResolveBundleWithSingleAvailable() {
        final Bundle unavailableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "0.0.0-NONEXISTENT");
        final Bundle availableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "2.0.0");

        final ComponentBundleLookup bundleLookup = mock(ComponentBundleLookup.class);
        when(bundleLookup.getAvailableBundles("org.apache.nifi.processors.standard.GenerateFlowFile")).thenReturn(List.of(availableBundle));

        final VersionedProcessGroup group = createProcessGroupWithProcessor("GenerateFlowFile", "org.apache.nifi.processors.standard.GenerateFlowFile", unavailableBundle);

        final StandardConnectorInitializationContext context = createContext(bundleLookup);
        context.resolveBundles(group, BundleCompatibility.RESOLVE_BUNDLE);

        final VersionedProcessor processor = group.getProcessors().iterator().next();
        assertEquals("2.0.0", processor.getBundle().getVersion(), "RESOLVE_BUNDLE should resolve to the single available bundle");
    }

    @Test
    public void testResolveBundlesResolveBundleWithMultipleAvailable() {
        final Bundle unavailableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "0.0.0-NONEXISTENT");
        final Bundle availableBundle1 = createBundle("org.apache.nifi", "nifi-standard-nar", "1.0.0");
        final Bundle availableBundle2 = createBundle("org.apache.nifi", "nifi-standard-nar", "2.0.0");

        final ComponentBundleLookup bundleLookup = mock(ComponentBundleLookup.class);
        when(bundleLookup.getAvailableBundles("org.apache.nifi.processors.standard.GenerateFlowFile")).thenReturn(List.of(availableBundle1, availableBundle2));

        final VersionedProcessGroup group = createProcessGroupWithProcessor("GenerateFlowFile", "org.apache.nifi.processors.standard.GenerateFlowFile", unavailableBundle);

        final StandardConnectorInitializationContext context = createContext(bundleLookup);
        context.resolveBundles(group, BundleCompatibility.RESOLVE_BUNDLE);

        final VersionedProcessor processor = group.getProcessors().iterator().next();
        assertEquals("0.0.0-NONEXISTENT", processor.getBundle().getVersion(), "RESOLVE_BUNDLE should not change bundle when multiple are available");
    }

    @Test
    public void testResolveBundlesResolveBundleWithNoneAvailable() {
        final Bundle unavailableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "0.0.0-NONEXISTENT");

        final ComponentBundleLookup bundleLookup = mock(ComponentBundleLookup.class);
        when(bundleLookup.getAvailableBundles("org.apache.nifi.processors.standard.GenerateFlowFile")).thenReturn(List.of());

        final VersionedProcessGroup group = createProcessGroupWithProcessor("GenerateFlowFile", "org.apache.nifi.processors.standard.GenerateFlowFile", unavailableBundle);

        final StandardConnectorInitializationContext context = createContext(bundleLookup);
        context.resolveBundles(group, BundleCompatibility.RESOLVE_BUNDLE);

        final VersionedProcessor processor = group.getProcessors().iterator().next();
        assertEquals("0.0.0-NONEXISTENT", processor.getBundle().getVersion(), "RESOLVE_BUNDLE should not change bundle when none are available");
    }

    @Test
    public void testResolveBundlesResolveNewestBundle() {
        final Bundle unavailableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "0.0.0-NONEXISTENT");
        final Bundle newestBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "2.0.0");

        final ComponentBundleLookup bundleLookup = mock(ComponentBundleLookup.class);
        when(bundleLookup.getAvailableBundles("org.apache.nifi.processors.standard.GenerateFlowFile")).thenReturn(List.of(createBundle("org.apache.nifi", "nifi-standard-nar", "1.0.0"), newestBundle));
        when(bundleLookup.getLatestBundle("org.apache.nifi.processors.standard.GenerateFlowFile")).thenReturn(Optional.of(newestBundle));

        final VersionedProcessGroup group = createProcessGroupWithProcessor("GenerateFlowFile", "org.apache.nifi.processors.standard.GenerateFlowFile", unavailableBundle);

        final StandardConnectorInitializationContext context = createContext(bundleLookup);
        context.resolveBundles(group, BundleCompatibility.RESOLVE_NEWEST_BUNDLE);

        final VersionedProcessor processor = group.getProcessors().iterator().next();
        assertEquals("2.0.0", processor.getBundle().getVersion(), "RESOLVE_NEWEST_BUNDLE should resolve to the newest available bundle");
    }

    @Test
    public void testResolveBundlesResolveNewestBundleWithNoneAvailable() {
        final Bundle unavailableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "0.0.0-NONEXISTENT");

        final ComponentBundleLookup bundleLookup = mock(ComponentBundleLookup.class);
        when(bundleLookup.getAvailableBundles("org.apache.nifi.processors.standard.GenerateFlowFile"))
            .thenReturn(List.of());

        final VersionedProcessGroup group = createProcessGroupWithProcessor("GenerateFlowFile",
            "org.apache.nifi.processors.standard.GenerateFlowFile", unavailableBundle);

        final StandardConnectorInitializationContext context = createContext(bundleLookup);
        context.resolveBundles(group, BundleCompatibility.RESOLVE_NEWEST_BUNDLE);

        final VersionedProcessor processor = group.getProcessors().iterator().next();
        assertEquals("0.0.0-NONEXISTENT", processor.getBundle().getVersion(), "RESOLVE_NEWEST_BUNDLE should not change bundle when none are available");
    }

    @Test
    public void testResolveBundlesWithControllerService() {
        final Bundle unavailableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "0.0.0-NONEXISTENT");
        final Bundle availableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "2.0.0");

        final ComponentBundleLookup bundleLookup = mock(ComponentBundleLookup.class);
        when(bundleLookup.getAvailableBundles("org.apache.nifi.ssl.StandardSSLContextService"))
            .thenReturn(List.of(availableBundle));

        final VersionedProcessGroup group = createProcessGroupWithControllerService("SSLContextService",
            "org.apache.nifi.ssl.StandardSSLContextService", unavailableBundle);

        final StandardConnectorInitializationContext context = createContext(bundleLookup);
        context.resolveBundles(group, BundleCompatibility.RESOLVE_BUNDLE);

        final VersionedControllerService service = group.getControllerServices().iterator().next();
        assertEquals("2.0.0", service.getBundle().getVersion(), "RESOLVE_BUNDLE should resolve controller service bundle");
    }

    @Test
    public void testResolveBundlesWithNestedProcessGroups() {
        final Bundle unavailableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "0.0.0-NONEXISTENT");
        final Bundle availableBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "2.0.0");

        final ComponentBundleLookup bundleLookup = mock(ComponentBundleLookup.class);
        when(bundleLookup.getAvailableBundles("org.apache.nifi.processors.standard.GenerateFlowFile"))
            .thenReturn(List.of(availableBundle));

        final VersionedProcessGroup childGroup = createProcessGroupWithProcessor("GenerateFlowFile",
            "org.apache.nifi.processors.standard.GenerateFlowFile", unavailableBundle);
        childGroup.setIdentifier("child-group");

        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setIdentifier("root-group");
        rootGroup.setName("Root");
        rootGroup.setProcessors(new HashSet<>());
        rootGroup.setControllerServices(new HashSet<>());
        rootGroup.setProcessGroups(Set.of(childGroup));

        final StandardConnectorInitializationContext context = createContext(bundleLookup);
        context.resolveBundles(rootGroup, BundleCompatibility.RESOLVE_BUNDLE);

        final VersionedProcessor processor = childGroup.getProcessors().iterator().next();
        assertEquals("2.0.0", processor.getBundle().getVersion(), "RESOLVE_BUNDLE should resolve bundles in nested process groups");
    }

    @Test
    public void testResolveBundlesDoesNotChangeAvailableBundle() {
        final Bundle specifiedBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "1.0.0");
        final Bundle newerBundle = createBundle("org.apache.nifi", "nifi-standard-nar", "2.0.0");

        final ComponentBundleLookup bundleLookup = mock(ComponentBundleLookup.class);
        when(bundleLookup.getAvailableBundles("org.apache.nifi.processors.standard.GenerateFlowFile"))
            .thenReturn(List.of(specifiedBundle, newerBundle));

        final VersionedProcessGroup group = createProcessGroupWithProcessor("GenerateFlowFile",
            "org.apache.nifi.processors.standard.GenerateFlowFile", specifiedBundle);

        final StandardConnectorInitializationContext context = createContext(bundleLookup);
        context.resolveBundles(group, BundleCompatibility.RESOLVE_NEWEST_BUNDLE);

        final VersionedProcessor processor = group.getProcessors().iterator().next();
        assertEquals("1.0.0", processor.getBundle().getVersion(), "Should not change bundle when it is already available");
    }

    private Bundle createBundle(final String group, final String artifact, final String version) {
        final Bundle bundle = new Bundle();
        bundle.setGroup(group);
        bundle.setArtifact(artifact);
        bundle.setVersion(version);
        return bundle;
    }

    private VersionedProcessGroup createProcessGroupWithProcessor(final String name, final String type, final Bundle bundle) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier("processor-1");
        processor.setName(name);
        processor.setType(type);
        processor.setBundle(bundle);
        processor.setComponentType(ComponentType.PROCESSOR);
        processor.setProperties(new HashMap<>());
        processor.setPropertyDescriptors(new HashMap<>());

        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setIdentifier("test-group");
        group.setName("Test Group");
        group.setProcessors(new HashSet<>(Set.of(processor)));
        group.setControllerServices(new HashSet<>());
        group.setProcessGroups(new HashSet<>());
        return group;
    }

    private VersionedProcessGroup createProcessGroupWithControllerService(final String name, final String type, final Bundle bundle) {
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier("service-1");
        service.setName(name);
        service.setType(type);
        service.setBundle(bundle);
        service.setComponentType(ComponentType.CONTROLLER_SERVICE);
        service.setProperties(new HashMap<>());
        service.setPropertyDescriptors(new HashMap<>());

        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setIdentifier("test-group");
        group.setName("Test Group");
        group.setProcessors(new HashSet<>());
        group.setControllerServices(new HashSet<>(Set.of(service)));
        group.setProcessGroups(new HashSet<>());
        return group;
    }

    private StandardConnectorInitializationContext createContext(final ComponentBundleLookup bundleLookup) {
        return new StandardConnectorInitializationContext.Builder()
            .identifier("test-connector")
            .name("Test Connector")
            .componentLog(mock(ComponentLog.class))
            .secretsManager(mock(SecretsManager.class))
            .assetManager(mock(AssetManager.class))
            .componentBundleLookup(bundleLookup)
            .build();
    }
}
