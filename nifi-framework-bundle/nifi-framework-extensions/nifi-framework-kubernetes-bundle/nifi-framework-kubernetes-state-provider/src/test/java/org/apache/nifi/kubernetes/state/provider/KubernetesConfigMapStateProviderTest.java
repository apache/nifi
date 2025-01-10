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
package org.apache.nifi.kubernetes.state.provider;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServerExtension;
import io.fabric8.mockwebserver.dsl.HttpMethod;
import io.fabric8.mockwebserver.http.RecordedRequest;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.kubernetes.client.ServiceAccountNamespaceProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(MockitoExtension.class)
@ExtendWith(KubernetesMockServerExtension.class)
class KubernetesConfigMapStateProviderTest {
    private static final String IDENTIFIER = KubernetesConfigMapStateProvider.class.getSimpleName();

    private static final String FIRST_VERSION = "1";

    private static final String SECOND_VERSION = "2";

    private static final String DEFAULT_NAMESPACE = new ServiceAccountNamespaceProvider().getNamespace();

    private static final String COMPONENT_ID = "COMPONENT-ID";

    private static final String STATE_PROPERTY = "started";

    private static final String STATE_PROPERTY_ENCODED = "c3RhcnRlZA";

    private static final String STATE_VALUE = "now";

    private static final String CONFIG_MAP_NAME_PREFIX_VALUE = "label";

    private static final String EMPTY = "";

    @Mock
    StateProviderInitializationContext context;

    @Mock
    ComponentLog logger;

    KubernetesMockServer kubernetesMockServer;

    KubernetesClient kubernetesClient;

    KubernetesConfigMapStateProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new MockKubernetesConfigMapStateProvider();
    }

    @Test
    void testGetSupportedScopes() {
        final Scope[] supportedScopes = provider.getSupportedScopes();

        assertArrayEquals(new Scope[]{Scope.CLUSTER}, supportedScopes);
    }

    @Test
    void testInitializeValidateShutdown() {
        setContextWithConfigMapNamePrefix(EMPTY);

        provider.initialize(context);
        assertEquals(IDENTIFIER, provider.getIdentifier());

        final MockProcessContext processContext = new MockProcessContext(provider);
        processContext.setProperty(KubernetesConfigMapStateProvider.CONFIG_MAP_NAME_PREFIX, EMPTY);
        final MockValidationContext validationContext = new MockValidationContext(processContext, null);
        final Collection<ValidationResult> results = provider.validate(validationContext);

        assertTrue(results.isEmpty());

        provider.shutdown();
    }

    @Test
    void testInitializeEnableDisable() {
        setContext();
        provider.initialize(context);

        assertEquals(IDENTIFIER, provider.getIdentifier());

        assertFalse(provider.isEnabled());

        provider.enable();
        assertTrue(provider.isEnabled());

        provider.disable();
        assertFalse(provider.isEnabled());
    }

    @Test
    void testGetStateNotFound() throws IOException {
        setContext();
        provider.initialize(context);

        final StateMap stateMap = provider.getState(COMPONENT_ID);

        assertTrue(stateMap.toMap().isEmpty());
        assertFalse(stateMap.getStateVersion().isPresent());
    }

    @Test
    void testSetStateGetState() throws IOException {
        setContext();
        provider.initialize(context);

        final Map<String, String> state = Collections.singletonMap(STATE_PROPERTY, STATE_VALUE);

        provider.setState(state, COMPONENT_ID);

        final StateMap stateMap = provider.getState(COMPONENT_ID);

        assertNotNull(stateMap);
        final Map<String, String> stateRetrieved = stateMap.toMap();
        assertEquals(state, stateRetrieved);

        assertConfigMapFound();
    }

    @Test
    void testSetStateOnComponentRemoved() throws IOException {
        setContext();
        provider.initialize(context);

        final Map<String, String> state = Collections.singletonMap(STATE_PROPERTY, STATE_VALUE);

        provider.setState(state, COMPONENT_ID);

        final StateMap stateMap = provider.getState(COMPONENT_ID);
        assertStateEquals(state, stateMap);

        provider.onComponentRemoved(COMPONENT_ID);

        final StateMap removedStateMap = provider.getState(COMPONENT_ID);
        assertStateEquals(Collections.emptyMap(), removedStateMap);
    }

    @Test
    void testClearGetState() throws IOException {
        setContext();
        provider.initialize(context);

        provider.clear(COMPONENT_ID);

        final StateMap stateMap = provider.getState(COMPONENT_ID);

        assertNotNull(stateMap);
        final Map<String, String> stateRetrieved = stateMap.toMap();
        assertTrue(stateRetrieved.isEmpty());
    }

    @Test
    void testReplaceNotFound() throws IOException {
        setContext();
        provider.initialize(context);

        final StateMap stateMap = new StandardStateMap(Collections.emptyMap(), Optional.empty());
        final boolean replaced = provider.replace(stateMap, Collections.emptyMap(), COMPONENT_ID);

        assertTrue(replaced);

        final StateMap replacedStateMap = provider.getState(COMPONENT_ID);
        final Optional<String> replacedVersion = replacedStateMap.getStateVersion();
        assertTrue(replacedVersion.isPresent());
        assertEquals(FIRST_VERSION, replacedVersion.get());
    }

    @Test
    void testSetStateReplace() throws IOException {
        setContext();
        provider.initialize(context);

        final Map<String, String> state = Collections.singletonMap(STATE_PROPERTY, STATE_VALUE);
        provider.setState(state, COMPONENT_ID);

        final StateMap initialStateMap = provider.getState(COMPONENT_ID);
        final Optional<String> initialVersion = initialStateMap.getStateVersion();
        assertTrue(initialVersion.isPresent());
        assertEquals(FIRST_VERSION, initialVersion.get());

        final boolean replaced = provider.replace(initialStateMap, Collections.emptyMap(), COMPONENT_ID);

        assertTrue(replaced);

        final StateMap replacedStateMap = provider.getState(COMPONENT_ID);
        final Optional<String> replacedVersion = replacedStateMap.getStateVersion();
        assertTrue(replacedVersion.isPresent());
        assertEquals(SECOND_VERSION, replacedVersion.get());
    }

    @Test
    void testOnComponentRemovedNotFound() throws IOException, InterruptedException {
        setContext();
        provider.initialize(context);

        provider.onComponentRemoved(COMPONENT_ID);

        final RecordedRequest request = kubernetesMockServer.getLastRequest();
        assertEquals(HttpMethod.DELETE.name(), request.getMethod());
    }

    @Test
    void testSetStateGetStoredComponentIds() throws IOException {
        setContext();
        provider.initialize(context);

        final Collection<String> initialStoredComponentIds = provider.getStoredComponentIds();
        assertTrue(initialStoredComponentIds.isEmpty());

        final Map<String, String> state = Collections.singletonMap(STATE_PROPERTY, STATE_VALUE);
        provider.setState(state, COMPONENT_ID);

        final Collection<String> storedComponentIds = provider.getStoredComponentIds();
        final Iterator<String> componentIds = storedComponentIds.iterator();

        assertTrue(componentIds.hasNext());
        assertEquals(COMPONENT_ID, componentIds.next());
    }

    @Test
    void testReplaceConcurrentCreate() throws IOException {
        setContext();
        provider.initialize(context);

        final StateMap stateMap1 = provider.getState(COMPONENT_ID);
        final StateMap stateMap2 = provider.getState(COMPONENT_ID);

        final boolean replaced1 = provider.replace(stateMap1, Collections.emptyMap(), COMPONENT_ID);

        assertTrue(replaced1);

        final StateMap replacedStateMap = provider.getState(COMPONENT_ID);
        final Optional<String> replacedVersion = replacedStateMap.getStateVersion();
        assertTrue(replacedVersion.isPresent());
        assertEquals(FIRST_VERSION, replacedVersion.get());

        final boolean replaced2 = provider.replace(stateMap2, Collections.emptyMap(), COMPONENT_ID);

        assertFalse(replaced2);
    }

    @Test
    void testReplaceConcurrentUpdate() throws IOException {
        setContext();
        provider.initialize(context);

        provider.setState(Collections.singletonMap("key", "0"), COMPONENT_ID);

        final StateMap stateMap1 = provider.getState(COMPONENT_ID);
        final StateMap stateMap2 = provider.getState(COMPONENT_ID);

        final boolean replaced1 = provider.replace(stateMap1, Collections.singletonMap("key", "1"), COMPONENT_ID);

        assertTrue(replaced1);

        final StateMap replacedStateMap = provider.getState(COMPONENT_ID);
        final Optional<String> replacedVersion = replacedStateMap.getStateVersion();
        assertTrue(replacedVersion.isPresent());
        assertEquals(SECOND_VERSION, replacedVersion.get());

        final boolean replaced2 = provider.replace(stateMap2, Collections.singletonMap("key", "2"), COMPONENT_ID);

        assertFalse(replaced2);
    }

    @Test
    void testSetStateGetStateWithPrefix() throws IOException {
        setContextWithConfigMapNamePrefix(CONFIG_MAP_NAME_PREFIX_VALUE);
        provider.initialize(context);

        final Map<String, String> state = Collections.singletonMap(STATE_PROPERTY, STATE_VALUE);

        provider.setState(state, COMPONENT_ID);

        final StateMap stateMap = provider.getState(COMPONENT_ID);

        assertNotNull(stateMap);
        final Map<String, String> stateRetrieved = stateMap.toMap();
        assertEquals(state, stateRetrieved);

        assertConfigMapFound();
    }

    @Test
    void testSetStateGetStoredComponentIdsWithPrefix() throws IOException {
        setContextWithConfigMapNamePrefix(CONFIG_MAP_NAME_PREFIX_VALUE);
        provider.initialize(context);

        final Collection<String> initialStoredComponentIds = provider.getStoredComponentIds();
        assertTrue(initialStoredComponentIds.isEmpty());

        final Map<String, String> state = Collections.singletonMap(STATE_PROPERTY, STATE_VALUE);
        provider.setState(state, COMPONENT_ID);

        final Collection<String> storedComponentIds = provider.getStoredComponentIds();
        final Iterator<String> componentIds = storedComponentIds.iterator();

        assertTrue(componentIds.hasNext());
        assertEquals(COMPONENT_ID, componentIds.next());
    }

    private void setContext() {
        when(context.getIdentifier()).thenReturn(IDENTIFIER);
        when(context.getLogger()).thenReturn(logger);
        when(context.getProperty(KubernetesConfigMapStateProvider.CONFIG_MAP_NAME_PREFIX))
                .thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
    }

    private void setContextWithConfigMapNamePrefix(final String configMapNamePrefix) {
        setContext();
        when(context.getProperty(KubernetesConfigMapStateProvider.CONFIG_MAP_NAME_PREFIX))
                .thenReturn(new StandardPropertyValue(configMapNamePrefix, null, ParameterLookup.EMPTY));
    }

    private void assertStateEquals(final Map<String, String> expected, final StateMap stateMap) {
        assertNotNull(stateMap);
        final Map<String, String> stateRetrieved = stateMap.toMap();
        assertEquals(expected, stateRetrieved);
    }

    private void assertConfigMapFound() {
        final ConfigMapList configMapList = kubernetesClient.configMaps().inNamespace(DEFAULT_NAMESPACE).list();
        final Optional<ConfigMap> configMapFound = configMapList.getItems()
                .stream()
                .filter(configMap -> configMap.getMetadata().getName().endsWith(COMPONENT_ID))
                .findFirst();
        assertTrue(configMapFound.isPresent());

        final ConfigMap configMap = configMapFound.get();
        final Map<String, String> configMapData = configMap.getData();
        final Map<String, String> expectedData = Collections.singletonMap(STATE_PROPERTY_ENCODED, STATE_VALUE);
        assertEquals(expectedData, configMapData);
    }

    private class MockKubernetesConfigMapStateProvider extends KubernetesConfigMapStateProvider {

        @Override
        protected KubernetesClient getKubernetesClient() {
            return kubernetesClient;
        }
    }
}
