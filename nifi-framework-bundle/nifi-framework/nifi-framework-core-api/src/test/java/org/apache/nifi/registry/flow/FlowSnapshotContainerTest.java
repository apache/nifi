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
package org.apache.nifi.registry.flow;

import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowSnapshotContainerTest {

    public static final String CONTEXT_1_NAME = "Context 1";
    public static final String CONTEXT_2_NAME = "Context 2";

    public static final String CONTEXT_1_PARAM_A_NAME = "Context 1 - Param A";
    public static final String CONTEXT_1_PARAM_B_NAME = "Context 1 - Param B";
    public static final String CONTEXT_1_PARAM_C_NAME = "Context 1 - Param C";
    public static final String CONTEXT_2_PARAM_D_NAME = "Context 2 - Param D";

    public static final String PROVIDER_1_ID = "provider1";
    public static final String PROVIDER_2_ID = "provider2";
    public static final String PROVIDER_3_ID = "provider3";

    private FlowSnapshotContainer flowSnapshotContainer;

    @BeforeEach
    public void setup() {
        final VersionedParameter parameterA = createParameter(CONTEXT_1_PARAM_A_NAME);
        final VersionedParameter parameterB = createParameter(CONTEXT_1_PARAM_B_NAME);
        final VersionedParameterContext context1 = createContext(CONTEXT_1_NAME, parameterA, parameterB);

        final Map<String, VersionedParameterContext> topLevelParamContexts = new HashMap<>();
        topLevelParamContexts.put(context1.getName(), context1);

        final ParameterProviderReference providerReference1 = createProviderReference(PROVIDER_1_ID, "Provider 1");
        final ParameterProviderReference providerReference2 = createProviderReference(PROVIDER_2_ID, "Provider 2");

        final Map<String, ParameterProviderReference> topLevelProviderReferences = new HashMap<>();
        topLevelProviderReferences.put(providerReference1.getIdentifier(), providerReference1);
        topLevelProviderReferences.put(providerReference2.getIdentifier(), providerReference2);

        final RegisteredFlowSnapshot topLevelSnapshot = new RegisteredFlowSnapshot();
        topLevelSnapshot.setFlowContents(new VersionedProcessGroup());
        topLevelSnapshot.setParameterContexts(topLevelParamContexts);
        topLevelSnapshot.setParameterProviders(topLevelProviderReferences);

        flowSnapshotContainer = new FlowSnapshotContainer(topLevelSnapshot);
    }

    @Test
    public void testAddChildSnapshot() {
        // Child has same context with an additional parameter
        final VersionedParameter parameterC = createParameter(CONTEXT_1_PARAM_C_NAME);
        final VersionedParameterContext childContext1 = createContext(CONTEXT_1_NAME, parameterC);

        // Child has an additional context that didn't exist in parent
        final VersionedParameter parameterD = createParameter(CONTEXT_2_PARAM_D_NAME);
        final VersionedParameterContext childContext2 = createContext(CONTEXT_2_NAME, parameterD);

        final Map<String, VersionedParameterContext> childParamContexts = new HashMap<>();
        childParamContexts.put(childContext1.getName(), childContext1);
        childParamContexts.put(childContext2.getName(), childContext2);

        // Child has additional provider reference
        final ParameterProviderReference providerReference3 = createProviderReference(PROVIDER_3_ID, "Provider 3");

        final Map<String, ParameterProviderReference> childProviderReferences = new HashMap<>();
        childProviderReferences.put(providerReference3.getIdentifier(), providerReference3);

        // Setup child snapshot
        final RegisteredFlowSnapshot childSnapshot = new RegisteredFlowSnapshot();
        childSnapshot.setFlowContents(new VersionedProcessGroup());
        childSnapshot.setParameterContexts(childParamContexts);
        childSnapshot.setParameterProviders(childProviderReferences);

        // Add to child to container
        final VersionedProcessGroup destGroup = new VersionedProcessGroup();
        destGroup.setIdentifier(UUID.randomUUID().toString());
        flowSnapshotContainer.addChildSnapshot(childSnapshot, destGroup);

        // Verify get by group id
        final RegisteredFlowSnapshot retrievedChildSnapshot = flowSnapshotContainer.getChildSnapshot(destGroup.getIdentifier());
        assertEquals(childSnapshot, retrievedChildSnapshot);

        // Verify additional context was added
        final Map<String, VersionedParameterContext> topLevelParamContexts = flowSnapshotContainer.getFlowSnapshot().getParameterContexts();
        assertEquals(2, topLevelParamContexts.size());
        assertTrue(topLevelParamContexts.containsKey(CONTEXT_1_NAME));
        assertTrue(topLevelParamContexts.containsKey(CONTEXT_2_NAME));

        // Verify additional parameters added to context 1
        final VersionedParameterContext context1 = topLevelParamContexts.get(CONTEXT_1_NAME);
        final Set<VersionedParameter> context1Parameters = context1.getParameters();
        assertEquals(3, context1Parameters.size());
        assertNotNull(context1Parameters.stream().filter(p -> p.getName().equals(CONTEXT_1_PARAM_A_NAME)).findFirst().orElse(null));
        assertNotNull(context1Parameters.stream().filter(p -> p.getName().equals(CONTEXT_1_PARAM_B_NAME)).findFirst().orElse(null));
        assertNotNull(context1Parameters.stream().filter(p -> p.getName().equals(CONTEXT_1_PARAM_C_NAME)).findFirst().orElse(null));

        // Verify additional provider added
        final Map<String, ParameterProviderReference> topLevelProviders = flowSnapshotContainer.getFlowSnapshot().getParameterProviders();
        assertEquals(3, topLevelProviders.size());
        assertTrue(topLevelProviders.containsKey(PROVIDER_1_ID));
        assertTrue(topLevelProviders.containsKey(PROVIDER_2_ID));
        assertTrue(topLevelProviders.containsKey(PROVIDER_3_ID));
    }

    private VersionedParameter createParameter(final String name) {
        final VersionedParameter parameter = new VersionedParameter();
        parameter.setName(name);
        return parameter;
    }

    private VersionedParameterContext createContext(final String name, final VersionedParameter ... parameters) {
        final VersionedParameterContext paramContext = new VersionedParameterContext();
        paramContext.setName(name);
        paramContext.setParameters(new HashSet<>(Arrays.asList(parameters)));
        return paramContext;
    }

    private ParameterProviderReference createProviderReference(final String id, final String name) {
        final ParameterProviderReference reference = new ParameterProviderReference();
        reference.setIdentifier(id);
        reference.setName(name);
        return reference;
    }
}
