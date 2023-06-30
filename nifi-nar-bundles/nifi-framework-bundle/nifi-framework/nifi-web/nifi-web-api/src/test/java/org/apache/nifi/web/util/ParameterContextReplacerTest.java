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
package org.apache.nifi.web.util;

import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class ParameterContextReplacerTest {
    private static final String CONTEXT_ONE_NAME = "contextOne";
    private static final String CONTEXT_TWO_NAME = "contextTwo (1)";
    private static final String CONTEXT_ONE_NAME_AFTER_REPLACE = "contextOne (1)";
    private static final String CONTEXT_TWO_NAME_AFTER_REPLACE = "contextTwo (2)";

    @Test
    public void testReplacementWithoutSubgroups() {
        final ParameterContextNameCollisionResolver collisionResolver = new ParameterContextNameCollisionResolver();
        final ParameterContextReplacer testSubject = new ParameterContextReplacer(collisionResolver);
        final RegisteredFlowSnapshot snapshot = getSimpleSnapshot();

        testSubject.replaceParameterContexts(snapshot, new HashSet<>());

        final Map<String, VersionedParameterContext> parameterContexts = snapshot.getParameterContexts();
        Assertions.assertEquals(1, parameterContexts.size());
        Assertions.assertTrue(parameterContexts.containsKey(CONTEXT_ONE_NAME_AFTER_REPLACE));

        final VersionedParameterContext replacedContext = parameterContexts.get(CONTEXT_ONE_NAME_AFTER_REPLACE);
        final Set<VersionedParameter> parameters = replacedContext.getParameters();
        final Map<String, VersionedParameter> parametersByName = new HashMap<>();

        for (final VersionedParameter parameter : parameters) {
            parametersByName.put(parameter.getName(), parameter);
        }

        Assertions.assertEquals(CONTEXT_ONE_NAME_AFTER_REPLACE, snapshot.getFlowContents().getParameterContextName());
        Assertions.assertEquals("value1", parametersByName.get("param1").getValue());
        Assertions.assertEquals("value2", parametersByName.get("param2").getValue());
    }

    @Test
    public void testReplacementWithSubgroups() {
        final ParameterContextNameCollisionResolver collisionResolver = new ParameterContextNameCollisionResolver();
        final ParameterContextReplacer testSubject = new ParameterContextReplacer(collisionResolver);
        final RegisteredFlowSnapshot snapshot = getMultiLevelSnapshot();

        testSubject.replaceParameterContexts(snapshot, new HashSet<>());

        final Map<String, VersionedParameterContext> parameterContexts = snapshot.getParameterContexts();
        Assertions.assertEquals(2, parameterContexts.size());
        Assertions.assertTrue(parameterContexts.containsKey(CONTEXT_ONE_NAME_AFTER_REPLACE));
        Assertions.assertTrue(parameterContexts.containsKey(CONTEXT_TWO_NAME_AFTER_REPLACE));
    }

    private static RegisteredFlowSnapshot getSimpleSnapshot() {
        final VersionedProcessGroup processGroup = getProcessGroup("PG1", CONTEXT_ONE_NAME);

        final RegisteredFlowSnapshot snapshot = new RegisteredFlowSnapshot();
        snapshot.setParameterContexts(new HashMap<>(Collections.singletonMap(CONTEXT_ONE_NAME, getParameterContextOne())));
        snapshot.setFlowContents(processGroup);

        return snapshot;
    }

    private static RegisteredFlowSnapshot getMultiLevelSnapshot() {
        final VersionedProcessGroup childProcessGroup1 = getProcessGroup("CPG1", CONTEXT_ONE_NAME);
        final VersionedProcessGroup childProcessGroup2 = getProcessGroup("CPG2", CONTEXT_TWO_NAME);
        final VersionedProcessGroup processGroup = getProcessGroup("PG1", CONTEXT_TWO_NAME, childProcessGroup1, childProcessGroup2);

        final Map<String, VersionedParameterContext> parameterContexts = new HashMap<>();
        parameterContexts.put(CONTEXT_ONE_NAME, getParameterContextOne());
        parameterContexts.put(CONTEXT_TWO_NAME, getParameterContextTwo());

        final RegisteredFlowSnapshot snapshot = new RegisteredFlowSnapshot();
        snapshot.setParameterContexts(parameterContexts);
        snapshot.setFlowContents(processGroup);

        return snapshot;
    }

    private static VersionedProcessGroup getProcessGroup(final String name, final String parameterContext, final VersionedProcessGroup... children) {
        final VersionedProcessGroup result = new VersionedProcessGroup();
        result.setName(name);
        result.setIdentifier(name); // Needed for equals check
        result.setParameterContextName(parameterContext);
        result.setProcessGroups(new HashSet<>(Arrays.asList(children)));
        return result;
    }

    private static VersionedParameterContext getParameterContextOne() {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("param1", "value1");
        parameters.put("param2", "value2");
        final VersionedParameterContext context = getParameterContext(CONTEXT_ONE_NAME, parameters);
        return context;
    }

    private static VersionedParameterContext getParameterContextTwo() {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("param3", "value3");
        parameters.put("param4", "value4");
        final VersionedParameterContext context = getParameterContext(CONTEXT_TWO_NAME, parameters);
        return context;
    }

    public static VersionedParameterContext getParameterContext(final String name, final Map<String, String> parameters) {
        final Set<VersionedParameter> contextParameters = new HashSet<>();

        for (final Map.Entry<String, String> parameter : parameters.entrySet()) {
            final VersionedParameter versionedParameter = new VersionedParameter();
            versionedParameter.setName(parameter.getKey());
            versionedParameter.setValue(parameter.getValue());
            contextParameters.add(versionedParameter);
        }

        final VersionedParameterContext context = new VersionedParameterContext();
        context.setName(name);
        context.setParameters(contextParameters);

        return context;
    }
}