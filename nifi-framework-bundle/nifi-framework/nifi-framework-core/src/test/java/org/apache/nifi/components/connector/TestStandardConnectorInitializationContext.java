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

import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
