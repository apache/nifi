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
package org.apache.nifi.parameter;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestStandardParameterContext {

    @Test
    public void testUpdatesApply() {
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();

        final StandardParameterContext context = new StandardParameterContext.Builder()
                .id("unit-test-context")
                .name("unit-test-context")
                .parameterReferenceManager(referenceManager)
                .build();

        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").build();
        final ParameterDescriptor xyzDescriptor = new ParameterDescriptor.Builder().name("xyz").build();
        final ParameterDescriptor fooDescriptor = new ParameterDescriptor.Builder().name("foo").description("bar").sensitive(true).build();

        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("abc", createParameter(abcDescriptor, "123"));
        parameters.put("xyz", createParameter(xyzDescriptor, "242526"));

        context.setParameters(parameters);

        final Parameter abcParam = context.getParameter("abc").get();
        assertEquals(abcDescriptor, abcParam.getDescriptor());
        assertNull(abcParam.getDescriptor().getDescription());
        assertEquals("123", abcParam.getValue());

        final Parameter xyzParam = context.getParameter("xyz").get();
        assertEquals(xyzDescriptor, xyzParam.getDescriptor());
        assertNull(xyzParam.getDescriptor().getDescription());
        assertEquals("242526", xyzParam.getValue());

        final Map<String, Parameter> secondParameters = new HashMap<>();
        secondParameters.put("foo", createParameter(fooDescriptor, "baz"));
        context.setParameters(secondParameters);

        assertTrue(context.getParameter("abc").isPresent());
        assertTrue(context.getParameter("xyz").isPresent());

        secondParameters.put("abc", null);
        secondParameters.put("xyz", null);

        context.setParameters(secondParameters);

        assertFalse(context.getParameter("abc").isPresent());
        assertFalse(context.getParameter("xyz").isPresent());

        final Parameter fooParam = context.getParameter(fooDescriptor).get();
        assertEquals(fooDescriptor, fooParam.getDescriptor());
        assertTrue(fooParam.getDescriptor().isSensitive());
        assertEquals("bar", fooParam.getDescriptor().getDescription());
        assertEquals("baz", fooParam.getValue());

        assertEquals(Collections.singletonMap(fooDescriptor, fooParam), context.getParameters());

        final Map<String, Parameter> thirdParameters = new HashMap<>();
        thirdParameters.put("foo", createParameter(fooDescriptor, "other"));
        context.setParameters(thirdParameters);

        assertEquals("other", context.getParameter("foo").get().getValue());
    }

    private static Parameter createParameter(final ParameterDescriptor descriptor, final String value) {
        return createParameter(descriptor, value, false);
    }

    private static Parameter createParameter(final ParameterDescriptor descriptor, final String value, final boolean provided) {
        return new Parameter.Builder().descriptor(descriptor).value(value).provided(provided).build();
    }

    @Test
    public void testUpdateDescription() {
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContext context = new StandardParameterContext.Builder()
                .id("unit-test-context")
                .name("unit-test-context")
                .parameterReferenceManager(referenceManager)
                .build();
        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").description("abc").build();

        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("abc", createParameter(abcDescriptor, "123"));

        context.setParameters(parameters);

        Parameter abcParam = context.getParameter("abc").get();
        assertEquals(abcDescriptor, abcParam.getDescriptor());
        assertEquals("abc", abcParam.getDescriptor().getDescription());
        assertEquals("123", abcParam.getValue());

        ParameterDescriptor updatedDescriptor = new ParameterDescriptor.Builder().name("abc").description("Updated").build();
        final Parameter newDescriptionParam = createParameter(updatedDescriptor, "321");
        context.setParameters(Collections.singletonMap("abc", newDescriptionParam));

        abcParam = context.getParameter("abc").get();
        assertEquals(abcDescriptor, abcParam.getDescriptor());
        assertEquals("Updated", abcParam.getDescriptor().getDescription());
        assertEquals("321", abcParam.getValue());

        updatedDescriptor = new ParameterDescriptor.Builder().name("abc").description("Updated Again").build();
        final Parameter paramWithoutValue = createParameter(updatedDescriptor, null);
        context.setParameters(Collections.singletonMap("abc", paramWithoutValue));

        abcParam = context.getParameter("abc").get();
        assertEquals(abcDescriptor, abcParam.getDescriptor());
        assertEquals("Updated Again", abcParam.getDescriptor().getDescription());
        assertNull(abcParam.getValue());
    }

    @Test
    public void testUpdateSensitivity() {
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContext context = new StandardParameterContext.Builder()
                .id("unit-test-context")
                .name("unit-test-context")
                .parameterReferenceManager(referenceManager)
                .build();
        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").description("abc").build();

        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("abc", createParameter(abcDescriptor, "123", true));

        context.setParameters(parameters);

        Parameter abcParam = context.getParameter("abc").get();
        assertEquals(abcDescriptor, abcParam.getDescriptor());
        assertEquals("abc", abcParam.getDescriptor().getDescription());
        assertEquals("123", abcParam.getValue());

        ParameterDescriptor updatedDescriptor = new ParameterDescriptor.Builder().name("abc").description("abc").sensitive(true).build();
        final Parameter unprovidedParam = createParameter(updatedDescriptor, "321", false);
        assertThrows(IllegalStateException.class, () -> context.setParameters(Collections.singletonMap("abc", unprovidedParam)));

        final Parameter newSensitivityParam = createParameter(updatedDescriptor, "321", true);
        context.setParameters(Collections.singletonMap("abc", newSensitivityParam));

        abcParam = context.getParameter("abc").get();
        assertEquals(abcDescriptor, abcParam.getDescriptor());
        assertTrue(abcParam.getDescriptor().isSensitive());

        context.getParameters().keySet().forEach(pd -> {
            assertTrue(pd.isSensitive());
        });
    }

    @Test
    public void testChangeDescription() {
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContext context = new StandardParameterContext.Builder()
                .id("unit-test-context")
                .name("unit-test-context")
                .parameterReferenceManager(referenceManager)
                .build();
        final ParameterDescriptor xyzDescriptor = new ParameterDescriptor.Builder().name("xyz").build();
        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("xyz", createParameter(xyzDescriptor, "123"));
        context.setParameters(parameters);

        final Map<String, Parameter> updates = new HashMap<>();
        final ParameterDescriptor xyzDescriptor2 = new ParameterDescriptor.Builder().from(xyzDescriptor).description("changed").build();
        final Parameter updatedParameter = createParameter(xyzDescriptor2, "123");
        updates.put("xyz", updatedParameter);
        assertEquals(1, context.getEffectiveParameterUpdates(updates, Collections.emptyList()).size());

        // Now there is no change, since the description is the same
        final Map<String, Parameter> updates2 = new HashMap<>();
        final ParameterDescriptor xyzDescriptor3 = new ParameterDescriptor.Builder().from(xyzDescriptor).description("changed").build();
        final Parameter updatedParameter2 = createParameter(xyzDescriptor3, "123");
        updates.put("xyz", updatedParameter2);
        assertEquals(0, context.getEffectiveParameterUpdates(updates2, Collections.emptyList()).size());
    }

    @Test
    public void testChangingSensitivity() {
        // Ensure no changes applied
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContext context = new StandardParameterContext.Builder()
                .id("unit-test-context")
                .name("unit-test-context")
                .parameterReferenceManager(referenceManager)
                .build();
        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").sensitive(true).build();
        final ParameterDescriptor xyzDescriptor = new ParameterDescriptor.Builder().name("xyz").build();
        final ParameterDescriptor fooDescriptor = new ParameterDescriptor.Builder().name("foo").description("bar").sensitive(true).build();

        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("abc", createParameter(abcDescriptor, "123"));
        parameters.put("xyz", createParameter(xyzDescriptor, "242526"));

        context.setParameters(parameters);

        final ParameterDescriptor sensitiveXyzDescriptor = new ParameterDescriptor.Builder().name("xyz").sensitive(true).build();

        final Map<String, Parameter> updatedParameters = new HashMap<>();
        updatedParameters.put("foo", createParameter(fooDescriptor, "baz"));
        updatedParameters.put("xyz", createParameter(sensitiveXyzDescriptor, "242526"));

        assertThrows(IllegalStateException.class,
                () -> context.setParameters(updatedParameters));

        final ParameterDescriptor insensitiveAbcDescriptor = new ParameterDescriptor.Builder().name("abc").sensitive(false).build();
        updatedParameters.clear();
        updatedParameters.put("abc", createParameter(insensitiveAbcDescriptor, "123"));

        assertThrows(IllegalStateException.class,
                () -> context.setParameters(updatedParameters));
    }

    @Test
    public void testChangingParameterForRunningProcessor() {
        final HashMapParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final ParameterContext context = createStandardParameterContext(referenceManager);
        final ProcessorNode procNode = getProcessorNode("abc", referenceManager);

        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").sensitive(true).build();

        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("abc", createParameter(abcDescriptor, "123"));

        context.setParameters(parameters);

        parameters.clear();
        parameters.put("abc", createParameter(abcDescriptor, "321"));
        context.setParameters(parameters);

        assertEquals("321", context.getParameter("abc").get().getValue());

        // Make processor 'running'
        startProcessor(procNode);

        parameters.clear();
        parameters.put("abc", createParameter(abcDescriptor, "123"));

        // Cannot update parameters while running
        assertThrows(IllegalStateException.class, () -> context.setParameters(parameters));

        // This passes no parameters to update, so it should be fine
        context.setParameters(Collections.emptyMap());

        parameters.clear();
        parameters.put("abc", createParameter(abcDescriptor, null));

        assertThrows(IllegalStateException.class,
                () -> context.setParameters(parameters));

        assertEquals("321", context.getParameter("abc").get().getValue());
    }

    @Test
    public void testChangingNestedParameterForRunningProcessor() {
        final String inheritedParamName = "def";
        final String originalValue = "123";
        final String changedValue = "321";

        final HashMapParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();
        final ParameterContext a = createParameterContext("a", parameterContextLookup, referenceManager);
        addParameter(a, "abc", "123");

        final ParameterContext b = createParameterContext("b", parameterContextLookup, referenceManager);
        addParameter(b, inheritedParamName, originalValue);

        a.setInheritedParameterContexts(Arrays.asList(b));

        // Structure is now:
        // Param context A
        //   Param abc
        //   (Inherited) Param def (from B)

        // Processor references param 'def'
        final ProcessorNode procNode = getProcessorNode(inheritedParamName, referenceManager);

        // Show that inherited param 'def' starts with the original value from B
        assertEquals(originalValue, a.getParameter(inheritedParamName).get().getValue());

        // Now demonstrate that we can't effectively add the parameter by referencing Context B while processor runs
        a.setInheritedParameterContexts(Collections.emptyList()); // A now no longer includes 'def'
        startProcessor(procNode);
        try {
            a.setInheritedParameterContexts(Arrays.asList(b));
            fail("Was able to change effective parameter while referencing processor was running");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("def"));
        }

        // Safely add Context B, and show we can't effectively remove 'def' while processor runs
        stopProcessor(procNode);
        a.setInheritedParameterContexts(Arrays.asList(b));
        startProcessor(procNode);

        IllegalStateException illegalStateException =
                assertThrows(IllegalStateException.class,
                        () -> a.setInheritedParameterContexts(Collections.emptyList()));
        assertTrue(illegalStateException.getMessage().contains("def"));

        // Show we can't effectively change the value by changing it in B
        illegalStateException = assertThrows(IllegalStateException.class,
                        () -> a.setInheritedParameterContexts(Collections.emptyList()));
        assertTrue(illegalStateException.getMessage().contains("def"));
        assertEquals(originalValue, a.getParameter(inheritedParamName).get().getValue());

        // Show we can't effectively change the value by adding Context C with 'def' ahead of 'B'
        stopProcessor(procNode);
        final ParameterContext c = createParameterContext("c", parameterContextLookup, referenceManager);
        addParameter(c, inheritedParamName, changedValue);
        startProcessor(procNode);

        illegalStateException = assertThrows(IllegalStateException.class,
                () ->  a.setInheritedParameterContexts(Arrays.asList(c, b)));
        assertTrue(illegalStateException.getMessage().contains("def"));
        assertEquals(originalValue, a.getParameter(inheritedParamName).get().getValue());

        // Show that if the effective value of 'def' doesn't change, we don't prevent updating
        // ParameterContext references that refer to 'def'
        a.setInheritedParameterContexts(Arrays.asList(b, c));
        assertEquals(originalValue, a.getParameter(inheritedParamName).get().getValue());

        stopProcessor(procNode);
        removeParameter(b, inheritedParamName);
        b.setInheritedParameterContexts(Collections.singletonList(c));
        // Now a gets 'def' by inheriting through B and then C.

        // Show that updating a value on a grandchild is prevented because the processor is running and
        // references the parameter via the grandparent
        startProcessor(procNode);
        assertThrows(IllegalStateException.class, () -> removeParameter(c, inheritedParamName));
    }

    private static ProcessorNode getProcessorNode(String parameterName, HashMapParameterReferenceManager referenceManager) {
        final ProcessorNode procNode = Mockito.mock(ProcessorNode.class);
        Mockito.when(procNode.isRunning()).thenReturn(false);
        referenceManager.addProcessorReference(parameterName, procNode);
        return procNode;
    }

    private static void startProcessor(final ProcessorNode processorNode) {
        setProcessorRunning(processorNode, true);
    }

    private static void stopProcessor(final ProcessorNode processorNode) {
        setProcessorRunning(processorNode, false);
    }

    private static void setProcessorRunning(final ProcessorNode processorNode, final boolean isRunning) {
        Mockito.when(processorNode.isRunning()).thenReturn(isRunning);
    }

    private static void setControllerServiceState(final ControllerServiceNode serviceNode, final ControllerServiceState state) {
        Mockito.when(serviceNode.getState()).thenReturn(state);
    }

    private static void enableControllerService(final ControllerServiceNode serviceNode) {
        setControllerServiceState(serviceNode, ControllerServiceState.ENABLED);
    }

    @Test
    public void testAlertReferencingComponents() {
        final String inheritedParamName = "def";
        final String originalValue = "123";

        final HashMapParameterReferenceManager referenceManager = Mockito.spy(new HashMapParameterReferenceManager());
        final Set<ProcessGroup> processGroups = new HashSet<>();
        final ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);
        processGroups.add(processGroup);
        Mockito.when(referenceManager.getProcessGroupsBound(ArgumentMatchers.any())).thenReturn(processGroups);
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();
        final ParameterContext a = createParameterContext("a", parameterContextLookup, referenceManager);
        addParameter(a, "abc", "123");

        final ParameterContext b = createParameterContext("b", parameterContextLookup, referenceManager);
        addParameter(b, inheritedParamName, originalValue);

        getProcessorNode(inheritedParamName, referenceManager);

        a.setInheritedParameterContexts(Arrays.asList(b));

        // Once for setting abc, once for setting def, and once for adding B to context A
        Mockito.verify(processGroup, Mockito.times(3)).onParameterContextUpdated(ArgumentMatchers.anyMap());
    }

    @Test
    public void testChangingNestedParameterForEnabledControllerService() {
        final String inheritedParamName = "def";
        final String inheritedParamName2 = "ghi";
        final String originalValue = "123";
        final String changedValue = "321";

        final HashMapParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();
        final ParameterContext a = createParameterContext("a", parameterContextLookup, referenceManager);
        addParameter(a, "abc", "123");

        final ParameterContext b = createParameterContext("b", parameterContextLookup, referenceManager);
        addParameter(b, inheritedParamName, originalValue);

        a.setInheritedParameterContexts(Arrays.asList(b));

        final ParameterContext c = createParameterContext("c", parameterContextLookup, referenceManager);
        addParameter(c, "ghi", originalValue);

        // Structure is now:
        // Param context A
        //   Param abc
        //   (Inherited) Param def (from B)

        final ControllerServiceNode serviceNode = Mockito.mock(ControllerServiceNode.class);
        enableControllerService(serviceNode);

        referenceManager.addControllerServiceReference(inheritedParamName, serviceNode);
        referenceManager.addControllerServiceReference(inheritedParamName2, serviceNode);

        for (final ControllerServiceState state : EnumSet.of(ControllerServiceState.ENABLED, ControllerServiceState.ENABLING, ControllerServiceState.DISABLING)) {
            setControllerServiceState(serviceNode, state);

            assertThrows(IllegalStateException.class, () -> addParameter(b, inheritedParamName, changedValue));

            assertThrows(IllegalStateException.class, () -> b.setInheritedParameterContexts(Collections.singletonList(c)));

            assertEquals(originalValue, a.getParameter(inheritedParamName).get().getValue());
        }

        assertThrows(IllegalStateException.class, () -> removeParameter(b, inheritedParamName));
        setControllerServiceState(serviceNode, ControllerServiceState.DISABLED);

        b.setInheritedParameterContexts(Collections.singletonList(c));

        setControllerServiceState(serviceNode, ControllerServiceState.DISABLING);

        assertThrows(IllegalStateException.class, () -> b.setInheritedParameterContexts(Collections.emptyList()));
    }

    @Test
    public void testChangingParameterForEnabledControllerService() {
        final HashMapParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final ParameterContext context = createStandardParameterContext(referenceManager);
        final ControllerServiceNode serviceNode = Mockito.mock(ControllerServiceNode.class);
        enableControllerService(serviceNode);

        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").sensitive(true).build();
        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("abc", createParameter(abcDescriptor, "123"));

        context.setParameters(parameters);

        referenceManager.addControllerServiceReference("abc", serviceNode);

        parameters.clear();
        parameters.put("abc", createParameter(abcDescriptor, "321"));

        for (final ControllerServiceState state : EnumSet.of(ControllerServiceState.ENABLED, ControllerServiceState.ENABLING, ControllerServiceState.DISABLING)) {
            setControllerServiceState(serviceNode, state);

            try {
                context.setParameters(parameters);
                fail("Was able to update parameter being referenced by Controller Service that is " + state);
            } catch (final IllegalStateException expected) {
            }

            assertEquals("123", context.getParameter("abc").get().getValue());
        }

        parameters.clear();
        context.setParameters(parameters);

        parameters.put("abc", createParameter(abcDescriptor, null));
        try {
            context.setParameters(parameters);
            fail("Was able to remove parameter being referenced by Controller Service that is DISABLING");
        } catch (final IllegalStateException expected) {
        }
    }

    private ParameterContext createStandardParameterContext(final ParameterReferenceManager referenceManager) {
        return new StandardParameterContext.Builder()
                .id("unit-test-context")
                .name("unit-test-context")
                .parameterReferenceManager(referenceManager)
                .build();
    }

    @Test
    public void testSetParameterContexts_foundCycle() {
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();
        // Set up a hierarchy as follows:
        //       a
        //     /  |
        //    b   c
        //   / |
        //  d  e
        //  |
        //  a (cyclical)
        //
        final ParameterContext a = createParameterContext("a", parameterContextLookup);
        final ParameterContext b = createParameterContext("b", parameterContextLookup);
        final ParameterContext c = createParameterContext("c", parameterContextLookup);
        final ParameterContext d = createParameterContext("d", parameterContextLookup, a); // Here's the cycle
        final ParameterContext e = createParameterContext("e", parameterContextLookup);

        b.setInheritedParameterContexts(Arrays.asList(d, e));

        assertThrows(IllegalStateException.class, () -> a.setInheritedParameterContexts(Arrays.asList(b, c)));
    }

    @Test
    public void testSetParameterContexts_duplicationButNoCycle() {
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();
        // Set up a hierarchy as follows:
        //       a
        //     /  |
        //    b   c
        //   / |
        //  d  e
        //  |
        //  c (duplicate node, but not a cycle)
        //
        final ParameterContext a = createParameterContext("a", parameterContextLookup);
        final ParameterContext b = createParameterContext("b", parameterContextLookup);
        final ParameterContext c = createParameterContext("c", parameterContextLookup);
        final ParameterContext d = createParameterContext("d", parameterContextLookup, c); // Here's the duplicate
        final ParameterContext e = createParameterContext("e", parameterContextLookup);

        b.setInheritedParameterContexts(Arrays.asList(d, e));
        a.setInheritedParameterContexts(Arrays.asList(b, c));
    }

    @Test
    public void testSetParameterContexts_success() {
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();
        final ParameterContext a = createParameterContext("a", parameterContextLookup);
        final ParameterContext b = createParameterContext("b", parameterContextLookup);
        final ParameterContext c = createParameterContext("c", parameterContextLookup);
        final ParameterContext d = createParameterContext("d", parameterContextLookup);
        final ParameterContext e = createParameterContext("e", parameterContextLookup);
        final ParameterContext f = createParameterContext("f", parameterContextLookup);

        b.setInheritedParameterContexts(Arrays.asList(d, e));
        d.setInheritedParameterContexts(Arrays.asList(f));

        a.setInheritedParameterContexts(Arrays.asList(b, c));
        assertEquals(Arrays.asList(b, c), a.getInheritedParameterContexts());

        assertArrayEquals(new String[] {"B", "C"}, a.getInheritedParameterContextNames().toArray());
    }

    @Test
    public void testGetEffectiveParameters() {
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();
        // Set up a hierarchy as follows:
        //       a
        //     /  |
        //    b   c
        //    |
        //    d
        //
        // Parameter priority should be: a, b, c, d
        final ParameterContext a = createParameterContext("a", parameterContextLookup);
        final ParameterDescriptor foo = addParameter(a, "foo", "a.foo"); // Should take precedence over all other foo params
        final ParameterDescriptor bar = addParameter(a, "bar", "a.bar"); // Should take precedence over all other foo params

        final ParameterContext b = createParameterContext("b", parameterContextLookup);
        addParameter(b, "foo", "b.foo");      // Overridden by a.foo since a is the parent
        final ParameterDescriptor child = addParameter(b, "child", "b.child");

        final ParameterContext c = createParameterContext("c", parameterContextLookup);
        addParameter(c, "foo", "c.foo");     // Overridden by a.foo since a is the parent
        addParameter(c, "child", "c.child"); // Overridden by b.child since b comes first in the list
        final ParameterDescriptor secondChild = addParameter(c, "secondChild", "c.secondChild");

        final ParameterContext d = createParameterContext("d", parameterContextLookup);
        addParameter(d, "foo", "d.foo");     // Overridden by a.foo since a is the grandparent
        addParameter(d, "child", "d.child"); // Overridden by b.foo since b is the parent
        final ParameterDescriptor grandchild = addParameter(d, "grandchild", "d.grandchild");

        a.setInheritedParameterContexts(Arrays.asList(b, c));
        b.setInheritedParameterContexts(Arrays.asList(d));

        final Map<ParameterDescriptor, Parameter> effectiveParameters = a.getEffectiveParameters();

        assertEquals(5, effectiveParameters.size());

        assertEquals("a.foo", effectiveParameters.get(foo).getValue());
        assertEquals("a", effectiveParameters.get(foo).getParameterContextId());

        assertEquals("a.bar", effectiveParameters.get(bar).getValue());
        assertEquals("a", effectiveParameters.get(bar).getParameterContextId());

        assertEquals("b.child", effectiveParameters.get(child).getValue());
        assertEquals("b", effectiveParameters.get(child).getParameterContextId());

        assertEquals("c.secondChild", effectiveParameters.get(secondChild).getValue());
        assertEquals("c", effectiveParameters.get(secondChild).getParameterContextId());

        assertEquals("d.grandchild", effectiveParameters.get(grandchild).getValue());
        assertEquals("d", effectiveParameters.get(grandchild).getParameterContextId());
    }

    @Test
    public void testGetEffectiveParameters_duplicateOverride() {
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();
        // Set up a hierarchy as follows:
        //       a
        //     /  |
        //    c   b
        //        |
        //        d
        //        |
        //        c
        //
        // Parameter priority should be: a, c, b, d
        final ParameterContext a = createParameterContext("a", parameterContextLookup);

        final ParameterContext b = createParameterContext("b", parameterContextLookup);
        final ParameterDescriptor child = addParameter(b, "child", "b.child"); // Overridden by c.child since c comes first in the list

        final ParameterContext c = createParameterContext("c", parameterContextLookup);
        addParameter(c, "child", "c.child");

        final ParameterContext d = createParameterContext("d", parameterContextLookup);
        addParameter(d, "child", "d.child"); // Overridden by c.foo since c precedes d's ancestor b

        a.setInheritedParameterContexts(Arrays.asList(c, b));
        b.setInheritedParameterContexts(Arrays.asList(d));
        d.setInheritedParameterContexts(Arrays.asList(c));

        final Map<ParameterDescriptor, Parameter> effectiveParameters = a.getEffectiveParameters();

        assertEquals(1, effectiveParameters.size());

        assertEquals("c.child", effectiveParameters.get(child).getValue());
        assertEquals("c", effectiveParameters.get(child).getParameterContextId());
    }

    @Test
    public void testSetParameterContexts_noParameterConflict() {
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();

        final ParameterContext a = createParameterContext("a", parameterContextLookup);
        addParameter(a, "foo", "a.foo", true);
        addParameter(a, "bar", "a.bar", false);

        final ParameterContext b = createParameterContext("b", parameterContextLookup);
        addParameter(b, "foo", "b.foo", true);  // Sensitivity matches, no conflict
        addParameter(b, "child", "b.child", false);

        a.setInheritedParameterContexts(Arrays.asList(b));
        assertEquals(Arrays.asList(b), a.getInheritedParameterContexts());
    }

    @Test
    public void testInheritsFrom() {
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();

        final ParameterContext a = createParameterContext("a", parameterContextLookup);
        final ParameterContext b = createParameterContext("b", parameterContextLookup);
        final ParameterContext c = createParameterContext("c", parameterContextLookup);
        final ParameterContext d = createParameterContext("d", parameterContextLookup);
        final ParameterContext e = createParameterContext("e", parameterContextLookup);

        a.setInheritedParameterContexts(Arrays.asList(b));
        b.setInheritedParameterContexts(Arrays.asList(c, d));
        d.setInheritedParameterContexts(Arrays.asList(e));

        assertTrue(a.inheritsFrom("b"));
        assertTrue(a.inheritsFrom("c"));
        assertTrue(a.inheritsFrom("d"));
        assertTrue(a.inheritsFrom("e"));
        assertFalse(a.inheritsFrom("a"));

        assertTrue(b.inheritsFrom("e"));
        assertFalse(b.inheritsFrom("a"));

    }

    @Test
    public void testSetParameterContexts_parameterSensitivityConflict() {
        final StandardParameterContextManager parameterContextLookup = new StandardParameterContextManager();

        final ParameterContext a = createParameterContext("a", parameterContextLookup);
        addParameter(a, "foo", "a.foo", true);
        addParameter(a, "bar", "a.bar", false);

        final ParameterContext b = createParameterContext("b", parameterContextLookup);
        addParameter(b, "foo", "b.foo", false);  // Sensitivity does not match!
        addParameter(b, "child", "b.child", false);

        try {
            a.setInheritedParameterContexts(Arrays.asList(b));
            fail("Should get a failure for sensitivity mismatch in overriding");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("foo"));
        }
        assertEquals(Collections.emptyList(), a.getInheritedParameterContexts());

        // Now switch and set a.foo to non-sensitive and b.foo to sensitive
        removeParameter(a, "foo");
        addParameter(a, "foo", "a.foo", false);

        removeParameter(b, "foo");
        addParameter(b, "foo", "a.foo", true);

        try {
            a.setInheritedParameterContexts(Arrays.asList(b));
            fail("Should get a failure for sensitivity mismatch in overriding");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("foo"));
        }
        assertEquals(Collections.emptyList(), a.getInheritedParameterContexts());
    }

    private static void removeParameter(final ParameterContext parameterContext, final String name) {
        final Map<String, Parameter> parameters = new HashMap<>();
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : parameterContext.getParameters().entrySet()) {
            if (entry.getKey().getName().equals(name)) {
                parameters.put(name, null);
            } else {
                parameters.put(entry.getKey().getName(), entry.getValue());
            }
        }
        parameterContext.setParameters(parameters);
    }

    private static ParameterDescriptor addParameter(final ParameterContext parameterContext, final String name, final String value) {
        return addParameter(parameterContext, name, value, false);
    }

    private static ParameterDescriptor addParameter(final ParameterContext parameterContext, final String name, final String value, final boolean isSensitive) {
        final Map<String, Parameter> parameters = new HashMap<>();
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : parameterContext.getParameters().entrySet()) {
            parameters.put(entry.getKey().getName(), entry.getValue());
        }
        final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder().name(name).sensitive(isSensitive).build();
        parameters.put(name, createParameter(parameterDescriptor, value));
        parameterContext.setParameters(parameters);
        return parameterDescriptor;
    }

    private static ParameterContext createParameterContext(final String id, final ParameterContextManager parameterContextLookup,
                                                           final ParameterContext... children) {
        return createParameterContext(id, parameterContextLookup, ParameterReferenceManager.EMPTY, children);
    }

    private static ParameterContext createParameterContext(final String id, final ParameterContextManager parameterContextLookup,
                                                           final ParameterReferenceManager referenceManager, final ParameterContext... children) {
        final ParameterContext parameterContext = new StandardParameterContext.Builder().id(id).name(id.toUpperCase()).parameterReferenceManager(referenceManager).build();
        parameterContext.setInheritedParameterContexts(Arrays.asList(children));

        parameterContextLookup.addParameterContext(parameterContext);
        return parameterContext;
    }

    private static class HashMapParameterReferenceManager implements ParameterReferenceManager {
        private final Map<String, ProcessorNode> processors = new HashMap<>();
        private final Map<String, ControllerServiceNode> controllerServices = new HashMap<>();

        @Override
        public Set<ProcessorNode> getProcessorsReferencing(final ParameterContext parameterContext, final String parameterName) {
            final ProcessorNode node = processors.get(parameterName);
            return node == null ? Collections.emptySet() : Collections.singleton(node);
        }

        @Override
        public Set<ControllerServiceNode> getControllerServicesReferencing(final ParameterContext parameterContext, final String parameterName) {
            final ControllerServiceNode node = controllerServices.get(parameterName);
            return node == null ? Collections.emptySet() : Collections.singleton(node);
        }

        @Override
        public List<ParameterReferencedControllerServiceData> getReferencedControllerServiceData(ParameterContext parameterContext, String parameterName) {
            return Collections.emptyList();
        }

        @Override
        public Set<ProcessGroup> getProcessGroupsBound(final ParameterContext parameterContext) {
            return Collections.emptySet();
        }

        public void addProcessorReference(final String parameterName, final ProcessorNode processor) {
            processors.put(parameterName, processor);
        }

        public void addControllerServiceReference(final String parameterName, final ControllerServiceNode service) {
            controllerServices.put(parameterName, service);
        }
    }
}
