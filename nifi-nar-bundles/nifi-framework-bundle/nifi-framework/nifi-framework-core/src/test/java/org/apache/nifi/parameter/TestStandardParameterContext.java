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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestStandardParameterContext {

    @Test
    public void testUpdatesApply() {
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContext context = new StandardParameterContext("unit-test-context", "unit-test-context", referenceManager);

        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").build();
        final ParameterDescriptor xyzDescriptor = new ParameterDescriptor.Builder().name("xyz").build();
        final ParameterDescriptor fooDescriptor = new ParameterDescriptor.Builder().name("foo").description("bar").sensitive(true).build();

        final Set<Parameter> parameters = new HashSet<>();
        parameters.add(new Parameter(abcDescriptor, "123"));
        parameters.add(new Parameter(xyzDescriptor, "242526"));

        context.setParameters(parameters);

        final Parameter abcParam = context.getParameter("abc").get();
        assertEquals(abcDescriptor, abcParam.getDescriptor());
        assertEquals("", abcParam.getDescriptor().getDescription());
        assertEquals("123", abcParam.getValue());

        final Parameter xyzParam = context.getParameter("xyz").get();
        assertEquals(xyzDescriptor, xyzParam.getDescriptor());
        assertEquals("", xyzParam.getDescriptor().getDescription());
        assertEquals("242526", xyzParam.getValue());

        final Set<Parameter> secondParameters = new HashSet<>();
        secondParameters.add(new Parameter(fooDescriptor, "baz"));
        context.setParameters(secondParameters);

        assertTrue(context.getParameter("abc").isPresent());
        assertTrue(context.getParameter("xyz").isPresent());

        secondParameters.add(new Parameter(abcParam.getDescriptor(), null));
        secondParameters.add(new Parameter(xyzParam.getDescriptor(), null));

        context.setParameters(secondParameters);

        assertFalse(context.getParameter("abc").isPresent());
        assertFalse(context.getParameter("xyz").isPresent());

        final Parameter fooParam = context.getParameter(fooDescriptor).get();
        assertEquals(fooDescriptor, fooParam.getDescriptor());
        assertTrue(fooParam.getDescriptor().isSensitive());
        assertEquals("bar", fooParam.getDescriptor().getDescription());
        assertEquals("baz", fooParam.getValue());

        assertEquals(Collections.singletonMap(fooDescriptor, fooParam), context.getParameters());

        final Set<Parameter> thirdParameters = new HashSet<>();
        thirdParameters.add(new Parameter(fooDescriptor, "other"));
        context.setParameters(thirdParameters);

        assertEquals("other", context.getParameter("foo").get().getValue());
    }

    @Test
    public void testChangingSensitivity() {
        // Ensure no changes applied
        final ParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContext context = new StandardParameterContext("unit-test-context", "unit-test-context", referenceManager);

        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").sensitive(true).build();
        final ParameterDescriptor xyzDescriptor = new ParameterDescriptor.Builder().name("xyz").build();
        final ParameterDescriptor fooDescriptor = new ParameterDescriptor.Builder().name("foo").description("bar").sensitive(true).build();

        final Set<Parameter> parameters = new HashSet<>();
        parameters.add(new Parameter(abcDescriptor, "123"));
        parameters.add(new Parameter(xyzDescriptor, "242526"));

        context.setParameters(parameters);

        final ParameterDescriptor sensitiveXyzDescriptor = new ParameterDescriptor.Builder().name("xyz").sensitive(true).build();

        final Set<Parameter> updatedParameters = new HashSet<>();
        updatedParameters.add(new Parameter(fooDescriptor, "baz"));
        updatedParameters.add(new Parameter(sensitiveXyzDescriptor, "242526"));

        try {
            context.setParameters(updatedParameters);
            Assert.fail("Succeeded in changing parameter from non-sensitive to sensitive");
        } catch (final IllegalStateException expected) {
        }

        final ParameterDescriptor insensitiveAbcDescriptor = new ParameterDescriptor.Builder().name("abc").sensitive(false).build();
        updatedParameters.clear();
        updatedParameters.add(new Parameter(insensitiveAbcDescriptor, "123"));

        try {
            context.setParameters(updatedParameters);
            Assert.fail("Succeeded in changing parameter from sensitive to non-sensitive");
        } catch (final IllegalStateException expected) {
        }
    }

    @Test
    public void testChangingParameterForRunningProcessor() {
        final HashMapParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContext context = new StandardParameterContext("unit-test-context", "unit-test-context", referenceManager);

        final ProcessorNode procNode = Mockito.mock(ProcessorNode.class);
        Mockito.when(procNode.isRunning()).thenReturn(false);
        referenceManager.addProcessorReference("abc", procNode);

        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").sensitive(true).build();

        final Set<Parameter> parameters = new HashSet<>();
        parameters.add(new Parameter(abcDescriptor, "123"));

        context.setParameters(parameters);

        parameters.clear();
        parameters.add(new Parameter(abcDescriptor, "321"));
        context.setParameters(parameters);

        assertEquals("321", context.getParameter("abc").get().getValue());

        // Make processor 'running'
        Mockito.when(procNode.isRunning()).thenReturn(true);

        parameters.clear();
        parameters.add(new Parameter(abcDescriptor, "123"));

        try {
            context.setParameters(parameters);
            Assert.fail("Was able to change parameter while referencing processor was running");
        } catch (final IllegalStateException expected) {
        }

        context.setParameters(Collections.emptySet());

        parameters.clear();
        parameters.add(new Parameter(abcDescriptor, null));
        try {
            context.setParameters(parameters);
            Assert.fail("Was able to remove parameter while referencing processor was running");
        } catch (final IllegalStateException expected) {
        }

        assertEquals("321", context.getParameter("abc").get().getValue());
    }

    @Test
    public void testChangingParameterForEnabledControllerService() {
        final HashMapParameterReferenceManager referenceManager = new HashMapParameterReferenceManager();
        final StandardParameterContext context = new StandardParameterContext("unit-test-context", "unit-test-context", referenceManager);

        final ControllerServiceNode serviceNode = Mockito.mock(ControllerServiceNode.class);
        Mockito.when(serviceNode.getState()).thenReturn(ControllerServiceState.ENABLED);

        final ParameterDescriptor abcDescriptor = new ParameterDescriptor.Builder().name("abc").sensitive(true).build();
        final Set<Parameter> parameters = new HashSet<>();
        parameters.add(new Parameter(abcDescriptor, "123"));

        context.setParameters(parameters);

        referenceManager.addControllerServiceReference("abc", serviceNode);

        parameters.clear();
        parameters.add(new Parameter(abcDescriptor, "321"));

        for (final ControllerServiceState state : EnumSet.of(ControllerServiceState.ENABLED, ControllerServiceState.ENABLING, ControllerServiceState.DISABLING)) {
            Mockito.when(serviceNode.getState()).thenReturn(state);

            try {
                context.setParameters(parameters);
                Assert.fail("Was able to update parameter being referenced by Controller Service that is " + state);
            } catch (final IllegalStateException expected) {
            }

            assertEquals("123", context.getParameter("abc").get().getValue());
        }

        parameters.clear();
        context.setParameters(parameters);

        parameters.add(new Parameter(abcDescriptor, null));
        try {
            context.setParameters(parameters);
            Assert.fail("Was able to remove parameter being referenced by Controller Service that is DISABLING");
        } catch (final IllegalStateException expected) {
        }
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
