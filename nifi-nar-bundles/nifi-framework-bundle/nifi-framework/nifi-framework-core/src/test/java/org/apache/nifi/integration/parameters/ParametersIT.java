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
package org.apache.nifi.integration.parameters;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.integration.processors.GenerateProcessor;
import org.apache.nifi.integration.processors.UpdateAttributeCreateOwnProperty;
import org.apache.nifi.integration.processors.UpdateAttributeNoEL;
import org.apache.nifi.integration.processors.UpdateAttributeWithEL;
import org.apache.nifi.integration.processors.UsernamePasswordProcessor;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterContext;
import org.apache.nifi.parameter.StandardParameterReferenceManager;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.StandardProcessContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class ParametersIT extends FrameworkIntegrationTest {

    @Test
    public void testSimpleParameterSubstitution() throws ExecutionException, InterruptedException {
        final ProcessorNode generate = createProcessorNode(GenerateProcessor.class);
        final ProcessorNode updateAttribute = createProcessorNode(UpdateAttributeNoEL.class);
        final ProcessorNode terminate = getTerminateProcessor();

        final Connection generatedFlowFileConnection = connect(generate, updateAttribute, REL_SUCCESS);
        final Connection updatedAttributeConnection = connect(updateAttribute, terminate, REL_SUCCESS);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);
        updateAttribute.setProperties(Collections.singletonMap("test", "#{test}"));

        triggerOnce(generate);
        triggerOnce(updateAttribute);

        final FlowFileQueue flowFileQueue = updatedAttributeConnection.getFlowFileQueue();
        final FlowFileRecord flowFileRecord = flowFileQueue.poll(Collections.emptySet());

        assertEquals("unit", flowFileRecord.getAttribute("test"));
    }

    @Test
    public void testParameterSubstitutionWithinELWhenELNotSupported() throws ExecutionException, InterruptedException {
        final ProcessorNode generate = createProcessorNode(GenerateProcessor.class);
        final ProcessorNode updateAttribute = createProcessorNode(UpdateAttributeNoEL.class);
        final ProcessorNode terminate = getTerminateProcessor();

        final Connection generatedFlowFileConnection = connect(generate, updateAttribute, REL_SUCCESS);
        final Connection updatedAttributeConnection = connect(updateAttribute, terminate, REL_SUCCESS);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);
        updateAttribute.setProperties(Collections.singletonMap("test", "${#{test}:toUpper()}"));

        triggerOnce(generate);
        triggerOnce(updateAttribute);

        final FlowFileQueue flowFileQueue = updatedAttributeConnection.getFlowFileQueue();
        final FlowFileRecord flowFileRecord = flowFileQueue.poll(Collections.emptySet());

        assertEquals("${unit:toUpper()}", flowFileRecord.getAttribute("test"));
    }

    @Test
    public void testParameterSubstitutionWithinELWhenELIsSupported() throws ExecutionException, InterruptedException {
        final ProcessorNode generate = createProcessorNode(GenerateProcessor.class);
        final ProcessorNode updateAttribute = createProcessorNode(UpdateAttributeWithEL.class);
        final ProcessorNode terminate = getTerminateProcessor();

        final Connection generatedFlowFileConnection = connect(generate, updateAttribute, REL_SUCCESS);
        final Connection updatedAttributeConnection = connect(updateAttribute, terminate, REL_SUCCESS);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);
        updateAttribute.setProperties(Collections.singletonMap("test", "${#{test}:toUpper()}"));

        triggerOnce(generate);
        triggerOnce(updateAttribute);

        final FlowFileQueue flowFileQueue = updatedAttributeConnection.getFlowFileQueue();
        final FlowFileRecord flowFileRecord = flowFileQueue.poll(Collections.emptySet());

        assertEquals("UNIT", flowFileRecord.getAttribute("test"));
    }

    @Test
    public void testMixAndMatchELAndParameters() throws ExecutionException, InterruptedException {
        final ProcessorNode generate = createProcessorNode(GenerateProcessor.class);
        final ProcessorNode updateAttribute = createProcessorNode(UpdateAttributeWithEL.class);
        final ProcessorNode terminate = getTerminateProcessor();

        final Connection generatedFlowFileConnection = connect(generate, updateAttribute, REL_SUCCESS);
        final Connection updatedAttributeConnection = connect(updateAttribute, terminate, REL_SUCCESS);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> properties = new HashMap<>();
        properties.put("mixed", "test ${#{test}} #{test} ${#{test}:toUpper()} ${#{test}:equalsIgnoreCase('uNiT')} again ${#{test}}");
        properties.put("ends with text", "test ${#{test}} #{test} ${#{test}:toUpper()} ${#{test}:equalsIgnoreCase('uNiT')} again");
        properties.put("el and param", "#{test} - ${#{test}}");
        updateAttribute.setProperties(properties);

        triggerOnce(generate);
        triggerOnce(updateAttribute);

        final FlowFileQueue flowFileQueue = updatedAttributeConnection.getFlowFileQueue();
        final FlowFileRecord flowFileRecord = flowFileQueue.poll(Collections.emptySet());

        assertEquals("test unit unit UNIT true again unit", flowFileRecord.getAttribute("mixed"));
        assertEquals("test unit unit UNIT true again", flowFileRecord.getAttribute("ends with text"));
        assertEquals("unit - unit", flowFileRecord.getAttribute("el and param"));
    }

    @Test
    public void testParametersInELFromNewPropertyValueAndText() throws ExecutionException, InterruptedException {
        final ProcessorNode generate = createProcessorNode(GenerateProcessor.class);
        final ProcessorNode updateAttribute = createProcessorNode(UpdateAttributeCreateOwnProperty.class);
        final ProcessorNode terminate = getTerminateProcessor();

        final Connection generatedFlowFileConnection = connect(generate, updateAttribute, REL_SUCCESS);
        final Connection updatedAttributeConnection = connect(updateAttribute, terminate, REL_SUCCESS);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> properties = new HashMap<>();
        properties.put("bar", "${#{test}:toUpper()}");
        updateAttribute.setProperties(properties);

        triggerOnce(generate);
        triggerOnce(updateAttribute);

        final FlowFileQueue flowFileQueue = updatedAttributeConnection.getFlowFileQueue();
        final FlowFileRecord flowFileRecord = flowFileQueue.poll(Collections.emptySet());

        assertEquals("UNIT", flowFileRecord.getAttribute("bar"));
    }

    @Test
    public void testParametersWhereELSupportedButNotPresent() throws ExecutionException, InterruptedException {
        final ProcessorNode generate = createProcessorNode(GenerateProcessor.class);
        final ProcessorNode updateAttribute = createProcessorNode(UpdateAttributeWithEL.class);
        final ProcessorNode terminate = getTerminateProcessor();

        final Connection generatedFlowFileConnection = connect(generate, updateAttribute, REL_SUCCESS);
        final Connection updatedAttributeConnection = connect(updateAttribute, terminate, REL_SUCCESS);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> properties = new HashMap<>();
        properties.put("foo", "#{test}");
        properties.put("bar", "#{test}#{test}");
        properties.put("baz", "foo#{test}bar");
        updateAttribute.setProperties(properties);

        triggerOnce(generate);
        triggerOnce(updateAttribute);

        final FlowFileQueue flowFileQueue = updatedAttributeConnection.getFlowFileQueue();
        final FlowFileRecord flowFileRecord = flowFileQueue.poll(Collections.emptySet());

        assertEquals("unit", flowFileRecord.getAttribute("foo"));
        assertEquals("unitunit", flowFileRecord.getAttribute("bar"));
        assertEquals("foounitbar", flowFileRecord.getAttribute("baz"));
    }

    @Test
    public void testCornerCases() throws ExecutionException, InterruptedException {
        final ProcessorNode generate = createProcessorNode(GenerateProcessor.class);
        final ProcessorNode updateAttribute = createProcessorNode(UpdateAttributeWithEL.class);
        final ProcessorNode terminate = getTerminateProcessor();

        final Connection generatedFlowFileConnection = connect(generate, updateAttribute, REL_SUCCESS);
        final Connection updatedAttributeConnection = connect(updateAttribute, terminate, REL_SUCCESS);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> variables = new HashMap<>();
        variables.put("#{test}", "variable #{test}");
        variables.put("${var}", "variable ${var}");
        variables.put("var", "abc");
        variables.put("abc", "123");
        getRootGroup().setVariables(variables);

        final Map<String, String> properties = new HashMap<>();
        properties.put("foo", "${#{test}}");  // References a Parameter named 'test'
        properties.put("bar", "${'#{test}'}");  // Parameter reference is quoted, which means that it's treated as a String, not a reference. This references a variable/attribute named '#{test}'
        properties.put("baz", "${ # this is a comment\n#{test}}"); // Test Parameter reference following a comment in EL
        properties.put("multi", "${ #### this is a comment\n#{test}}");  // Test several #'s for a comment, followed by a parameter reference
        properties.put("embedded", "${'$${var}'}");  // Here, we reference a variable/attribute named '${var}' - since EL Expressions can be embedded even within quotes, we must escape with extra $.
        properties.put("indirect", "${'${var}'}"); // Reference a variable/attribute whose name is defined by variable/attribute 'var' - i.e., reference variable 'var', whose value is 'abc', then
                                                   // use that to reference variable "abc" to get a value of 123
        updateAttribute.setProperties(properties);

        triggerOnce(generate);
        triggerOnce(updateAttribute);

        final FlowFileQueue flowFileQueue = updatedAttributeConnection.getFlowFileQueue();
        final FlowFileRecord flowFileRecord = flowFileQueue.poll(Collections.emptySet());

        assertEquals("unit", flowFileRecord.getAttribute("foo"));
        assertEquals("variable #{test}", flowFileRecord.getAttribute("bar"));
        assertEquals("unit", flowFileRecord.getAttribute("baz"));
        assertEquals("unit", flowFileRecord.getAttribute("multi"));
        assertEquals("variable ${var}", flowFileRecord.getAttribute("embedded"));
        assertEquals("123", flowFileRecord.getAttribute("indirect"));
    }

    @Test
    public void testReferencesStoredProperly() {
        final ProcessorNode updateAttributeWithEL = createProcessorNode(UpdateAttributeWithEL.class);
        final ProcessorNode updateAttributeNoEL = createProcessorNode(UpdateAttributeNoEL.class);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> properties = new HashMap<>();
        properties.put("foo", "#{test}");
        properties.put("bar", "${#{test}:toUpper()}");

        updateAttributeWithEL.setProperties(properties);
        updateAttributeNoEL.setProperties(properties);

        List<ParameterReference> references = updateAttributeWithEL.getProperty(updateAttributeWithEL.getPropertyDescriptor("foo")).getParameterReferences();
        assertEquals(1, references.size());

        references = updateAttributeWithEL.getProperty(updateAttributeWithEL.getPropertyDescriptor("bar")).getParameterReferences();
        assertEquals(1, references.size());

        references = updateAttributeNoEL.getProperty(updateAttributeNoEL.getPropertyDescriptor("foo")).getParameterReferences();
        assertEquals(1, references.size());

        references = updateAttributeNoEL.getProperty(updateAttributeNoEL.getPropertyDescriptor("bar")).getParameterReferences();
        assertEquals(1, references.size());
    }

    @Test
    public void testEscaping() throws ExecutionException, InterruptedException {
        testEscaping(UpdateAttributeNoEL.class);
        testEscaping(UpdateAttributeWithEL.class);
        testEscaping(UpdateAttributeCreateOwnProperty.class);
    }

    private void testEscaping(final Class<? extends Processor> updateAttributeClass) throws ExecutionException, InterruptedException {
        final ProcessorNode generate = createProcessorNode(GenerateProcessor.class);
        final ProcessorNode updateAttribute = createProcessorNode(updateAttributeClass);

        connect(generate, updateAttribute, REL_SUCCESS);
        final Connection terminateConnection = connect(updateAttribute, getTerminateProcessor(), REL_SUCCESS);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> properties = new HashMap<>();
        properties.put("1pound", "#{test}");
        properties.put("2pound", "##{test}");
        properties.put("3pound", "###{test}");
        properties.put("4pound", "####{test}");
        properties.put("5pound", "#####{test}");

        updateAttribute.setProperties(properties);

        triggerOnce(generate);
        triggerOnce(updateAttribute);

        final FlowFileRecord flowFile = terminateConnection.getFlowFileQueue().poll(Collections.emptySet());
        assertEquals("unit", flowFile.getAttribute("1pound"));
        assertEquals("#{test}", flowFile.getAttribute("2pound"));
        assertEquals("#unit", flowFile.getAttribute("3pound"));
        assertEquals("##{test}", flowFile.getAttribute("4pound"));
        assertEquals("##unit", flowFile.getAttribute("5pound"));
    }

    @Test
    public void testParameterReferenceCounts() {
        final ProcessorNode updateAttribute = createProcessorNode(UpdateAttributeWithEL.class);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("test", new Parameter(new ParameterDescriptor.Builder().name("test").build(), "unit")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> properties = new HashMap<>();
        properties.put("test", "#{test} #{test}");
        updateAttribute.setProperties(properties);

        final Set<String> allParamNames = Collections.singleton("test");

        Set<String> referencedParameters = updateAttribute.getReferencedParameterNames();
        assertEquals(allParamNames, referencedParameters);

        properties.put("test", "#{test} #{test} #{test}");
        updateAttribute.setProperties(properties);
        referencedParameters = updateAttribute.getReferencedParameterNames();
        assertEquals(allParamNames, referencedParameters);

        properties.put("test", null);
        updateAttribute.setProperties(properties);
        referencedParameters = updateAttribute.getReferencedParameterNames();
        assertEquals(Collections.emptySet(), referencedParameters);

        properties.put("test", "#{test}");
        updateAttribute.setProperties(properties);
        referencedParameters = updateAttribute.getReferencedParameterNames();
        assertEquals(allParamNames, referencedParameters);
    }

    @Test
    public void testSensitivePropertyReferenceParameterSupportsEL() {
        final ProcessorNode usernamePassword = createProcessorNode(UsernamePasswordProcessor.class);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("pass", new Parameter(new ParameterDescriptor.Builder().name("pass").sensitive(true).build(), "secret")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> properties = new HashMap<>();
        properties.put("password", "#{pass}");
        usernamePassword.setProperties(properties);

        final ProcessContext processContext = new StandardProcessContext(usernamePassword, getFlowController().getControllerServiceProvider(), getFlowController().getEncryptor(),
            getFlowController().getStateManagerProvider().getStateManager(usernamePassword.getIdentifier()), () -> false, getFlowController());
        final PropertyDescriptor descriptor = usernamePassword.getPropertyDescriptor("password");
        final PropertyValue propertyValue = processContext.getProperty(descriptor);
        final PropertyValue evaluatedPropertyValue = propertyValue.evaluateAttributeExpressions();
        final String evaluatedPassword = evaluatedPropertyValue.getValue();
        assertEquals("secret", evaluatedPassword);
    }

    @Test
    public void testSensitivePropertyNotAccessibleFromWithinEL() {
        final ProcessorNode usernamePassword = createProcessorNode(UsernamePasswordProcessor.class);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("pass", new Parameter(new ParameterDescriptor.Builder().name("pass").sensitive(true).build(), "secret")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> properties = new HashMap<>();
        properties.put("username", "${#{pass}}");

        try {
            usernamePassword.setProperties(properties);
            Assert.fail("Was able to set properties when referencing sensitive parameter from within EL");
        } catch (final IllegalArgumentException iae) {
            // Expected. Since the parameter is sensitive, it may referenced by a sensitive property
        }
    }

    @Test
    public void testSensitivePropertyCannotBeSetToReferenceParamFromEL() {
        final ProcessorNode usernamePassword = createProcessorNode(UsernamePasswordProcessor.class);

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(getFlowController().getFlowManager());
        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "param-context", referenceManager, null);
        parameterContext.setParameters(Collections.singletonMap("pass", new Parameter(new ParameterDescriptor.Builder().name("pass").sensitive(true).build(), "secret")));

        getRootGroup().setParameterContext(parameterContext);

        final Map<String, String> properties = new HashMap<>();
        properties.put("password", "${#{pass}}");

        try {
            usernamePassword.setProperties(properties);
            Assert.fail("Was able to set properties when referencing sensitive parameter from within EL");
        } catch (final IllegalArgumentException iae) {
            // Expected. Since the property is sensitive, it may reference a parameter only if that is the only value.
        }
    }

}
