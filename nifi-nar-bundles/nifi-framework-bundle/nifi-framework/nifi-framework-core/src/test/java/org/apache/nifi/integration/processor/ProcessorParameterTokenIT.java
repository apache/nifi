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
package org.apache.nifi.integration.processor;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.integration.DirectInjectionExtensionManager;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterContext;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.junit.Assert.assertEquals;

public class ProcessorParameterTokenIT extends FrameworkIntegrationTest {

    @Override
    protected void injectExtensionTypes(final DirectInjectionExtensionManager extensionManager) {
        extensionManager.injectExtensionType(Processor.class, WriteText.class);
    }

    @Test
    public void testEscapedParameterReference() throws ExecutionException, InterruptedException {
        final ProcessorNode procNode = createProcessorNode(WriteText.class);
        procNode.setAutoTerminatedRelationships(Collections.singleton(REL_SUCCESS));

        verifyText(procNode, "hello", "hello");
        verifyText(procNode, "##{foo}", "#{foo}");
        verifyText(procNode, "####{foo}", "##{foo}");
        verifyText(procNode, "## hello ##{foo} ##{bar}", "## hello #{foo} #{bar}");
    }


    @Test
    public void testProperReferences() throws ExecutionException, InterruptedException {
        final ProcessorNode procNode = createProcessorNode(WriteText.class);
        procNode.setAutoTerminatedRelationships(Collections.singleton(REL_SUCCESS));

        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "testEscapedParameterReference", ParameterReferenceManager.EMPTY, null);
        getRootGroup().setParameterContext(parameterContext);

        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("foo", new Parameter(new ParameterDescriptor.Builder().name("foo").build(), "bar"));
        parameters.put("sensitive", new Parameter(new ParameterDescriptor.Builder().name("sensitive").sensitive(true).build(), "*password*"));
        parameterContext.setParameters(parameters);

        verifyText(procNode, "hello", "hello");
        verifyText(procNode, "##{foo}", "#{foo}");
        verifyText(procNode, "#{foo}", "bar");
        verifyText(procNode, "####{foo}", "##{foo}");
        verifyText(procNode, "#####{foo}", "##bar");
        verifyText(procNode, "## hello #{foo} ##{bar}", "## hello bar #{bar}");

        try {
            verifyText(procNode, "#{bar}", "ISE");
        } catch (final IllegalStateException expected) {
            // Expect IllegalStateException because processor is not valid because it references a non-existent parameter.
        }

        verifyText(procNode, "#{foo}", "password", "barpassword");
        verifyText(procNode, "#{foo}", "#{sensitive}", "bar*password*");
    }

    @Test
    public void testSensitiveParameters() throws ExecutionException, InterruptedException {
        final ProcessorNode procNode = createProcessorNode(WriteText.class);
        procNode.setAutoTerminatedRelationships(Collections.singleton(REL_SUCCESS));

        final ParameterContext parameterContext = new StandardParameterContext(UUID.randomUUID().toString(), "testEscapedParameterReference", ParameterReferenceManager.EMPTY, null);
        getRootGroup().setParameterContext(parameterContext);

        final Map<String, Parameter> parameters = new HashMap<>();
        parameters.put("foo", new Parameter(new ParameterDescriptor.Builder().name("foo").build(), "bar"));
        parameters.put("sensitive", new Parameter(new ParameterDescriptor.Builder().name("sensitive").sensitive(true).build(), "*password*"));
        parameterContext.setParameters(parameters);

        verifyCannotSetParameter(procNode, "#{sensitive}", null);
        verifyCannotSetParameter(procNode, "abc#{sensitive}foo", null);
        verifyCannotSetParameter(procNode, "#{foo}", "#{foo}");
        verifyCannotSetParameter(procNode, null, "#{sensitive}#{sensitive}");
        verifyCannotSetParameter(procNode, null, "#{sensitive}123");
        verifyCannotSetParameter(procNode, null, "123#{sensitive}");
        verifyCannotSetParameter(procNode, null, "#{foo}");

        verifyText(procNode, "#{foo}", "#{sensitive}", "bar*password*");
        verifyText(procNode, "#{foo}", "##{sensitive}##{sensitive}##{sensitive}", "bar#{sensitive}#{sensitive}#{sensitive}");
    }

    private void verifyCannotSetParameter(final ProcessorNode procNode, final String text, final String password) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(WriteText.TEXT.getName(), text);
        properties.put(WriteText.PASSWORD.getName(), password);

        try {
            procNode.setProperties(properties);
            Assert.fail("Expected to fail when setting properties to " + properties);
        } catch (final IllegalArgumentException expected) {
        }
    }

    private void verifyText(final ProcessorNode procNode, final String text, final String expectedOutput) throws ExecutionException, InterruptedException {
        verifyText(procNode, text, null, expectedOutput);
    }

    private void verifyText(final ProcessorNode procNode, final String text, final String password, final String expectedOutput) throws ExecutionException, InterruptedException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(WriteText.TEXT.getName(), text);
        properties.put(WriteText.PASSWORD.getName(), password);

        procNode.setProperties(properties);
        triggerOnce(procNode);

        final WriteText writeText = (WriteText) procNode.getProcessor();
        final String textWritten = writeText.getTextLastWritten();

        assertEquals("For input text <" + text+ "> and password <" + password + ">, expected output was <" + expectedOutput + "> but got <" + textWritten + ">", expectedOutput, textWritten);
    }


    public static class WriteText extends AbstractProcessor {
        private volatile String textLastWritten = null;

        static final PropertyDescriptor TEXT = new Builder()
            .name("Text")
            .displayName("Text")
            .description("The text to write")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

        static final PropertyDescriptor PASSWORD = new Builder()
            .name("password")
            .displayName("password")
            .description("Password")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Arrays.asList(TEXT, PASSWORD);
        }

        @Override
        public Set<Relationship> getRelationships() {
            return Collections.singleton(REL_SUCCESS);
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            FlowFile flowFile = session.create();

            final String text = context.getProperty(TEXT).getValue();

            final String fullText;
            if (context.getProperty(PASSWORD).isSet()) {
                fullText = text + context.getProperty(PASSWORD).getValue();
            } else {
                fullText = text;
            }

            flowFile = session.write(flowFile, out -> out.write(fullText.getBytes(StandardCharsets.UTF_8)));
            session.transfer(flowFile, REL_SUCCESS);

            textLastWritten = fullText;
        }

        private String getTextLastWritten() {
            return textLastWritten;
        }
    }
}
