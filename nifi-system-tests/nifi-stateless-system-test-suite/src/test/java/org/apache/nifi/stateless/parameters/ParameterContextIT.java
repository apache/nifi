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

package org.apache.nifi.stateless.parameters;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedParameter;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.ParameterContextDefinition;
import org.apache.nifi.stateless.config.ParameterDefinition;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ParameterContextIT extends StatelessSystemIT {

    @Test
    public void testMultipleParameterContexts() throws IOException, StatelessConfigurationException {
        // Build dataflow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort inPort = flowBuilder.createInputPort("In");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessGroup groupA = createChildGroup(flowBuilder, "A", "a", "Context A");
        final VersionedProcessGroup groupB = createChildGroup(flowBuilder, "B", "b", "Context B");

        flowBuilder.createConnection(inPort, groupA.getInputPorts().iterator().next(), Relationship.ANONYMOUS.getName());
        flowBuilder.createConnection(groupA.getOutputPorts().iterator().next(), groupB.getInputPorts().iterator().next(), Relationship.ANONYMOUS.getName());
        flowBuilder.createConnection(groupB.getOutputPorts().iterator().next(), outPort, Relationship.ANONYMOUS.getName());

        // Create Parameter Contexts that we want to inject into the flow
        final List<ParameterContextDefinition> parameterContexts = new ArrayList<>();

        final List<ParameterDefinition> parametersA = new ArrayList<>();
        parametersA.add(createParameter("number", "42"));
        parametersA.add(createParameter("other", "hello")); // add a parameter that is ignored to ensure that doesn't cause problems

        final ParameterContextDefinition contextA = new ParameterContextDefinition();
        contextA.setName("Context A");
        contextA.setParameters(parametersA);
        parameterContexts.add(contextA);

        final List<ParameterDefinition> parametersB = new ArrayList<>();
        parametersB.add(createParameter("number", "100"));
        parametersA.add(createParameter("yet another", "good-bye")); // add a parameter that is ignored to ensure that doesn't cause problems

        final ParameterContextDefinition contextB = new ParameterContextDefinition();
        contextB.setName("Context B");
        contextB.setParameters(parametersB);
        parameterContexts.add(contextB);

        // Create the dataflow
        final VersionedFlowSnapshot flowSnapshot = flowBuilder.getFlowSnapshot();

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowSnapshot, parameterContexts);

        // Enqueue data and trigger
        dataflow.enqueue(new byte[0], Collections.singletonMap("abc", "123"), "In");
        dataflow.trigger();

        // Validate results
        final List<FlowFile> outputFlowFiles = dataflow.drainOutputQueues("Out");
        assertEquals(1, outputFlowFiles.size());

        final FlowFile output = outputFlowFiles.get(0);
        assertEquals("red", output.getAttribute("color"));  // Verify that the parameter that wasn't overridden works
        assertEquals("42", output.getAttribute("a"));   // Verify that Parameter from Context A works
        assertEquals("100", output.getAttribute("b"));  // Verify that Parameter from Context B works
    }

    private ParameterDefinition createParameter(final String name, final String value) {
        final ParameterDefinition definition = new ParameterDefinition();
        definition.setName(name);
        definition.setValue(value);
        return definition;
    }

    private VersionedProcessGroup createChildGroup(final VersionedFlowBuilder flowBuilder, final String groupName, final String attributeName, final String parameterContextName) {
        final VersionedProcessGroup child = flowBuilder.createProcessGroup(groupName);
        child.setParameterContextName(parameterContextName);

        final VersionedProcessor setAttribute = flowBuilder.createProcessor(SYSTEM_TEST_EXTENSIONS_BUNDLE, "org.apache.nifi.processors.tests.system.SetAttribute", child);
        final Map<String, String> properties = new HashMap<>();
        properties.put(attributeName, "#{number}");
        properties.put("color", "#{color}");
        setAttribute.setProperties(properties);

        final VersionedPort inPort = flowBuilder.createInputPort("In", child);
        final VersionedPort outPort = flowBuilder.createOutputPort("Out", child);

        flowBuilder.createConnection(inPort, setAttribute, Relationship.ANONYMOUS.getName(), child);
        flowBuilder.createConnection(setAttribute, outPort, "success", child);

        final VersionedParameterContext parameterContext = flowBuilder.createParameterContext(parameterContextName);
        parameterContext.getParameters().add(createVersionedParameter("number", "8"));
        parameterContext.getParameters().add(createVersionedParameter("color", "red"));

        return child;
    }

    private VersionedParameter createVersionedParameter(final String name, final String value) {
        final VersionedParameter parameter = new VersionedParameter();
        parameter.setName(name);
        parameter.setValue(value);
        parameter.setSensitive(false);
        return parameter;
    }
}
