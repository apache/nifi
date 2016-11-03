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

package org.apache.nifi.minifi.bootstrap.util;

import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.FunnelSchema;
import org.apache.nifi.minifi.commons.schema.PortSchema;
import org.apache.nifi.minifi.commons.schema.ProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.RemoteInputPortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessingGroupSchema;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConfigTransformerTest {

    private XPathFactory xPathFactory;
    private Document document;
    private Element config;
    private DocumentBuilder documentBuilder;

    @Before
    public void setup() throws ParserConfigurationException {
        documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        document = documentBuilder.newDocument();
        config = document.createElement("config");
        xPathFactory = XPathFactory.newInstance();
    }

    @Test
    public void testNullQueuePrioritizerNotWritten() throws ConfigurationChangeException, XPathExpressionException {
        ConfigTransformer.addConnection(config, new ConnectionSchema(Collections.emptyMap()), new ParentGroupIdResolver(new ProcessGroupSchema(Collections.emptyMap(), ConfigSchema.TOP_LEVEL_NAME)));
        XPath xpath = xPathFactory.newXPath();
        String expression = "connection/queuePrioritizerClass";
        assertNull(xpath.evaluate(expression, config, XPathConstants.NODE));
    }

    @Test
    public void testEmptyQueuePrioritizerNotWritten() throws ConfigurationChangeException, XPathExpressionException {
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, "");

        ConfigTransformer.addConnection(config, new ConnectionSchema(map), new ParentGroupIdResolver(new ProcessGroupSchema(Collections.emptyMap(), ConfigSchema.TOP_LEVEL_NAME)));
        XPath xpath = xPathFactory.newXPath();
        String expression = "connection/queuePrioritizerClass";
        assertNull(xpath.evaluate(expression, config, XPathConstants.NODE));
    }

    @Test
    public void testQueuePrioritizerWritten() throws ConfigurationChangeException, XPathExpressionException {
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, FirstInFirstOutPrioritizer.class.getCanonicalName());

        ConfigTransformer.addConnection(config, new ConnectionSchema(map), new ParentGroupIdResolver(new ProcessGroupSchema(Collections.emptyMap(), ConfigSchema.TOP_LEVEL_NAME)));
        XPath xpath = xPathFactory.newXPath();
        String expression = "connection/queuePrioritizerClass/text()";
        assertEquals(FirstInFirstOutPrioritizer.class.getCanonicalName(), xpath.evaluate(expression, config, XPathConstants.STRING));
    }

    @Test
    public void testProcessGroupsTransform() throws Exception {
        testConfigFileTransform("config-process-groups.yml");
    }

    @Test
    public void testFunnelsTransform() throws Exception {
        testConfigFileTransform("stress-test-framework-funnel.yml");
    }

    public void testConfigFileTransform(String configFile) throws Exception {
        ConfigSchema configSchema = SchemaLoader.loadConfigSchemaFromYaml(ConfigTransformerTest.class.getClassLoader().getResourceAsStream(configFile));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ConfigTransformer.writeFlowXmlFile(configSchema, outputStream);
        Document document = documentBuilder.parse(new ByteArrayInputStream(outputStream.toByteArray()));

        testProcessGroup((Element) xPathFactory.newXPath().evaluate("flowController/rootGroup", document, XPathConstants.NODE), configSchema.getProcessGroupSchema());
    }

    private void testProcessGroup(Element element, ProcessGroupSchema processGroupSchema) throws XPathExpressionException {
        assertEquals(processGroupSchema.getId(), getText(element, "id"));
        assertEquals(processGroupSchema.getName(), getText(element, "name"));
        assertEquals(nullToEmpty(processGroupSchema.getComment()), nullToEmpty(getText(element, "comment")));

        NodeList processorElements = (NodeList) xPathFactory.newXPath().evaluate("processor", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getProcessors().size(), processorElements.getLength());
        for (int i = 0; i < processorElements.getLength(); i++) {
            testProcessor((Element) processorElements.item(i), processGroupSchema.getProcessors().get(i));
        }

        NodeList remoteProcessGroupElements = (NodeList) xPathFactory.newXPath().evaluate("remoteProcessGroup", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getRemoteProcessingGroups().size(), remoteProcessGroupElements.getLength());
        for (int i = 0; i < remoteProcessGroupElements.getLength(); i++) {
            testRemoteProcessGroups((Element) remoteProcessGroupElements.item(i), processGroupSchema.getRemoteProcessingGroups().get(i));
        }

        NodeList funnelElements = (NodeList) xPathFactory.newXPath().evaluate("funnel", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getFunnels().size(), funnelElements.getLength());
        for (int i = 0; i < funnelElements.getLength(); i++) {
            testFunnel((Element) funnelElements.item(i), processGroupSchema.getFunnels().get(i));
        }

        NodeList inputPortElements = (NodeList) xPathFactory.newXPath().evaluate("inputPort", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getInputPortSchemas().size(), inputPortElements.getLength());
        for (int i = 0; i < inputPortElements.getLength(); i++) {
            testPort((Element) inputPortElements.item(i), processGroupSchema.getInputPortSchemas().get(i));
        }

        NodeList outputPortElements = (NodeList) xPathFactory.newXPath().evaluate("outputPort", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getOutputPortSchemas().size(), outputPortElements.getLength());
        for (int i = 0; i < outputPortElements.getLength(); i++) {
            testPort((Element) outputPortElements.item(i), processGroupSchema.getOutputPortSchemas().get(i));
        }

        NodeList processGroupElements = (NodeList) xPathFactory.newXPath().evaluate("processGroup", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getProcessGroupSchemas().size(), processGroupElements.getLength());
        for (int i = 0; i < processGroupElements.getLength(); i++) {
            testProcessGroup((Element) processGroupElements.item(i), processGroupSchema.getProcessGroupSchemas().get(i));
        }

        NodeList connectionElements = (NodeList) xPathFactory.newXPath().evaluate("connection", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getConnections().size(), connectionElements.getLength());
        for (int i = 0; i < connectionElements.getLength(); i++) {
            testConnection((Element) connectionElements.item(i), processGroupSchema.getConnections().get(i));
        }
    }

    private void testProcessor(Element element, ProcessorSchema processorSchema) throws XPathExpressionException {
        assertEquals(processorSchema.getId(), getText(element, "id"));
        assertEquals(processorSchema.getName(), getText(element, "name"));
        assertEquals(processorSchema.getProcessorClass(), getText(element, "class"));
        assertEquals(processorSchema.getMaxConcurrentTasks().toString(), getText(element, "maxConcurrentTasks"));
        assertEquals(processorSchema.getSchedulingPeriod(), getText(element, "schedulingPeriod"));
        assertEquals(processorSchema.getPenalizationPeriod(), getText(element, "penalizationPeriod"));
        assertEquals(processorSchema.getYieldPeriod(), getText(element, "yieldPeriod"));
        assertEquals(processorSchema.getSchedulingStrategy(), getText(element, "schedulingStrategy"));
        assertEquals(processorSchema.getRunDurationNanos().toString(), getText(element, "runDurationNanos"));

        testProperties(element, processorSchema.getProperties());
    }

    private void testRemoteProcessGroups(Element element, RemoteProcessingGroupSchema remoteProcessingGroupSchema) throws XPathExpressionException {
        assertEquals(remoteProcessingGroupSchema.getName(), getText(element, "id"));
        assertEquals(remoteProcessingGroupSchema.getName(), getText(element, "name"));
        assertEquals(remoteProcessingGroupSchema.getComment(), getText(element, "comment"));
        assertEquals(remoteProcessingGroupSchema.getUrl(), getText(element, "url"));
        assertEquals(remoteProcessingGroupSchema.getTimeout(), getText(element, "timeout"));
        assertEquals(remoteProcessingGroupSchema.getYieldPeriod(), getText(element, "yieldPeriod"));


        NodeList inputPortElements = (NodeList) xPathFactory.newXPath().evaluate("inputPort", element, XPathConstants.NODESET);
        assertEquals(remoteProcessingGroupSchema.getInputPorts().size(), inputPortElements.getLength());
        for (int i = 0; i < inputPortElements.getLength(); i++) {
            testRemoteInputPort((Element) inputPortElements.item(i), remoteProcessingGroupSchema.getInputPorts().get(i));
        }
    }

    private void testRemoteInputPort(Element element, RemoteInputPortSchema remoteInputPortSchema) throws XPathExpressionException {
        assertEquals(remoteInputPortSchema.getId(), getText(element, "id"));
        assertEquals(remoteInputPortSchema.getName(), getText(element, "name"));
        assertEquals(remoteInputPortSchema.getComment(), getText(element, "comment"));
        assertEquals(remoteInputPortSchema.getMax_concurrent_tasks().toString(), getText(element, "maxConcurrentTasks"));
        assertEquals(remoteInputPortSchema.getUseCompression(), Boolean.parseBoolean(getText(element, "useCompression")));
    }

    private void testPort(Element element, PortSchema portSchema) throws XPathExpressionException {
        assertEquals(portSchema.getId(), getText(element, "id"));
        assertEquals(portSchema.getName(), getText(element, "name"));
        assertEquals("RUNNING", getText(element, "scheduledState"));
    }

    private void testFunnel(Element element, FunnelSchema funnelSchema) throws XPathExpressionException {
        assertEquals(funnelSchema.getId(), getText(element, "id"));
    }

    private void testConnection(Element element, ConnectionSchema connectionSchema) throws XPathExpressionException {
        assertEquals(connectionSchema.getId(), getText(element, "id"));
        assertEquals(connectionSchema.getName(), getText(element, "name"));

        assertEquals(connectionSchema.getSourceId(), getText(element, "sourceId"));
        assertEquals(connectionSchema.getDestinationId(), getText(element, "destinationId"));

        NodeList relationshipNodes = (NodeList) xPathFactory.newXPath().evaluate("relationship", element, XPathConstants.NODESET);
        Set<String> sourceRelationships = new HashSet<>();
        for (int i = 0; i < relationshipNodes.getLength(); i++) {
            String textContent = relationshipNodes.item(i).getTextContent();
            if (!StringUtil.isNullOrEmpty(textContent)) {
                sourceRelationships.add(textContent);
            }
        }

        assertEquals(new HashSet<>(connectionSchema.getSourceRelationshipNames()), sourceRelationships);

        assertEquals(connectionSchema.getMaxWorkQueueSize().toString(), getText(element, "maxWorkQueueSize"));
        assertEquals(connectionSchema.getMaxWorkQueueDataSize(), getText(element, "maxWorkQueueDataSize"));
        assertEquals(connectionSchema.getFlowfileExpiration(), getText(element, "flowFileExpiration"));
        assertEquals(connectionSchema.getQueuePrioritizerClass(), getText(element, "queuePrioritizerClass"));
    }

    private void testProperties(Element element, Map<String, Object> expected) throws XPathExpressionException {
        NodeList propertyElements = (NodeList) xPathFactory.newXPath().evaluate("property", element, XPathConstants.NODESET);
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i < propertyElements.getLength(); i++) {
            Element item = (Element) propertyElements.item(i);
            properties.put(getText(item, "name"), getText(item, "value"));
        }
        assertEquals(expected.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> nullToEmpty(e.getValue()))), properties);
    }

    private String getText(Element element, String path) throws XPathExpressionException {
        return (String) xPathFactory.newXPath().evaluate(path + "/text()", element, XPathConstants.STRING);
    }

    private String nullToEmpty(Object val) {
        return val == null ? "" : val.toString();
    }
}
