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

import org.apache.commons.io.FileUtils;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.exception.InvalidConfigurationException;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.ControllerServiceSchema;
import org.apache.nifi.minifi.commons.schema.FunnelSchema;
import org.apache.nifi.minifi.commons.schema.PortSchema;
import org.apache.nifi.minifi.commons.schema.ProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.RemotePortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.ReportingSchema;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigTransformerTest {
    public static final Map<String, Integer> PG_ELEMENT_ORDER_MAP = generateOrderMap(
            Arrays.asList("processor", "inputPort", "outputPort", "funnel", "processGroup", "remoteProcessGroup", "connection"));
    private XPathFactory xPathFactory;
    private Document document;
    private Element config;
    private DocumentBuilder documentBuilder;

    @Rule
    final public TemporaryFolder tempOutputFolder = new TemporaryFolder();

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
        map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, "org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer");

        ConfigTransformer.addConnection(config, new ConnectionSchema(map), new ParentGroupIdResolver(new ProcessGroupSchema(Collections.emptyMap(), ConfigSchema.TOP_LEVEL_NAME)));
        XPath xpath = xPathFactory.newXPath();
        String expression = "connection/queuePrioritizerClass/text()";
        assertEquals("org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer", xpath.evaluate(expression, config, XPathConstants.STRING));
    }

    @Test
    public void testReportingTasksTransform() throws Exception {
        testConfigFileTransform("config-reporting-task.yml");
    }

    @Test
    public void testProcessGroupsTransform() throws Exception {
        testConfigFileTransform("config-process-groups.yml");
    }

    @Test
    public void testFunnelsTransform() throws Exception {
        testConfigFileTransform("stress-test-framework-funnel.yml");
    }

    @Test
    public void testFunnelAndRpgTransform() throws Exception {
        testConfigFileTransform("config-funnel-and-rpg.yml");
    }

    @Test
    public void testRpgTransform() throws Exception {
        testConfigFileTransform("config-multiple-RPGs.yml");
    }

    @Test
    public void testRpgProxyNoPassTransform() throws Exception {
        testConfigFileTransform("InvokeHttpMiNiFiProxyNoPasswordTemplateTest.yml");
    }

    @Test
    public void testRpgProxyPassTransform() throws Exception {
        testConfigFileTransform("InvokeHttpMiNiFiProxyPasswordTemplateTest.yml");
    }

    @Test
    public void testRpgOutputPort() throws Exception {
        testConfigFileTransform("SimpleRPGToLogAttributes.yml");
    }

    @Test
    public void testNifiPropertiesNoOverrides() throws IOException, ConfigurationChangeException, SchemaLoaderException {
        Properties pre216Properties = new Properties();
        try (InputStream pre216PropertiesStream = ConfigTransformerTest.class.getClassLoader().getResourceAsStream("MINIFI-216/nifi.properties.before")) {
            pre216Properties.load(pre216PropertiesStream);
        }
        pre216Properties.setProperty(ConfigTransformer.NIFI_VERSION_KEY, ConfigTransformer.NIFI_VERSION);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (InputStream configStream = ConfigTransformerTest.class.getClassLoader().getResourceAsStream("MINIFI-216/config.yml")) {
            ConfigTransformer.writeNiFiProperties(SchemaLoader.loadConfigSchemaFromYaml(configStream), outputStream);
        }
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(outputStream.toByteArray()));

        for (String name : pre216Properties.stringPropertyNames()) {
            assertEquals("Property key " + name + " doesn't match.", pre216Properties.getProperty(name), properties.getProperty(name));
        }
    }

    @Test
    public void testNifiPropertiesOverrides() throws IOException, ConfigurationChangeException, SchemaLoaderException {
        Properties pre216Properties = new Properties();
        try (InputStream pre216PropertiesStream = ConfigTransformerTest.class.getClassLoader().getResourceAsStream("MINIFI-216/nifi.properties.before")) {
            pre216Properties.load(pre216PropertiesStream);
        }
        pre216Properties.setProperty(ConfigTransformer.NIFI_VERSION_KEY, ConfigTransformer.NIFI_VERSION);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (InputStream configStream = ConfigTransformerTest.class.getClassLoader().getResourceAsStream("MINIFI-216/configOverrides.yml")) {
            ConfigSchema configSchema = SchemaLoader.loadConfigSchemaFromYaml(configStream);
            assertTrue(configSchema.getNifiPropertiesOverrides().size() > 0);
            for (Map.Entry<String, String> entry : configSchema.getNifiPropertiesOverrides().entrySet()) {
                pre216Properties.setProperty(entry.getKey(), entry.getValue());
            }
            ConfigTransformer.writeNiFiProperties(configSchema, outputStream);
        }
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(outputStream.toByteArray()));

        for (String name : pre216Properties.stringPropertyNames()) {
            assertEquals("Property key " + name + " doesn't match.", pre216Properties.getProperty(name), properties.getProperty(name));
        }
    }

    @Test
    public void testNifiPropertiesVariableRegistry() throws IOException, ConfigurationChangeException, SchemaLoaderException {
        Properties initialProperties = new Properties();
        try (InputStream pre216PropertiesStream = ConfigTransformerTest.class.getClassLoader().getResourceAsStream("MINIFI-277/nifi.properties")) {
            initialProperties.load(pre216PropertiesStream);
        }
        initialProperties.setProperty(ConfigTransformer.NIFI_VERSION_KEY, ConfigTransformer.NIFI_VERSION);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (InputStream configStream = ConfigTransformerTest.class.getClassLoader().getResourceAsStream("MINIFI-277/config.yml")) {
            ConfigSchema configSchema = SchemaLoader.loadConfigSchemaFromYaml(configStream);
            ConfigTransformer.writeNiFiProperties(configSchema, outputStream);
        }
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(outputStream.toByteArray()));

        for (String name : initialProperties.stringPropertyNames()) {
            assertEquals("Property key " + name + " doesn't match.", initialProperties.getProperty(name), properties.getProperty(name));
        }
    }

    @Test
    public void doesTransformFile() throws Exception {
        File inputFile = new File("./src/test/resources/config.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformV1File() throws Exception {
        File inputFile = new File("./src/test/resources/config-v1.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformInputStream() throws Exception {
        File inputFile = new File("./src/test/resources/config.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);

        File nifiPropertiesFile = new File("./target/nifi.properties");
        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnDefaultFile() throws Exception {
        File inputFile = new File("./src/test/resources/default.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnMultipleProcessors() throws Exception {
        File inputFile = new File("./src/test/resources/config-multiple-processors.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnMultipleRemoteProcessGroups() throws Exception {
        File inputFile = new File("./src/test/resources/config-multiple-RPGs.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnMultipleInputPorts() throws Exception {
        File inputFile = new File("./src/test/resources/config-multiple-input-ports.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnMinimal() throws Exception {
        File inputFile = new File("./src/test/resources/config-minimal.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnProvenanceRepository() throws Exception {
        File inputFile = new File("./src/test/resources/config-provenance-repository.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        String nifi = FileUtils.readFileToString(nifiPropertiesFile, Charset.defaultCharset());
        assertTrue(nifi.contains("nifi.provenance.repository.implementation=org.apache.nifi.provenance.WriteAheadProvenanceRepository"));

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnCustomProvenanceRepository() throws Exception {
        File inputFile = new File("./src/test/resources/config-provenance-custom-repository.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        String nifi = FileUtils.readFileToString(nifiPropertiesFile, Charset.defaultCharset());
        assertTrue(nifi.contains("nifi.provenance.repository.implementation=org.apache.nifi.provenance.CustomProvenanceRepository"));

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void handleTransformInvalidFile() throws Exception {
        try {
            File inputFile = new File("./src/test/resources/config-invalid.yml");
            ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
            fail("Invalid configuration file was not detected.");
        } catch (SchemaLoaderException e){
            assertEquals("Provided YAML configuration is not a Map", e.getMessage());
        }
    }

    @Test
    public void handleTransformMalformedField() throws Exception {
        try {
            File inputFile = new File("./src/test/resources/config-malformed-field.yml");
            ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
            fail("Invalid configuration file was not detected.");
        } catch (InvalidConfigurationException e){
            assertEquals("Failed to transform config file due to:['threshold' in section 'Swap' because it is found but could not be parsed as a Number]", e.getMessage());
        }
    }

    @Test
    public void handleTransformEmptyFile() throws Exception {
        try {
            File inputFile = new File("./src/test/resources/config-empty.yml");
            ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
            fail("Invalid configuration file was not detected.");
        } catch (SchemaLoaderException e){
            assertEquals("Provided YAML configuration is not a Map", e.getMessage());
        }
    }

    @Test
    public void handleTransformFileMissingRequiredField() throws Exception {
        try {
            File inputFile = new File("./src/test/resources/config-missing-required-field.yml");
            ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
            fail("Invalid configuration file was not detected.");
        } catch (InvalidConfigurationException e){
            assertEquals("Failed to transform config file due to:['class' in section 'Processors' because it was not found and it is required]", e.getMessage());
        }
    }

    @Test
    public void handleTransformFileMultipleProblems() throws Exception {
        try {
            File inputFile = new File("./src/test/resources/config-multiple-problems.yml");
            ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", null);
            fail("Invalid configuration file was not detected.");
        } catch (InvalidConfigurationException e){
            assertEquals("Failed to transform config file due to:['class' in section 'Processors' because it was not found and it is required], " +
                    "['scheduling strategy' in section 'Provenance Reporting' because it is not a valid scheduling strategy], " +
                    "['source name' in section 'Connections' because it was not found and it is required]", e.getMessage());
        }
    }

    @Test
    public void checkSSLOverrides() throws Exception {
        File inputFile = new File("./src/test/resources/MINIFI-516/config.yml");
        final Properties bootstrapProperties = getTestBootstrapProperties("MINIFI-516/bootstrap.conf");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/", bootstrapProperties);

        // nifi.properties testing
        File nifiPropertiesFile = new File("./target/nifi.properties");
        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        // flow.xml.gz testing
        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        String flow = loadFlowXML(new FileInputStream(flowXml));

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document xml = db.parse(new StringBufferInputStream(flow));

        XPath xPath = XPathFactory.newInstance().newXPath();
        String result = xPath.evaluate("/flowController/rootGroup/processor/property[name = \"SSL Context Service\"]/value/text()", xml);

        assertEquals(result, "SSL-Context-Service");

        flowXml.deleteOnExit();

    }

    public void testConfigFileTransform(String configFile) throws Exception {
        ConfigSchema configSchema = SchemaLoader.loadConfigSchemaFromYaml(ConfigTransformerTest.class.getClassLoader().getResourceAsStream(configFile));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ConfigTransformer.writeFlowXmlFile(configSchema, outputStream);
        Document document = documentBuilder.parse(new ByteArrayInputStream(outputStream.toByteArray()));

        testProcessGroup((Element) xPathFactory.newXPath().evaluate("flowController/rootGroup", document, XPathConstants.NODE), configSchema.getProcessGroupSchema());
        testReportingTasks((Element) xPathFactory.newXPath().evaluate("flowController/reportingTasks", document, XPathConstants.NODE), configSchema.getReportingTasksSchema());
    }

    private void testProcessGroup(Element element, ProcessGroupSchema processGroupSchema) throws XPathExpressionException {
        assertEquals(processGroupSchema.getId(), getText(element, "id"));
        assertEquals(processGroupSchema.getName(), getText(element, "name"));
        assertEquals(nullToEmpty(processGroupSchema.getComment()), nullToEmpty(getText(element, "comment")));

        checkOrderOfChildren(element, PG_ELEMENT_ORDER_MAP);

        NodeList processorElements = (NodeList) xPathFactory.newXPath().evaluate("processor", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getProcessors().size(), processorElements.getLength());
        for (int i = 0; i < processorElements.getLength(); i++) {
            testProcessor((Element) processorElements.item(i), processGroupSchema.getProcessors().get(i));
        }
        NodeList controllerServiceElements = (NodeList) xPathFactory.newXPath().evaluate("controllerService", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getControllerServices().size(), controllerServiceElements.getLength());
        for (int i = 0; i < controllerServiceElements.getLength(); i++) {
            testControllerService((Element) controllerServiceElements.item(i), processGroupSchema.getControllerServices().get(i));
        }

        NodeList remoteProcessGroupElements = (NodeList) xPathFactory.newXPath().evaluate("remoteProcessGroup", element, XPathConstants.NODESET);
        assertEquals(processGroupSchema.getRemoteProcessGroups().size(), remoteProcessGroupElements.getLength());
        for (int i = 0; i < remoteProcessGroupElements.getLength(); i++) {
            testRemoteProcessGroups((Element) remoteProcessGroupElements.item(i), processGroupSchema.getRemoteProcessGroups().get(i));
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
        assertEquals(processorSchema.getAnnotationData(), getText(element, "annotationData"));

        testProperties(element, processorSchema.getProperties());
    }

    private void testReportingTasks(Element element, List<ReportingSchema> reportingSchemas) throws XPathExpressionException {
        NodeList taskElements = (NodeList) xPathFactory.newXPath().evaluate("reportingTask", element, XPathConstants.NODESET);
        assertEquals(reportingSchemas.size(), taskElements.getLength());
        for (int i = 0; i < taskElements.getLength(); i++) {
            testReportingTask((Element) taskElements.item(i), reportingSchemas.get(i));
        }
    }

    private void testReportingTask(Element element, ReportingSchema reportingSchema) throws XPathExpressionException {
        assertEquals(reportingSchema.getId(), getText(element, "id"));
        assertEquals(reportingSchema.getName(), getText(element, "name"));
        assertEquals(reportingSchema.getComment(), getText(element, "comment"));
        assertEquals(reportingSchema.getReportingClass(), getText(element, "class"));
        assertEquals(reportingSchema.getSchedulingPeriod(), getText(element, "schedulingPeriod"));
        assertEquals(reportingSchema.getSchedulingStrategy(), getText(element, "schedulingStrategy"));

        testProperties(element, reportingSchema.getProperties());
    }

    private void testControllerService(Element element, ControllerServiceSchema controllerServiceSchema) throws XPathExpressionException {
        assertEquals(controllerServiceSchema.getId(), getText(element, "id"));
        assertEquals(controllerServiceSchema.getName(), getText(element, "name"));
        assertEquals(controllerServiceSchema.getServiceClass(), getText(element, "class"));
        assertEquals(controllerServiceSchema.getAnnotationData(), getText(element, "annotationData"));

        testProperties(element, controllerServiceSchema.getProperties());
    }

    private void testRemoteProcessGroups(Element element, RemoteProcessGroupSchema remoteProcessingGroupSchema) throws XPathExpressionException {
        assertEquals(remoteProcessingGroupSchema.getId(), getText(element, "id"));
        assertEquals(remoteProcessingGroupSchema.getName(), getText(element, "name"));
        assertEquals(remoteProcessingGroupSchema.getComment(), getText(element, "comment"));
        assertEquals(remoteProcessingGroupSchema.getUrls(), getText(element, "url"));
        assertEquals(remoteProcessingGroupSchema.getTimeout(), getText(element, "timeout"));
        assertEquals(remoteProcessingGroupSchema.getYieldPeriod(), getText(element, "yieldPeriod"));
        assertEquals(remoteProcessingGroupSchema.getTransportProtocol(), getText(element, "transportProtocol"));
        assertEquals(remoteProcessingGroupSchema.getProxyHost(), getText(element, "proxyHost"));
        String proxyPortText = getText(element, "proxyPort");
        assertEquals(remoteProcessingGroupSchema.getProxyPort(), StringUtil.isNullOrEmpty(proxyPortText) ? null : Integer.parseInt(proxyPortText));
        assertEquals(remoteProcessingGroupSchema.getProxyUser(), getText(element, "proxyUser"));
        assertEquals(remoteProcessingGroupSchema.getProxyPassword(), getText(element, "proxyPassword"));

        NodeList inputPortElements = (NodeList) xPathFactory.newXPath().evaluate("inputPort", element, XPathConstants.NODESET);
        assertEquals(remoteProcessingGroupSchema.getInputPorts().size(), inputPortElements.getLength());
        for (int i = 0; i < inputPortElements.getLength(); i++) {
            testRemotePort((Element) inputPortElements.item(i), remoteProcessingGroupSchema.getInputPorts().get(i));
        }

        NodeList outputPortElements = (NodeList) xPathFactory.newXPath().evaluate("outputPort", element, XPathConstants.NODESET);
        assertEquals(remoteProcessingGroupSchema.getOutputPorts().size(), outputPortElements.getLength());
        for (int i = 0; i < outputPortElements.getLength(); i++) {
            testRemotePort((Element) outputPortElements.item(i), remoteProcessingGroupSchema.getOutputPorts().get(i));
        }
    }

    private void testRemotePort(Element element, RemotePortSchema remoteInputPortSchema) throws XPathExpressionException {
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
        assertEquals(expected.entrySet().stream().collect(Collectors.toMap(Map.Entry<String, Object>::getKey, e -> nullToEmpty(e.getValue()))), properties);
    }

    @Test
    public void testContentRepoOverride() throws IOException, ConfigurationChangeException, SchemaLoaderException {
        Properties pre216Properties = new Properties();
        try (InputStream pre216PropertiesStream = ConfigTransformerTest.class.getClassLoader().getResourceAsStream("MINIFI-245/nifi.properties.before")) {
            pre216Properties.load(pre216PropertiesStream);
        }
        pre216Properties.setProperty(ConfigTransformer.NIFI_VERSION_KEY, ConfigTransformer.NIFI_VERSION);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (InputStream configStream = ConfigTransformerTest.class.getClassLoader().getResourceAsStream("MINIFI-245/config.yml")) {
            ConfigTransformer.writeNiFiProperties(SchemaLoader.loadConfigSchemaFromYaml(configStream), outputStream);
        }
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(outputStream.toByteArray()));

        for (String name : pre216Properties.stringPropertyNames()) {
            // Verify the Content Repo property was overridden
            if("nifi.content.repository.implementation".equals(name)) {
                assertNotEquals("Property key " + name + " was not overridden.", pre216Properties.getProperty(name), properties.getProperty(name));
            } else {
                assertEquals("Property key " + name + " doesn't match.", pre216Properties.getProperty(name), properties.getProperty(name));
            }
        }
    }

    @Test
    public void testNullSensitiveKey() throws IOException, ConfigurationChangeException, SchemaLoaderException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (final InputStream configStream = ConfigTransformerTest.class.getClassLoader().getResourceAsStream("MINIFI-537/config.yml")) {
            ConfigTransformer.writeNiFiProperties(SchemaLoader.loadConfigSchemaFromYaml(configStream), outputStream);
        }
        final Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(outputStream.toByteArray()));
        assertEquals("", properties.getProperty("nifi.sensitive.props.key"));
    }

    private String getText(Element element, String path) throws XPathExpressionException {
        return (String) xPathFactory.newXPath().evaluate(path + "/text()", element, XPathConstants.STRING);
    }

    private String nullToEmpty(Object val) {
        return val == null ? "" : val.toString();
    }

    private static Map<String, Integer> generateOrderMap(List<String> elements) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < elements.size(); i++) {
            map.put(elements.get(i), i);
        }
        return Collections.unmodifiableMap(map);
    }

    private static void checkOrderOfChildren(Element element, Map<String, Integer> orderMap) {
        int elementOrderList = 0;
        NodeList childNodes = element.getChildNodes();
        String lastOrderedElementName = null;
        for (int i = 0; i < childNodes.getLength(); i++) {
            String nodeName = childNodes.item(i).getNodeName();
            Integer index = orderMap.get(nodeName);
            if (index != null) {
                if (elementOrderList > index) {
                    fail("Found " + nodeName + " after " + lastOrderedElementName + "; expected all " + nodeName + " elements to come before the following elements: " + orderMap.entrySet().stream()
                            .filter(e -> e.getValue() > index ).sorted(Comparator.comparingInt(e -> e.getValue())).map(e -> e.getKey()).collect(Collectors.joining(", ")));
                }
                lastOrderedElementName = nodeName;
                elementOrderList = index;
            }
        }
    }

    public static Properties getTestBootstrapProperties(final String fileName) throws IOException {
        final Properties bootstrapProperties = new Properties();
        try (final InputStream fis = ConfigTransformerTest.class.getClassLoader().getResourceAsStream(fileName)) {
            bootstrapProperties.load(fis);
        }
        return bootstrapProperties;
    }

    public static String loadFlowXML(InputStream compressedData) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPInputStream gzipInputStream = new GZIPInputStream(compressedData);

        byte[] buffer = new byte[1024];
        int len;
        while ((len = gzipInputStream.read(buffer)) != -1) {
            byteArrayOutputStream.write(buffer, 0, len);
        }

        return byteArrayOutputStream.toString();
    }
}
