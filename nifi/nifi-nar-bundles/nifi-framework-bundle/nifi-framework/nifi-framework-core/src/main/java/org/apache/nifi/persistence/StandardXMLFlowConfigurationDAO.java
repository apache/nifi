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
package org.apache.nifi.persistence;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.FlowSerializationException;
import org.apache.nifi.controller.FlowSynchronizationException;
import org.apache.nifi.controller.FlowSynchronizer;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.StandardFlowSerializer;
import org.apache.nifi.controller.StandardFlowSynchronizer;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.reporting.StandardReportingInitializationContext;
import org.apache.nifi.controller.service.ControllerServiceLoader;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.util.NiFiProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public final class StandardXMLFlowConfigurationDAO implements FlowConfigurationDAO {

    public static final String CONFIGURATION_ARCHIVE_DIR_KEY = "nifi.flow.configuration.archive.dir";

    private final Path flowXmlPath;
    private final Path taskConfigXmlPath;
    private final ControllerServiceLoader servicerLoader;
    private final StringEncryptor encryptor;

    private static final Logger LOG = LoggerFactory.getLogger(StandardXMLFlowConfigurationDAO.class);

    public StandardXMLFlowConfigurationDAO(final Path flowXml, final Path taskConfigXml, final Path serviceConfigXml, final StringEncryptor encryptor) throws IOException {
        final File flowXmlFile = flowXml.toFile();
        if (!flowXmlFile.exists()) {
            Files.createDirectories(flowXml.getParent());
            Files.createFile(flowXml);
            //TODO: find a better solution. With Windows 7 and Java 7, Files.isWritable(source.getParent()) returns false, even when it should be true.
        } else if (!flowXmlFile.canRead() || !flowXmlFile.canWrite()) {
            throw new IOException(flowXml + " exists but you have insufficient read/write privileges");
        }

        final File taskConfigXmlFile = Objects.requireNonNull(taskConfigXml).toFile();
        if ((!taskConfigXmlFile.exists() || !taskConfigXmlFile.canRead())) {
            throw new IOException(taskConfigXml + " does not appear to exist or cannot be read. Cannot load configuration.");
        }

        this.flowXmlPath = flowXml;
        this.taskConfigXmlPath = taskConfigXml;
        this.servicerLoader = new ControllerServiceLoader(serviceConfigXml);
        this.encryptor = encryptor;
    }

    @Override
    public synchronized void load(final FlowController controller, final DataFlow dataFlow)
            throws IOException, FlowSerializationException, FlowSynchronizationException, UninheritableFlowException {

        final FlowSynchronizer flowSynchronizer = new StandardFlowSynchronizer(encryptor);
        controller.synchronize(flowSynchronizer, dataFlow);
        save(new ByteArrayInputStream(dataFlow.getFlow()));
    }

    @Override
    public synchronized void load(final OutputStream os) throws IOException {
        try (final InputStream inStream = Files.newInputStream(flowXmlPath, StandardOpenOption.READ);
                final InputStream gzipIn = new GZIPInputStream(inStream)) {
            FileUtils.copy(gzipIn, os);
        }
    }

    @Override
    public synchronized void save(final InputStream is) throws IOException {
        try (final OutputStream outStream = Files.newOutputStream(flowXmlPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                final OutputStream gzipOut = new GZIPOutputStream(outStream)) {
            FileUtils.copy(is, gzipOut);
        }
    }

    @Override
    public void save(final FlowController flow) throws IOException {
        LOG.trace("Saving flow to disk");
        try (final OutputStream outStream = Files.newOutputStream(flowXmlPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                final OutputStream gzipOut = new GZIPOutputStream(outStream)) {
            save(flow, gzipOut);
        }
        LOG.debug("Finished saving flow to disk");
    }

    @Override
    public synchronized void save(final FlowController flow, final OutputStream os) throws IOException {
        try {
            final StandardFlowSerializer xmlTransformer = new StandardFlowSerializer(encryptor);
            flow.serialize(xmlTransformer, os);
        } catch (final FlowSerializationException fse) {
            throw new IOException(fse);
        }
    }

    @Override
    public synchronized void save(final FlowController controller, final boolean archive) throws IOException {
        if (null == controller) {
            throw new NullPointerException();
        }

        Path tempFile;
        Path configFile;

        configFile = flowXmlPath;
        tempFile = configFile.getParent().resolve(configFile.toFile().getName() + ".new.xml.gz");

        try (final OutputStream fileOut = Files.newOutputStream(tempFile);
                final OutputStream outStream = new GZIPOutputStream(fileOut)) {

            final StandardFlowSerializer xmlTransformer = new StandardFlowSerializer(encryptor);
            controller.serialize(xmlTransformer, outStream);

            Files.deleteIfExists(configFile);
            FileUtils.renameFile(tempFile.toFile(), configFile.toFile(), 5, true);
        } catch (final FlowSerializationException fse) {
            throw new IOException(fse);
        } finally {
            Files.deleteIfExists(tempFile);
        }

        if (archive) {
            try {
                final String archiveDirVal = NiFiProperties.getInstance().getProperty(CONFIGURATION_ARCHIVE_DIR_KEY);
                final Path archiveDir = (archiveDirVal == null || archiveDirVal.equals("")) ? configFile.getParent().resolve("archive") : new File(archiveDirVal).toPath();
                Files.createDirectories(archiveDir);

                if (!Files.isDirectory(archiveDir)) {
                    throw new IOException("Archive directory doesn't appear to be a directory " + archiveDir);
                }
                final Path archiveFile = archiveDir.resolve(System.nanoTime() + "-" + configFile.toFile().getName());
                Files.copy(configFile, archiveFile);
            } catch (final Exception ex) {
                LOG.warn("Unable to archive flow configuration as requested due to " + ex);
                if (LOG.isDebugEnabled()) {
                    LOG.warn("", ex);
                }
            }
        }
    }

    @Override
    public List<ReportingTaskNode> loadReportingTasks(final FlowController controller) {
        final List<ReportingTaskNode> tasks = new ArrayList<>();
        if (taskConfigXmlPath == null) {
            LOG.info("No reporting tasks to start");
            return tasks;
        }

        try {
            final URL schemaUrl = getClass().getResource("/ReportingTaskConfiguration.xsd");
            final Document document = parse(taskConfigXmlPath.toFile(), schemaUrl);

            final NodeList tasksNodes = document.getElementsByTagName("tasks");
            final Element tasksElement = (Element) tasksNodes.item(0);

            for (final Element taskElement : DomUtils.getChildElementsByTagName(tasksElement, "task")) {
                //add global properties common to all tasks
                Map<String, String> properties = new HashMap<>();

                //get properties for the specific reporting task - id, name, class,
                //and schedulingPeriod must be set
                final String taskId = DomUtils.getChild(taskElement, "id").getTextContent().trim();
                final String taskName = DomUtils.getChild(taskElement, "name").getTextContent().trim();

                final List<Element> schedulingStrategyNodeList = DomUtils.getChildElementsByTagName(taskElement, "schedulingStrategy");
                String schedulingStrategyValue = SchedulingStrategy.TIMER_DRIVEN.name();
                if (schedulingStrategyNodeList.size() == 1) {
                    final String specifiedValue = schedulingStrategyNodeList.get(0).getTextContent();

                    try {
                        schedulingStrategyValue = SchedulingStrategy.valueOf(specifiedValue).name();
                    } catch (final Exception e) {
                        throw new RuntimeException("Cannot start Reporting Task with id " + taskId + " because its Scheduling Strategy does not have a valid value", e);
                    }
                }

                final SchedulingStrategy schedulingStrategy = SchedulingStrategy.valueOf(schedulingStrategyValue);
                final String taskSchedulingPeriod = DomUtils.getChild(taskElement, "schedulingPeriod").getTextContent().trim();
                final String taskClass = DomUtils.getChild(taskElement, "class").getTextContent().trim();

                //optional task-specific properties
                for (final Element optionalProperty : DomUtils.getChildElementsByTagName(taskElement, "property")) {
                    final String name = optionalProperty.getAttribute("name");
                    final String value = optionalProperty.getTextContent().trim();
                    properties.put(name, value);
                }

                //set the class to be used for the configured reporting task
                final ReportingTaskNode reportingTaskNode;
                try {
                    reportingTaskNode = controller.createReportingTask(taskClass, taskId);
                } catch (final ReportingTaskInstantiationException e) {
                    LOG.error("Unable to load reporting task {} due to {}", new Object[]{taskId, e});
                    if (LOG.isDebugEnabled()) {
                        LOG.error("", e);
                    }
                    continue;
                }

                reportingTaskNode.setName(taskName);
                reportingTaskNode.setScheduldingPeriod(taskSchedulingPeriod);
                reportingTaskNode.setSchedulingStrategy(schedulingStrategy);

                final ReportingTask reportingTask = reportingTaskNode.getReportingTask();

                final ReportingInitializationContext config = new StandardReportingInitializationContext(taskId, taskName, schedulingStrategy, taskSchedulingPeriod, controller);
                reportingTask.initialize(config);

                final Map<PropertyDescriptor, String> resolvedProps;
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    resolvedProps = new HashMap<>();
                    for (final Map.Entry<String, String> entry : properties.entrySet()) {
                        final PropertyDescriptor descriptor = reportingTask.getPropertyDescriptor(entry.getKey());
                        resolvedProps.put(descriptor, entry.getValue());
                    }
                }

                for (final Map.Entry<PropertyDescriptor, String> entry : resolvedProps.entrySet()) {
                    reportingTaskNode.setProperty(entry.getKey().getName(), entry.getValue());
                }

                tasks.add(reportingTaskNode);
                controller.startReportingTask(reportingTaskNode);
            }
        } catch (final SAXException | ParserConfigurationException | IOException | DOMException | NumberFormatException | InitializationException t) {
            LOG.error("Unable to load reporting tasks from {} due to {}", new Object[]{taskConfigXmlPath, t});
            if (LOG.isDebugEnabled()) {
                LOG.error("", t);
            }
        }

        return tasks;
    }

    @Override
    public List<ControllerServiceNode> loadControllerServices(final FlowController controller) throws IOException {
        return servicerLoader.loadControllerServices(controller);
    }

    private Document parse(final File xmlFile, final URL schemaUrl) throws SAXException, ParserConfigurationException, IOException {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema = schemaFactory.newSchema(schemaUrl);
        final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setSchema(schema);
        final DocumentBuilder builder = docFactory.newDocumentBuilder();

        builder.setErrorHandler(new org.xml.sax.ErrorHandler() {
            @Override
            public void fatalError(final SAXParseException err) throws SAXException {
                LOG.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.error("Error Stack Dump", err);
                }
                throw err;
            }

            @Override
            public void error(final SAXParseException err) throws SAXParseException {
                LOG.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.error("Error Stack Dump", err);
                }
                throw err;
            }

            @Override
            public void warning(final SAXParseException err) throws SAXParseException {
                LOG.warn(" Config file line " + err.getLineNumber() + ", uri " + err.getSystemId() + " : message : " + err.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.warn("Warning stack dump", err);
                }
                throw err;
            }
        });

        // build the docuemnt
        final Document document = builder.parse(xmlFile);

        // ensure schema compliance
        final Validator validator = schema.newValidator();
        validator.validate(new DOMSource(document));

        return document;
    }
}
