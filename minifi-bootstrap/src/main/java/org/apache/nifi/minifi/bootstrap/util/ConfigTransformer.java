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
import org.apache.nifi.minifi.bootstrap.exception.InvalidConfigurationException;
import org.apache.nifi.minifi.commons.schema.ComponentStatusRepositorySchema;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.ContentRepositorySchema;
import org.apache.nifi.minifi.commons.schema.CorePropertiesSchema;
import org.apache.nifi.minifi.commons.schema.FlowControllerSchema;
import org.apache.nifi.minifi.commons.schema.FlowFileRepositorySchema;
import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.ProvenanceReportingSchema;
import org.apache.nifi.minifi.commons.schema.ProvenanceRepositorySchema;
import org.apache.nifi.minifi.commons.schema.RemoteInputPortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessingGroupSchema;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.apache.nifi.minifi.commons.schema.SecurityPropertiesSchema;
import org.apache.nifi.minifi.commons.schema.SensitivePropsSchema;
import org.apache.nifi.minifi.commons.schema.SwapSchema;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public final class ConfigTransformer {
    // Underlying version of NIFI will be using
    public static final String NIFI_VERSION = "0.6.1";

    // Final util classes should have private constructor
    private ConfigTransformer() {
    }

    public static void transformConfigFile(String sourceFile, String destPath) throws Exception {
        File ymlConfigFile = new File(sourceFile);
        InputStream ios = new FileInputStream(ymlConfigFile);

        transformConfigFile(ios, destPath);
    }

    public static void transformConfigFile(InputStream sourceStream, String destPath) throws Exception {
        ConfigSchema configSchema = SchemaLoader.loadConfigSchemaFromYaml(sourceStream);
        if (!configSchema.isValid()) {
            throw new InvalidConfigurationException("Failed to transform config file due to:["
                    + configSchema.getValidationIssues().stream().sorted().collect(Collectors.joining("], [")) + "]");
        }

        // Create nifi.properties and flow.xml.gz in memory
        ByteArrayOutputStream nifiPropertiesOutputStream = new ByteArrayOutputStream();
        writeNiFiProperties(configSchema, nifiPropertiesOutputStream);

        DOMSource flowXml = createFlowXml(configSchema);

        // Write nifi.properties and flow.xml.gz
        writeNiFiPropertiesFile(nifiPropertiesOutputStream, destPath);

        writeFlowXmlFile(flowXml, destPath);
    }

    protected static void writeNiFiPropertiesFile(ByteArrayOutputStream nifiPropertiesOutputStream, String destPath) throws IOException {
        final Path nifiPropertiesPath = Paths.get(destPath, "nifi.properties");
        try (FileOutputStream nifiProperties = new FileOutputStream(nifiPropertiesPath.toString())) {
            nifiPropertiesOutputStream.writeTo(nifiProperties);
        } finally {
            if (nifiPropertiesOutputStream != null) {
                nifiPropertiesOutputStream.flush();
                nifiPropertiesOutputStream.close();
            }
        }
    }

    protected static void writeFlowXmlFile(DOMSource domSource, String path) throws IOException, TransformerException {
        final OutputStream fileOut = Files.newOutputStream(Paths.get(path, "flow.xml.gz"));
        final OutputStream outStream = new GZIPOutputStream(fileOut);
        final StreamResult streamResult = new StreamResult(outStream);

        // configure the transformer and convert the DOM
        final TransformerFactory transformFactory = TransformerFactory.newInstance();
        final Transformer transformer = transformFactory.newTransformer();
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");

        // transform the document to byte stream
        transformer.transform(domSource, streamResult);
        outStream.flush();
        outStream.close();
    }

    protected static void writeNiFiProperties(ConfigSchema configSchema, OutputStream outputStream) throws FileNotFoundException, UnsupportedEncodingException, ConfigurationChangeException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(outputStream, true);

            CorePropertiesSchema coreProperties = configSchema.getCoreProperties();
            FlowFileRepositorySchema flowfileRepoSchema = configSchema.getFlowfileRepositoryProperties();
            SwapSchema swapProperties = flowfileRepoSchema.getSwapProperties();
            ContentRepositorySchema contentRepoProperties = configSchema.getContentRepositoryProperties();
            ComponentStatusRepositorySchema componentStatusRepoProperties = configSchema.getComponentStatusRepositoryProperties();
            SecurityPropertiesSchema securityProperties = configSchema.getSecurityProperties();
            SensitivePropsSchema sensitiveProperties = securityProperties.getSensitiveProps();
            ProvenanceRepositorySchema provenanceRepositorySchema = configSchema.getProvenanceRepositorySchema();

            writer.print(PROPERTIES_FILE_APACHE_2_0_LICENSE);
            writer.println("# Core Properties #");
            writer.println();
            writer.println("nifi.version=" + NIFI_VERSION);
            writer.println("nifi.flow.configuration.file=./conf/flow.xml.gz");
            writer.println("nifi.flow.configuration.archive.dir=./conf/archive/");
            writer.println("nifi.flowcontroller.autoResumeState=true");
            writer.println("nifi.flowcontroller.graceful.shutdown.period=" + coreProperties.getFlowControllerGracefulShutdownPeriod());
            writer.println("nifi.flowservice.writedelay.interval=" + coreProperties.getFlowServiceWriteDelayInterval());
            writer.println("nifi.administrative.yield.duration=" + coreProperties.getAdministrativeYieldDuration());
            writer.println("# If a component has no work to do (is \"bored\"), how long should we wait before checking again for work?");
            writer.println("nifi.bored.yield.duration=" + coreProperties.getBoredYieldDuration());
            writer.println();
            writer.println("nifi.authority.provider.configuration.file=./conf/authority-providers.xml");
            writer.println("nifi.login.identity.provider.configuration.file=./conf/login-identity-providers.xml");
            writer.println("nifi.templates.directory=./conf/templates");
            writer.println("nifi.ui.banner.text= ");
            writer.println("nifi.ui.autorefresh.interval=30 sec");
            writer.println("nifi.nar.library.directory=./lib");
            writer.println("nifi.nar.working.directory=./work/nar/");
            writer.println("nifi.documentation.working.directory=./work/docs/components");
            writer.println();
            writer.println("####################");
            writer.println("# State Management #");
            writer.println("####################");
            writer.println("nifi.state.management.configuration.file=./conf/state-management.xml");
            writer.println("# The ID of the local state provider");
            writer.println("nifi.state.management.provider.local=local-provider");
            writer.println();
            writer.println("# H2 Settings");
            writer.println("nifi.database.directory=./database_repository");
            writer.println("nifi.h2.url.append=;LOCK_TIMEOUT=25000;WRITE_DELAY=0;AUTO_SERVER=FALSE");
            writer.println();
            writer.println("# FlowFile Repository");
            writer.println("nifi.flowfile.repository.implementation=org.apache.nifi.controller.repository.WriteAheadFlowFileRepository");
            writer.println("nifi.flowfile.repository.directory=./flowfile_repository");
            writer.println("nifi.flowfile.repository.partitions=" + flowfileRepoSchema.getPartitions());
            writer.println("nifi.flowfile.repository.checkpoint.interval=" + flowfileRepoSchema.getCheckpointInterval());
            writer.println("nifi.flowfile.repository.always.sync=" + flowfileRepoSchema.getAlwaysSync());
            writer.println();
            writer.println("nifi.swap.manager.implementation=org.apache.nifi.controller.FileSystemSwapManager");
            writer.println("nifi.queue.swap.threshold=" + swapProperties.getThreshold());
            writer.println("nifi.swap.in.period=" + swapProperties.getInPeriod());
            writer.println("nifi.swap.in.threads=" + swapProperties.getInThreads());
            writer.println("nifi.swap.out.period=" + swapProperties.getOutPeriod());
            writer.println("nifi.swap.out.threads=" + swapProperties.getOutThreads());
            writer.println();
            writer.println("# Content Repository");
            writer.println("nifi.content.repository.implementation=org.apache.nifi.controller.repository.FileSystemRepository");
            writer.println("nifi.content.claim.max.appendable.size=" + contentRepoProperties.getContentClaimMaxAppendableSize());
            writer.println("nifi.content.claim.max.flow.files=" + contentRepoProperties.getContentClaimMaxFlowFiles());
            writer.println("nifi.content.repository.archive.max.retention.period=");
            writer.println("nifi.content.repository.archive.max.usage.percentage=");
            writer.println("nifi.content.repository.archive.enabled=false");
            writer.println("nifi.content.repository.directory.default=./content_repository");
            writer.println("nifi.content.repository.always.sync=" + contentRepoProperties.getAlwaysSync());
            writer.println();
            writer.println("# Provenance Repository Properties");
            writer.println("nifi.provenance.repository.implementation=org.apache.nifi.provenance.MiNiFiPersistentProvenanceRepository");
            writer.println("nifi.provenance.repository.rollover.time=" + provenanceRepositorySchema.getProvenanceRepoRolloverTimeKey());
            writer.println();
            writer.println("# Volatile Provenance Respository Properties");
            writer.println("nifi.provenance.repository.buffer.size=10000");
            writer.println();
            writer.println("# Component Status Repository");
            writer.println("nifi.components.status.repository.implementation=org.apache.nifi.controller.status.history.VolatileComponentStatusRepository");
            writer.println("nifi.components.status.repository.buffer.size=" + componentStatusRepoProperties.getBufferSize());
            writer.println("nifi.components.status.snapshot.frequency=" + componentStatusRepoProperties.getSnapshotFrequency());
            writer.println();
            writer.println("# web properties #");
            writer.println("nifi.web.war.directory=./lib");
            writer.println("nifi.web.http.host=");
            writer.println("nifi.web.http.port=8081");
            writer.println("nifi.web.https.host=");
            writer.println("nifi.web.https.port=");
            writer.println("nifi.web.jetty.working.directory=./work/jetty");
            writer.println("nifi.web.jetty.threads=200");
            writer.println();
            writer.println("# security properties #");
            writer.println("nifi.sensitive.props.key=" + sensitiveProperties.getKey());
            writer.println("nifi.sensitive.props.algorithm=" + sensitiveProperties.getAlgorithm());
            writer.println("nifi.sensitive.props.provider=" + sensitiveProperties.getProvider());
            writer.println();
            writer.println("nifi.security.keystore=" + securityProperties.getKeystore());
            writer.println("nifi.security.keystoreType=" + securityProperties.getKeystoreType());
            writer.println("nifi.security.keystorePasswd=" + securityProperties.getKeystorePassword());
            writer.println("nifi.security.keyPasswd=" + securityProperties.getKeyPassword());
            writer.println("nifi.security.truststore=" + securityProperties.getTruststore());
            writer.println("nifi.security.truststoreType=" + securityProperties.getTruststoreType());
            writer.println("nifi.security.truststorePasswd=" + securityProperties.getTruststorePassword());
            writer.println("nifi.security.needClientAuth=");
            writer.println("nifi.security.user.credential.cache.duration=24 hours");
            writer.println("nifi.security.user.authority.provider=file-provider");
            writer.println("nifi.security.user.login.identity.provider=");
            writer.println("nifi.security.support.new.account.requests=");
            writer.println("# Valid Authorities include: ROLE_MONITOR,ROLE_DFM,ROLE_ADMIN,ROLE_PROVENANCE,ROLE_NIFI");
            writer.println("nifi.security.anonymous.authorities=");
            writer.println("nifi.security.ocsp.responder.url=");
            writer.println("nifi.security.ocsp.responder.certificate=");
            writer.println();
            writer.println();
            writer.println("# cluster node properties (only configure for cluster nodes) #");
            writer.println("nifi.cluster.is.node=false");
            writer.println();
            writer.println("# cluster manager properties (only configure for cluster manager) #");
            writer.println("nifi.cluster.is.manager=false");
        } catch (NullPointerException e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while creating the nifi.properties", e);
        } finally {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }

    protected static DOMSource createFlowXml(ConfigSchema configSchema) throws IOException, ConfigurationChangeException, ConfigTransformerException{
        try {
            // create a new, empty document
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);

            final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            final Document doc = docBuilder.newDocument();

            // populate document with controller state
            final Element rootNode = doc.createElement("flowController");
            doc.appendChild(rootNode);
            CorePropertiesSchema coreProperties = configSchema.getCoreProperties();
            addTextElement(rootNode, "maxTimerDrivenThreadCount", String.valueOf(coreProperties.getMaxConcurrentThreads()));
            addTextElement(rootNode, "maxEventDrivenThreadCount", String.valueOf(coreProperties.getMaxConcurrentThreads()));
            addProcessGroup(rootNode, configSchema, "rootGroup");

            SecurityPropertiesSchema securityProperties = configSchema.getSecurityProperties();
            if (securityProperties.useSSL()) {
                final Element controllerServicesNode = doc.createElement("controllerServices");
                rootNode.appendChild(controllerServicesNode);
                addSSLControllerService(controllerServicesNode, securityProperties);
            }

            ProvenanceReportingSchema provenanceProperties = configSchema.getProvenanceReportingProperties();
            if (provenanceProperties != null) {
                final Element reportingTasksNode = doc.createElement("reportingTasks");
                rootNode.appendChild(reportingTasksNode);
                addProvenanceReportingTask(reportingTasksNode, configSchema);
            }

            return new DOMSource(doc);
        } catch (final ParserConfigurationException | DOMException | TransformerFactoryConfigurationError | IllegalArgumentException e) {
            throw new ConfigTransformerException(e);
        } catch (Exception e){
            throw new ConfigTransformerException("Failed to parse the config YAML while writing the top level of the flow xml", e);
        }
    }

    protected static void addSSLControllerService(final Element element, SecurityPropertiesSchema securityProperties) throws ConfigurationChangeException {
        try {
            final Element serviceElement = element.getOwnerDocument().createElement("controllerService");
            addTextElement(serviceElement, "id", "SSL-Context-Service");
            addTextElement(serviceElement, "name", "SSL-Context-Service");
            addTextElement(serviceElement, "comment", "");
            addTextElement(serviceElement, "class", "org.apache.nifi.ssl.StandardSSLContextService");

            addTextElement(serviceElement, "enabled", "true");

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("Keystore Filename", securityProperties.getKeystore());
            attributes.put("Keystore Type", securityProperties.getKeystoreType());
            attributes.put("Keystore Password", securityProperties.getKeyPassword());
            attributes.put("Truststore Filename", securityProperties.getTruststore());
            attributes.put("Truststore Type", securityProperties.getTruststoreType());
            attributes.put("Truststore Password", securityProperties.getTruststorePassword());
            attributes.put("SSL Protocol", securityProperties.getSslProtocol());

            addConfiguration(serviceElement, attributes);

            element.appendChild(serviceElement);
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to create an SSL Controller Service", e);
        }
    }

    protected static void addProcessGroup(final Element parentElement, ConfigSchema configSchema, final String elementName) throws ConfigurationChangeException {
        try {
            FlowControllerSchema flowControllerProperties = configSchema.getFlowControllerProperties();

            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement(elementName);
            parentElement.appendChild(element);
            addTextElement(element, "id", "Root-Group");
            addTextElement(element, "name", flowControllerProperties.getName());
            addPosition(element);
            addTextElement(element, "comment", flowControllerProperties.getComment());

            List<ProcessorSchema> processors = configSchema.getProcessors();
            if (processors != null) {
                for (ProcessorSchema processorConfig : processors) {
                    addProcessor(element, processorConfig);
                }
            }

            List<RemoteProcessingGroupSchema> remoteProcessingGroups = configSchema.getRemoteProcessingGroups();
            if (remoteProcessingGroups != null) {
                for (RemoteProcessingGroupSchema remoteProcessingGroupSchema : remoteProcessingGroups) {
                    addRemoteProcessGroup(element, remoteProcessingGroupSchema);
                }
            }

            List<ConnectionSchema> connections = configSchema.getConnections();
            if (connections != null) {
                for (ConnectionSchema connectionConfig : connections) {
                    addConnection(element, connectionConfig, configSchema);
                }
            }
        } catch (ConfigurationChangeException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to creating the root Process Group", e);
        }
    }

    protected static void addProcessor(final Element parentElement, ProcessorSchema processorConfig) throws ConfigurationChangeException {
        try {
            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement("processor");
            parentElement.appendChild(element);

            addTextElement(element, "id", processorConfig.getId());
            addTextElement(element, "name", processorConfig.getName());

            addPosition(element);
            addStyle(element);

            addTextElement(element, "comment", "");
            addTextElement(element, "class", processorConfig.getProcessorClass());
            addTextElement(element, "maxConcurrentTasks", String.valueOf(processorConfig.getMaxConcurrentTasks()));
            addTextElement(element, "schedulingPeriod", processorConfig.getSchedulingPeriod());
            addTextElement(element, "penalizationPeriod", processorConfig.getPenalizationPeriod());
            addTextElement(element, "yieldPeriod", processorConfig.getYieldPeriod());
            addTextElement(element, "bulletinLevel", "WARN");
            addTextElement(element, "lossTolerant", "false");
            addTextElement(element, "scheduledState", "RUNNING");
            addTextElement(element, "schedulingStrategy", processorConfig.getSchedulingStrategy());
            addTextElement(element, "runDurationNanos", String.valueOf(processorConfig.getRunDurationNanos()));

            String annotationData = processorConfig.getAnnotationData();
            if(annotationData != null && !annotationData.isEmpty()) {
                addTextElement(element, "annotationData", annotationData);
            }

            addConfiguration(element, processorConfig.getProperties());

            Collection<String> autoTerminatedRelationships = processorConfig.getAutoTerminatedRelationshipsList();
            if (autoTerminatedRelationships != null) {
                for (String rel : autoTerminatedRelationships) {
                    addTextElement(element, "autoTerminatedRelationship", rel);
                }
            }
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add a Processor", e);
        }
    }

    protected static void addProvenanceReportingTask(final Element element, ConfigSchema configSchema) throws ConfigurationChangeException {
        try {
            ProvenanceReportingSchema provenanceProperties = configSchema.getProvenanceReportingProperties();
            final Element taskElement = element.getOwnerDocument().createElement("reportingTask");
            addTextElement(taskElement, "id", "Provenance-Reporting");
            addTextElement(taskElement, "name", "Site-To-Site-Provenance-Reporting");
            addTextElement(taskElement, "comment", provenanceProperties.getComment());
            addTextElement(taskElement, "class", "org.apache.nifi.minifi.provenance.reporting.ProvenanceReportingTask");
            addTextElement(taskElement, "schedulingPeriod", provenanceProperties.getSchedulingPeriod());
            addTextElement(taskElement, "scheduledState", "RUNNING");
            addTextElement(taskElement, "schedulingStrategy", provenanceProperties.getSchedulingStrategy());

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("Destination URL", provenanceProperties.getDestinationUrl());
            attributes.put("Input Port Name", provenanceProperties.getPortName());
            attributes.put("MiNiFi URL", provenanceProperties.getOriginatingUrl());
            attributes.put("Compress Events", provenanceProperties.getUseCompression());
            attributes.put("Batch Size", provenanceProperties.getBatchSize());
            attributes.put("Communications Timeout", provenanceProperties.getTimeout());

            SecurityPropertiesSchema securityProps = configSchema.getSecurityProperties();
            if (securityProps.useSSL()) {
                attributes.put("SSL Context Service", "SSL-Context-Service");
            }

            addConfiguration(taskElement, attributes);

            element.appendChild(taskElement);
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the Provenance Reporting Task", e);
        }
    }

    protected static void addConfiguration(final Element element, Map<String, Object> elementConfig) {
        final Document doc = element.getOwnerDocument();
        if (elementConfig == null) {
            return;
        }
        for (final Map.Entry<String, Object> entry : elementConfig.entrySet()) {

            final Element propElement = doc.createElement("property");
            addTextElement(propElement, "name", entry.getKey());
            if (entry.getValue() != null) {
                addTextElement(propElement, "value", entry.getValue().toString());
            }

            element.appendChild(propElement);
        }
    }

    protected static void addStyle(final Element parentElement) {
        final Element element = parentElement.getOwnerDocument().createElement("styles");
        parentElement.appendChild(element);
    }

    protected static void addRemoteProcessGroup(final Element parentElement, RemoteProcessingGroupSchema remoteProcessingGroupProperties) throws ConfigurationChangeException {
        try {
            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement("remoteProcessGroup");
            parentElement.appendChild(element);
            addTextElement(element, "id", remoteProcessingGroupProperties.getName());
            addTextElement(element, "name", remoteProcessingGroupProperties.getName());
            addPosition(element);
            addTextElement(element, "comment", remoteProcessingGroupProperties.getComment());
            addTextElement(element, "url", remoteProcessingGroupProperties.getUrl());
            addTextElement(element, "timeout", remoteProcessingGroupProperties.getTimeout());
            addTextElement(element, "yieldPeriod", remoteProcessingGroupProperties.getYieldPeriod());
            addTextElement(element, "transmitting", "true");

            List<RemoteInputPortSchema> remoteInputPorts = remoteProcessingGroupProperties.getInputPorts();
            for (RemoteInputPortSchema remoteInputPortSchema : remoteInputPorts) {
                addRemoteGroupPort(element, remoteInputPortSchema);
            }

            parentElement.appendChild(element);
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the Remote Process Group", e);
        }
    }

    protected static void addRemoteGroupPort(final Element parentElement, RemoteInputPortSchema inputPort) throws ConfigurationChangeException {
        try {
            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement("inputPort");
            parentElement.appendChild(element);
            addTextElement(element, "id", inputPort.getId());
            addTextElement(element, "name", inputPort.getName());
            addPosition(element);
            addTextElement(element, "comments", inputPort.getComment());
            addTextElement(element, "scheduledState", "RUNNING");
            addTextElement(element, "maxConcurrentTasks", String.valueOf(inputPort.getMax_concurrent_tasks()));
            addTextElement(element, "useCompression", String.valueOf(inputPort.getUseCompression()));

            parentElement.appendChild(element);
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the input port of the Remote Process Group", e);
        }
    }

    protected static void addConnection(final Element parentElement, ConnectionSchema connectionProperties, ConfigSchema configSchema) throws ConfigurationChangeException {
        try {
            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement("connection");
            parentElement.appendChild(element);

            addTextElement(element, "id", connectionProperties.getId());
            addTextElement(element, "name", connectionProperties.getName());

            final Element bendPointsElement = doc.createElement("bendPoints");
            element.appendChild(bendPointsElement);

            addTextElement(element, "labelIndex", "1");
            addTextElement(element, "zIndex", "0");

            addTextElement(element, "sourceId", connectionProperties.getSourceId());
            addTextElement(element, "sourceGroupId", "Root-Group");
            addTextElement(element, "sourceType", "PROCESSOR");

            final String connectionDestinationId = connectionProperties.getDestinationId();
            addTextElement(element, "destinationId", connectionDestinationId);
            final Optional<String> parentGroup = findInputPortParentGroup(connectionDestinationId, configSchema);
            if (parentGroup.isPresent()) {
                addTextElement(element, "destinationGroupId", parentGroup.get());
                addTextElement(element, "destinationType", "REMOTE_INPUT_PORT");
            } else {
                addTextElement(element, "destinationGroupId", "Root-Group");
                addTextElement(element, "destinationType", "PROCESSOR");
            }

            for (String relationshipName : connectionProperties.getSourceRelationshipNames()) {
                addTextElement(element, "relationship", relationshipName);
            }

            addTextElement(element, "maxWorkQueueSize", String.valueOf(connectionProperties.getMaxWorkQueueSize()));
            addTextElement(element, "maxWorkQueueDataSize", connectionProperties.getMaxWorkQueueDataSize());

            addTextElement(element, "flowFileExpiration", connectionProperties.getFlowfileExpiration());
            addTextElementIfNotNullOrEmpty(element, "queuePrioritizerClass", connectionProperties.getQueuePrioritizerClass());

            parentElement.appendChild(element);
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the connection from the Processor to the input port of the Remote Process Group", e);
        }
    }

    // Locate the associated parent group for a given input port by its id
    protected static Optional<String> findInputPortParentGroup(String inputPortId, ConfigSchema configSchema) {
        final List<RemoteProcessingGroupSchema> remoteProcessingGroups = configSchema.getRemoteProcessingGroups();
        if (remoteProcessingGroups != null) {
            for (final RemoteProcessingGroupSchema remoteProcessingGroupSchema : remoteProcessingGroups) {
                final List<RemoteInputPortSchema> remoteInputPorts = remoteProcessingGroupSchema.getInputPorts();
                for (final RemoteInputPortSchema remoteInputPortSchema : remoteInputPorts) {
                    if (remoteInputPortSchema != null && inputPortId.equals(remoteInputPortSchema.getId())) {
                        return Optional.of(remoteProcessingGroupSchema.getName());

                    }
                }
            }
        }

        return Optional.empty();
    }

    protected static void addPosition(final Element parentElement) {
        final Element element = parentElement.getOwnerDocument().createElement("position");
        element.setAttribute("x", String.valueOf("0"));
        element.setAttribute("y", String.valueOf("0"));
        parentElement.appendChild(element);
    }

    protected static void addTextElementIfNotNullOrEmpty(final Element element, final String name, final String value) {
        if (!StringUtil.isNullOrEmpty(value)) {
            addTextElement(element, name, value);
        }
    }

    protected static void addTextElement(final Element element, final String name, final String value) {
        final Document doc = element.getOwnerDocument();
        final Element toAdd = doc.createElement(name);
        toAdd.setTextContent(value);
        element.appendChild(toAdd);
    }

    public static final String PROPERTIES_FILE_APACHE_2_0_LICENSE =
            "# Licensed to the Apache Software Foundation (ASF) under one or more\n" +
            "# contributor license agreements.  See the NOTICE file distributed with\n" +
            "# this work for additional information regarding copyright ownership.\n" +
            "# The ASF licenses this file to You under the Apache License, Version 2.0\n" +
            "# (the \"License\"); you may not use this file except in compliance with\n" +
            "# the License.  You may obtain a copy of the License at\n" +
            "#\n" +
            "#     http://www.apache.org/licenses/LICENSE-2.0\n" +
            "#\n" +
            "# Unless required by applicable law or agreed to in writing, software\n" +
            "# distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
            "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
            "# See the License for the specific language governing permissions and\n" +
            "# limitations under the License.\n"+
            "\n";

}
