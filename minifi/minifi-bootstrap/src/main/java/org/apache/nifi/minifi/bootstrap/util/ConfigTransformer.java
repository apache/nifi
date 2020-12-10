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


import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.exception.InvalidConfigurationException;
import org.apache.nifi.minifi.commons.schema.ComponentStatusRepositorySchema;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.ContentRepositorySchema;
import org.apache.nifi.minifi.commons.schema.ControllerServiceSchema;
import org.apache.nifi.minifi.commons.schema.CorePropertiesSchema;
import org.apache.nifi.minifi.commons.schema.FlowControllerSchema;
import org.apache.nifi.minifi.commons.schema.FlowFileRepositorySchema;
import org.apache.nifi.minifi.commons.schema.FunnelSchema;
import org.apache.nifi.minifi.commons.schema.PortSchema;
import org.apache.nifi.minifi.commons.schema.ProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.ProvenanceReportingSchema;
import org.apache.nifi.minifi.commons.schema.ProvenanceRepositorySchema;
import org.apache.nifi.minifi.commons.schema.RemotePortSchema;
import org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.commons.schema.ReportingSchema;
import org.apache.nifi.minifi.commons.schema.SecurityPropertiesSchema;
import org.apache.nifi.minifi.commons.schema.SensitivePropsSchema;
import org.apache.nifi.minifi.commons.schema.SwapSchema;
import org.apache.nifi.minifi.commons.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.commons.schema.common.Schema;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public final class ConfigTransformer {
    // Underlying version of NIFI will be using
    public static final String NIFI_VERSION = "1.8.0";
    public static final String ROOT_GROUP = "Root-Group";
    public static final String NIFI_VERSION_KEY = "nifi.version";

    public static final Logger logger = LoggerFactory.getLogger(ConfigTransformer.class);

    // Final util classes should have private constructor
    private ConfigTransformer() {
    }

    public static void transformConfigFile(InputStream sourceStream, String destPath, Properties bootstrapProperties) throws Exception {
        ConvertableSchema<ConfigSchema> convertableSchema = throwIfInvalid(SchemaLoader.loadConvertableSchemaFromYaml(sourceStream));
        ConfigSchema configSchema = throwIfInvalid(convertableSchema.convert());

        SecurityPropertiesSchema securityProperties = BootstrapTransformer.buildSecurityPropertiesFromBootstrap(bootstrapProperties).orElse(null);
        ProvenanceReportingSchema provenanceReportingProperties = BootstrapTransformer.buildProvenanceReportingPropertiesFromBootstrap(bootstrapProperties).orElse(null);

        // See if we are providing defined properties from the filesystem configurations and use those as the definitive values
        if (securityProperties != null) {
            configSchema.setSecurityProperties(securityProperties);
            logger.info("Bootstrap flow override: Replaced security properties");
        }
        if (provenanceReportingProperties != null) {
            configSchema.setProvenanceReportingProperties(provenanceReportingProperties);
            logger.info("Bootstrap flow override: Replaced provenance reporting properties");
        }

        // Replace all processor SSL controller services with MiNiFi parent, if bootstrap boolean is set to true
        if (BootstrapTransformer.processorSSLOverride(bootstrapProperties)) {
            for (ProcessorSchema processorConfig : configSchema.getProcessGroupSchema().getProcessors()) {
                processorConfig.getProperties().replace("SSL Context Service", processorConfig.getProperties().get("SSL Context Service"), "SSL-Context-Service");
                logger.info("Bootstrap flow override: Replaced {} SSL Context Service with parent MiNiFi SSL", processorConfig.getName());
            }
        }

        // Create nifi.properties and flow.xml.gz in memory
        ByteArrayOutputStream nifiPropertiesOutputStream = new ByteArrayOutputStream();
        writeNiFiProperties(configSchema, nifiPropertiesOutputStream);

        writeFlowXmlFile(configSchema, destPath);

        // Write nifi.properties and flow.xml.gz
        writeNiFiPropertiesFile(nifiPropertiesOutputStream, destPath);
    }

    private static <T extends Schema> T throwIfInvalid(T schema) throws InvalidConfigurationException {
        if (!schema.isValid()) {
            throw new InvalidConfigurationException("Failed to transform config file due to:["
                    + schema.getValidationIssues().stream().sorted().collect(Collectors.joining("], [")) + "]");
        }
        return schema;
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

    protected static void writeFlowXmlFile(ConfigSchema configSchema, OutputStream outputStream) throws TransformerException, ConfigTransformerException, ConfigurationChangeException, IOException {
        final StreamResult streamResult = new StreamResult(outputStream);

        // configure the transformer and convert the DOM
        final TransformerFactory transformFactory = TransformerFactory.newInstance();
        final Transformer transformer = transformFactory.newTransformer();
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");

        // transform the document to byte stream
        transformer.transform(createFlowXml(configSchema), streamResult);
    }

    protected static void writeFlowXmlFile(ConfigSchema configSchema, String path) throws IOException, TransformerException, ConfigurationChangeException, ConfigTransformerException {
        try (OutputStream fileOut = Files.newOutputStream(Paths.get(path, "flow.xml.gz"))) {
            try (OutputStream outStream = new GZIPOutputStream(fileOut)) {
                writeFlowXmlFile(configSchema, outStream);
            }
        }
    }

    protected static void writeNiFiProperties(ConfigSchema configSchema, OutputStream outputStream) throws IOException, ConfigurationChangeException {
        try {
            CorePropertiesSchema coreProperties = configSchema.getCoreProperties();
            FlowFileRepositorySchema flowfileRepoSchema = configSchema.getFlowfileRepositoryProperties();
            SwapSchema swapProperties = flowfileRepoSchema.getSwapProperties();
            ContentRepositorySchema contentRepoProperties = configSchema.getContentRepositoryProperties();
            ComponentStatusRepositorySchema componentStatusRepoProperties = configSchema.getComponentStatusRepositoryProperties();
            SecurityPropertiesSchema securityProperties = configSchema.getSecurityProperties();
            SensitivePropsSchema sensitiveProperties = securityProperties.getSensitiveProps();
            ProvenanceRepositorySchema provenanceRepositorySchema = configSchema.getProvenanceRepositorySchema();

            OrderedProperties orderedProperties = new OrderedProperties();
            orderedProperties.setProperty(NIFI_VERSION_KEY, NIFI_VERSION, "# Core Properties #" + System.lineSeparator());
            orderedProperties.setProperty("nifi.flow.configuration.file", "./conf/flow.xml.gz");
            orderedProperties.setProperty("nifi.flow.configuration.archive.enabled", "false");
            orderedProperties.setProperty("nifi.flow.configuration.archive.dir", "./conf/archive/");
            orderedProperties.setProperty("nifi.flowcontroller.autoResumeState", "true");
            orderedProperties.setProperty("nifi.flowcontroller.graceful.shutdown.period", coreProperties.getFlowControllerGracefulShutdownPeriod());
            orderedProperties.setProperty("nifi.flowservice.writedelay.interval", coreProperties.getFlowServiceWriteDelayInterval());
            orderedProperties.setProperty("nifi.administrative.yield.duration", coreProperties.getAdministrativeYieldDuration());
            orderedProperties.setProperty("nifi.variable.registry.properties", coreProperties.getVariableRegistryProperties());

            orderedProperties.setProperty("nifi.bored.yield.duration", coreProperties.getBoredYieldDuration(),
                    "# If a component has no work to do (is \"bored\"), how long should we wait before checking again for work?");

            orderedProperties.setProperty("nifi.authority.provider.configuration.file", "./conf/authority-providers.xml", "");
            orderedProperties.setProperty("nifi.login.identity.provider.configuration.file", "./conf/login-identity-providers.xml");
            orderedProperties.setProperty("nifi.templates.directory", "./conf/templates");
            orderedProperties.setProperty("nifi.ui.banner.text", "");
            orderedProperties.setProperty("nifi.ui.autorefresh.interval", "30 sec");
            orderedProperties.setProperty("nifi.nar.library.directory", "./lib");
            orderedProperties.setProperty("nifi.nar.working.directory", "./work/nar/");
            orderedProperties.setProperty("nifi.documentation.working.directory", "./work/docs/components");

            orderedProperties.setProperty("nifi.state.management.configuration.file", "./conf/state-management.xml", System.lineSeparator() +
                    "####################" +
                    "# State Management #" +
                    "####################");

            orderedProperties.setProperty("nifi.state.management.provider.local", "local-provider", "# The ID of the local state provider");

            orderedProperties.setProperty("nifi.database.directory", "./database_repository", System.lineSeparator() + "# H2 Settings");
            orderedProperties.setProperty("nifi.h2.url.append", ";LOCK_TIMEOUT=25000;WRITE_DELAY=0;AUTO_SERVER=FALSE");
            orderedProperties.setProperty("nifi.flowfile.repository.implementation", "org.apache.nifi.controller.repository.WriteAheadFlowFileRepository",
                    System.lineSeparator() + "# FlowFile Repository");
            orderedProperties.setProperty("nifi.flowfile.repository.directory", "./flowfile_repository");
            orderedProperties.setProperty("nifi.flowfile.repository.partitions", String.valueOf(flowfileRepoSchema.getPartitions()));
            orderedProperties.setProperty("nifi.flowfile.repository.checkpoint.interval", flowfileRepoSchema.getCheckpointInterval());
            orderedProperties.setProperty("nifi.flowfile.repository.always.sync", Boolean.toString(flowfileRepoSchema.getAlwaysSync()));

            orderedProperties.setProperty("nifi.swap.manager.implementation", "org.apache.nifi.controller.FileSystemSwapManager", "");
            orderedProperties.setProperty("nifi.queue.swap.threshold", String.valueOf(swapProperties.getThreshold()));
            orderedProperties.setProperty("nifi.swap.in.period", swapProperties.getInPeriod());
            orderedProperties.setProperty("nifi.swap.in.threads", String.valueOf(swapProperties.getInThreads()));
            orderedProperties.setProperty("nifi.swap.out.period", swapProperties.getOutPeriod());
            orderedProperties.setProperty("nifi.swap.out.threads", String.valueOf(swapProperties.getOutThreads()));

            orderedProperties.setProperty("nifi.content.repository.implementation", contentRepoProperties.getContentRepository(), System.lineSeparator() + "# Content Repository");
            orderedProperties.setProperty("nifi.content.claim.max.appendable.size", contentRepoProperties.getContentClaimMaxAppendableSize());
            orderedProperties.setProperty("nifi.content.claim.max.flow.files", String.valueOf(contentRepoProperties.getContentClaimMaxFlowFiles()));
            orderedProperties.setProperty("nifi.content.repository.archive.max.retention.period", contentRepoProperties.getContentRepoArchiveMaxRetentionPeriod());
            orderedProperties.setProperty("nifi.content.repository.archive.max.usage.percentage", contentRepoProperties.getContentRepoArchiveMaxUsagePercentage());
            orderedProperties.setProperty("nifi.content.repository.archive.enabled", Boolean.toString(contentRepoProperties.getContentRepoArchiveEnabled()));
            orderedProperties.setProperty("nifi.content.repository.directory.default", "./content_repository");
            orderedProperties.setProperty("nifi.content.repository.always.sync", Boolean.toString(contentRepoProperties.getAlwaysSync()));

            orderedProperties.setProperty("nifi.provenance.repository.implementation", provenanceRepositorySchema.getProvenanceRepository(),
                    System.lineSeparator() + "# Provenance Repository Properties");

            orderedProperties.setProperty("nifi.provenance.repository.rollover.time", provenanceRepositorySchema.getProvenanceRepoRolloverTimeKey());

            orderedProperties.setProperty("nifi.provenance.repository.index.shard.size", provenanceRepositorySchema.getProvenanceRepoIndexShardSize());
            orderedProperties.setProperty("nifi.provenance.repository.max.storage.size", provenanceRepositorySchema.getProvenanceRepoMaxStorageSize());
            orderedProperties.setProperty("nifi.provenance.repository.max.storage.time", provenanceRepositorySchema.getProvenanceRepoMaxStorageTime());

            orderedProperties.setProperty("nifi.provenance.repository.buffer.size", String.valueOf(provenanceRepositorySchema.getProvenanceRepoBufferSize()),
                    System.lineSeparator() + "# Volatile Provenance Respository Properties");

            orderedProperties.setProperty("nifi.components.status.repository.implementation", "org.apache.nifi.controller.status.history.VolatileComponentStatusRepository",
                    System.lineSeparator() + "# Component Status Repository");
            orderedProperties.setProperty("nifi.components.status.repository.buffer.size", String.valueOf(componentStatusRepoProperties.getBufferSize()));
            orderedProperties.setProperty("nifi.components.status.snapshot.frequency", componentStatusRepoProperties.getSnapshotFrequency());

            orderedProperties.setProperty("nifi.web.war.directory", "./lib", System.lineSeparator() + "# web properties #");
            orderedProperties.setProperty("nifi.web.http.host", "");
            orderedProperties.setProperty("nifi.web.http.port", "8081");
            orderedProperties.setProperty("nifi.web.https.host", "");
            orderedProperties.setProperty("nifi.web.https.port", "");
            orderedProperties.setProperty("nifi.web.jetty.working.directory", "./work/jetty");
            orderedProperties.setProperty("nifi.web.jetty.threads", "200");

            final String sensitivePropertiesKey = sensitiveProperties.getKey();
            final String notnullSensitivePropertiesKey = sensitivePropertiesKey != null ? sensitivePropertiesKey : "";
            orderedProperties.setProperty("nifi.sensitive.props.key", notnullSensitivePropertiesKey, System.lineSeparator() + "# security properties #");
            orderedProperties.setProperty("nifi.sensitive.props.algorithm", sensitiveProperties.getAlgorithm());
            orderedProperties.setProperty("nifi.sensitive.props.provider", sensitiveProperties.getProvider());

            orderedProperties.setProperty("nifi.security.keystore", securityProperties.getKeystore(), "");
            orderedProperties.setProperty("nifi.security.keystoreType", securityProperties.getKeystoreType());
            orderedProperties.setProperty("nifi.security.keystorePasswd", securityProperties.getKeystorePassword());
            orderedProperties.setProperty("nifi.security.keyPasswd", securityProperties.getKeyPassword());
            orderedProperties.setProperty("nifi.security.truststore", securityProperties.getTruststore());
            orderedProperties.setProperty("nifi.security.truststoreType", securityProperties.getTruststoreType());
            orderedProperties.setProperty("nifi.security.truststorePasswd", securityProperties.getTruststorePassword());
            orderedProperties.setProperty("nifi.security.needClientAuth", "");
            orderedProperties.setProperty("nifi.security.user.credential.cache.duration", "24 hours");
            orderedProperties.setProperty("nifi.security.user.authority.provider", "file-provider");
            orderedProperties.setProperty("nifi.security.user.login.identity.provider", "");
            orderedProperties.setProperty("nifi.security.support.new.account.requests", "");

            orderedProperties.setProperty("nifi.security.anonymous.authorities", "", "# Valid Authorities include: ROLE_MONITOR,ROLE_DFM,ROLE_ADMIN,ROLE_PROVENANCE,ROLE_NIFI");
            orderedProperties.setProperty("nifi.security.ocsp.responder.url", "");
            orderedProperties.setProperty("nifi.security.ocsp.responder.certificate", "");

            orderedProperties.setProperty("nifi.cluster.is.node", "false", System.lineSeparator() + System.lineSeparator() + "# cluster node properties (only configure for cluster nodes) #");
            orderedProperties.setProperty("nifi.cluster.is.manager", "false", System.lineSeparator() + "# cluster manager properties (only configure for cluster manager) #");

            for (Map.Entry<String, String> entry : configSchema.getNifiPropertiesOverrides().entrySet()) {
                orderedProperties.setProperty(entry.getKey(), entry.getValue());
            }

            orderedProperties.store(outputStream, PROPERTIES_FILE_APACHE_2_0_LICENSE);
        } catch (NullPointerException e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while creating the nifi.properties", e);
        } finally {
            outputStream.close();
        }
    }

    protected static DOMSource createFlowXml(ConfigSchema configSchema) throws IOException, ConfigurationChangeException, ConfigTransformerException {
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

            FlowControllerSchema flowControllerProperties = configSchema.getFlowControllerProperties();

            final Element element = doc.createElement("rootGroup");
            rootNode.appendChild(element);

            ProcessGroupSchema processGroupSchema = configSchema.getProcessGroupSchema();
            processGroupSchema.setId(ROOT_GROUP);
            processGroupSchema.setName(flowControllerProperties.getName());
            processGroupSchema.setComment(flowControllerProperties.getComment());

            addProcessGroup(doc, element, processGroupSchema, new ParentGroupIdResolver(processGroupSchema));

            SecurityPropertiesSchema securityProperties = configSchema.getSecurityProperties();
            boolean useSSL = securityProperties.useSSL();
            if (useSSL) {
                Element controllerServicesNode = doc.getElementById("controllerServices");
                if (controllerServicesNode == null) {
                    controllerServicesNode = doc.createElement("controllerServices");
                }

                rootNode.appendChild(controllerServicesNode);
                addSSLControllerService(controllerServicesNode, securityProperties);
            }

            List<ReportingSchema> reportingTasks = configSchema.getReportingTasksSchema();
            ProvenanceReportingSchema provenanceProperties = configSchema.getProvenanceReportingProperties();
            if (provenanceProperties != null) {
                provenanceProperties.setSSL(useSSL);
                ReportingSchema provenance = provenanceProperties.convert();
                provenance.setId("Provenance-Reporting");
                provenance.setName("Site-To-Site-Provenance-Reporting");
                reportingTasks.add(provenance);
            }
            if (reportingTasks != null) {
                final Element reportingTasksNode = doc.createElement("reportingTasks");
                rootNode.appendChild(reportingTasksNode);
                for (ReportingSchema task : reportingTasks) {
                    addReportingTask(reportingTasksNode, task);
                }
            }

            return new DOMSource(doc);
        } catch (final ParserConfigurationException | DOMException | TransformerFactoryConfigurationError | IllegalArgumentException e) {
            throw new ConfigTransformerException(e);
        } catch (Exception e) {
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
            attributes.put("Keystore Password", securityProperties.getKeystorePassword());
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

    protected static void addControllerService(final Element element, ControllerServiceSchema controllerServiceSchema) throws ConfigurationChangeException {
        try {
            final Element serviceElement = element.getOwnerDocument().createElement("controllerService");
            addTextElement(serviceElement, "id", controllerServiceSchema.getId());
            addTextElement(serviceElement, "name", controllerServiceSchema.getName());
            addTextElement(serviceElement, "comment", "");
            addTextElement(serviceElement, "class", controllerServiceSchema.getServiceClass());

            addTextElement(serviceElement, "enabled", "true");

            Map<String, Object> attributes = controllerServiceSchema.getProperties();

            addConfiguration(serviceElement, attributes);

            String annotationData = controllerServiceSchema.getAnnotationData();
            if (annotationData != null && !annotationData.isEmpty()) {
                addTextElement(element, "annotationData", annotationData);
            }

            element.appendChild(serviceElement);
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to create an SSL Controller Service", e);
        }
    }

    protected static void addProcessGroup(Document doc, Element element, ProcessGroupSchema processGroupSchema, ParentGroupIdResolver parentGroupIdResolver) throws ConfigurationChangeException {
        try {
            String processGroupId = processGroupSchema.getId();
            addTextElement(element, "id", processGroupId);
            addTextElement(element, "name", processGroupSchema.getName());
            addPosition(element);
            addTextElement(element, "comment", processGroupSchema.getComment());

            for (ProcessorSchema processorConfig : processGroupSchema.getProcessors()) {
                addProcessor(element, processorConfig);
            }

            for (PortSchema portSchema : processGroupSchema.getInputPortSchemas()) {
                addPort(doc, element, portSchema, "inputPort");
            }

            for (PortSchema portSchema : processGroupSchema.getOutputPortSchemas()) {
                addPort(doc, element, portSchema, "outputPort");
            }

            for (FunnelSchema funnelSchema : processGroupSchema.getFunnels()) {
                addFunnel(element, funnelSchema);
            }

            for (ProcessGroupSchema child : processGroupSchema.getProcessGroupSchemas()) {
                Element processGroups = doc.createElement("processGroup");
                element.appendChild(processGroups);
                addProcessGroup(doc, processGroups, child, parentGroupIdResolver);
            }

            for (RemoteProcessGroupSchema remoteProcessGroupSchema : processGroupSchema.getRemoteProcessGroups()) {
                addRemoteProcessGroup(element, remoteProcessGroupSchema);
            }

            for (ConnectionSchema connectionConfig : processGroupSchema.getConnections()) {
                addConnection(element, connectionConfig, parentGroupIdResolver);
            }

            for (ControllerServiceSchema controllerServiceSchema : processGroupSchema.getControllerServices()) {
                addControllerService(element, controllerServiceSchema);
            }
        } catch (ConfigurationChangeException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to creating the root Process Group", e);
        }
    }

    protected static void addPort(Document doc, Element parentElement, PortSchema portSchema, String tag) {
        Element element = doc.createElement(tag);
        parentElement.appendChild(element);

        addTextElement(element, "id", portSchema.getId());
        addTextElement(element, "name", portSchema.getName());

        addPosition(element);
        addTextElement(element, "comments", null);

        addTextElement(element, "scheduledState", "RUNNING");
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
            if (annotationData != null && !annotationData.isEmpty()) {
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


    protected static void addFunnel(final Element parentElement, FunnelSchema funnelSchema) {
        Document document = parentElement.getOwnerDocument();
        Element element = document.createElement("funnel");
        parentElement.appendChild(element);

        addTextElement(element, "id", funnelSchema.getId());

        addPosition(element);
    }

    protected static void addReportingTask(final Element parentElement, ReportingSchema reportingSchema) throws ConfigurationChangeException {
        try {
            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement("reportingTask");
            parentElement.appendChild(element);

            addTextElement(element, "id", reportingSchema.getId());
            addTextElement(element, "name", reportingSchema.getName());
            addTextElement(element, "comment", reportingSchema.getComment());
            addTextElement(element, "class", reportingSchema.getReportingClass());
            addTextElement(element, "schedulingPeriod", reportingSchema.getSchedulingPeriod());
            addTextElement(element, "scheduledState", "RUNNING");
            addTextElement(element, "schedulingStrategy", reportingSchema.getSchedulingStrategy());

            addConfiguration(element, reportingSchema.getProperties());

            parentElement.appendChild(element);
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

    protected static void addRemoteProcessGroup(final Element parentElement, RemoteProcessGroupSchema remoteProcessGroupProperties) throws ConfigurationChangeException {
        try {
            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement("remoteProcessGroup");
            parentElement.appendChild(element);
            addTextElement(element, "id", remoteProcessGroupProperties.getId());
            addTextElement(element, "name", remoteProcessGroupProperties.getName());
            addPosition(element);
            addTextElement(element, "comment", remoteProcessGroupProperties.getComment());
            // In the case we have multiple urls, select the first
            addTextElement(element, "url", Arrays.asList(remoteProcessGroupProperties.getUrls().split(",")).get(0));
            addTextElement(element, "urls", remoteProcessGroupProperties.getUrls());
            addTextElement(element, "timeout", remoteProcessGroupProperties.getTimeout());
            addTextElement(element, "yieldPeriod", remoteProcessGroupProperties.getYieldPeriod());
            addTextElement(element, "transmitting", "true");
            addTextElement(element, "transportProtocol", remoteProcessGroupProperties.getTransportProtocol());
            addTextElement(element, "proxyHost", remoteProcessGroupProperties.getProxyHost());
            if (remoteProcessGroupProperties.getProxyPort() != null) {
                addTextElement(element, "proxyPort", Integer.toString(remoteProcessGroupProperties.getProxyPort()));
            }
            addTextElement(element, "proxyUser", remoteProcessGroupProperties.getProxyUser());
            if (!StringUtils.isEmpty(remoteProcessGroupProperties.getProxyPassword())) {
                addTextElement(element, "proxyPassword", remoteProcessGroupProperties.getProxyPassword());
            }

            List<RemotePortSchema> remoteInputPorts = remoteProcessGroupProperties.getInputPorts();
            for (RemotePortSchema remoteInputPortSchema : remoteInputPorts) {
                addRemoteGroupPort(element, remoteInputPortSchema, "inputPort");
            }

            List<RemotePortSchema> remoteOutputPorts = remoteProcessGroupProperties.getOutputPorts();
            for (RemotePortSchema remoteOutputPortSchema : remoteOutputPorts) {
                addRemoteGroupPort(element, remoteOutputPortSchema, "outputPort");
            }
            addTextElement(element, "networkInterface", remoteProcessGroupProperties.getLocalNetworkInterface());

            parentElement.appendChild(element);
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the Remote Process Group", e);
        }
    }

    protected static void addRemoteGroupPort(final Element parentElement, RemotePortSchema inputPort, String tagName) throws ConfigurationChangeException {
        try {
            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement(tagName);
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

    protected static void addConnection(final Element parentElement, ConnectionSchema connectionProperties, ParentGroupIdResolver parentGroupIdResolver) throws ConfigurationChangeException {
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

            addConnectionSourceOrDestination(element, "source", connectionProperties.getSourceId(), parentGroupIdResolver);
            addConnectionSourceOrDestination(element, "destination", connectionProperties.getDestinationId(), parentGroupIdResolver);

            List<String> sourceRelationshipNames = connectionProperties.getSourceRelationshipNames();
            if (sourceRelationshipNames.isEmpty()) {
                addTextElement(element, "relationship", null);
            } else {
                for (String relationshipName : sourceRelationshipNames) {
                    addTextElement(element, "relationship", relationshipName);
                }
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

    protected static void addConnectionSourceOrDestination(Element element, String sourceOrDestination, String id, ParentGroupIdResolver parentGroupIdResolver) {
        String idTag = sourceOrDestination + "Id";
        String groupIdTag = sourceOrDestination + "GroupId";
        String typeTag = sourceOrDestination + "Type";

        String parentId;
        String type;

        if ((parentId = parentGroupIdResolver.getRemoteInputPortParentId(id)) != null) {
            type = "REMOTE_INPUT_PORT";
        } else if ((parentId = parentGroupIdResolver.getRemoteOutputPortParentId(id)) != null) {
            type = "REMOTE_OUTPUT_PORT";
        } else if ((parentId = parentGroupIdResolver.getInputPortParentId(id)) != null) {
            type = "INPUT_PORT";
        } else if ((parentId = parentGroupIdResolver.getOutputPortParentId(id)) != null) {
            type = "OUTPUT_PORT";
        } else if ((parentId = parentGroupIdResolver.getFunnelParentId(id)) != null) {
            type = "FUNNEL";
        } else {
            parentId = parentGroupIdResolver.getProcessorParentId(id);
            type = "PROCESSOR";
        }

        addTextElement(element, idTag, id);
        if (parentId != null) {
            addTextElement(element, groupIdTag, parentId);
        }
        addTextElement(element, typeTag, type);
    }

    protected static void addPosition(final Element parentElement) {
        final Element element = parentElement.getOwnerDocument().createElement("position");
        element.setAttribute("x", String.valueOf("0"));
        element.setAttribute("y", String.valueOf("0"));
        parentElement.appendChild(element);
    }

    protected static void addTextElementIfNotNullOrEmpty(final Element element, final String name, final String value) {
        StringUtil.doIfNotNullOrEmpty(value, s -> addTextElement(element, name, value));
    }

    protected static void addTextElement(final Element element, final String name, final String value) {
        final Document doc = element.getOwnerDocument();
        final Element toAdd = doc.createElement(name);
        toAdd.setTextContent(value);
        element.appendChild(toAdd);
    }

    public static final String PROPERTIES_FILE_APACHE_2_0_LICENSE =
            " Licensed to the Apache Software Foundation (ASF) under one or more\n" +
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
            "# limitations under the License.\n" +
            "\n";

}
