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


import org.apache.nifi.controller.FlowSerializationException;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.yaml.snakeyaml.Yaml;

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
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public final class ConfigTransformer {
    // Underlying version NIFI POC will be using
    public static final String NIFI_VERSION = "0.6.0";

    public static final String NAME_KEY = "name";
    public static final String COMMENT_KEY = "comment";
    public static final String ALWAYS_SYNC_KEY = "always sync";
    public static final String YIELD_PERIOD_KEY = "yield period";
    public static final String MAX_CONCURRENT_TASKS_KEY = "max concurrent tasks";
    public static final String ID_KEY = "id";

    public static final String FLOW_CONTROLLER_PROPS_KEY = "Flow Controller";

    public static final String CORE_PROPS_KEY = "Core Properties";
    public static final String FLOW_CONTROLLER_SHUTDOWN_PERIOD_KEY = "flow controller graceful shutdown period";
    public static final String FLOW_SERVICE_WRITE_DELAY_INTERVAL_KEY = "flow service write delay interval";
    public static final String ADMINISTRATIVE_YIELD_DURATION_KEY = "administrative yield duration";
    public static final String BORED_YIELD_DURATION_KEY = "bored yield duration";

    public static final String FLOWFILE_REPO_KEY = "FlowFile Repository";
    public static final String PARTITIONS_KEY = "partitions";
    public static final String CHECKPOINT_INTERVAL_KEY = "checkpoint interval";
    public static final String THRESHOLD_KEY = "queue swap threshold";
    public static final String SWAP_PROPS_KEY = "Swap";
    public static final String IN_PERIOD_KEY = "in period";
    public static final String IN_THREADS_KEY = "in threads";
    public static final String OUT_PERIOD_KEY = "out period";
    public static final String OUT_THREADS_KEY = "out threads";


    public static final String CONTENT_REPO_KEY = "Content Repository";
    public static final String CONTENT_CLAIM_MAX_APPENDABLE_SIZE_KEY = "content claim max appendable size";
    public static final String CONTENT_CLAIM_MAX_FLOW_FILES_KEY = "content claim max flow files";

    public static final String COMPONENT_STATUS_REPO_KEY = "Component Status Repository";
    public static final String BUFFER_SIZE_KEY = "buffer size";
    public static final String SNAPSHOT_FREQUENCY_KEY = "snapshot frequency";

    public static final String SECURITY_PROPS_KEY = "Security Properties";
    public static final String KEYSTORE_KEY = "keystore";
    public static final String KEYSTORE_TYPE_KEY = "keystore type";
    public static final String KEYSTORE_PASSWORD_KEY = "keystore password";
    public static final String KEY_PASSWORD_KEY = "key password";
    public static final String TRUSTSTORE_KEY = "truststore";
    public static final String TRUSTSTORE_TYPE_KEY = "truststore type";
    public static final String TRUSTSTORE_PASSWORD_KEY = "truststore password";
    public static final String SENSITIVE_PROPS_KEY = "Sensitive Props";
    public static final String SENSITIVE_PROPS_KEY__KEY = "key";
    public static final String SENSITIVE_PROPS_ALGORITHM_KEY = "algorithm";
    public static final String SENSITIVE_PROPS_PROVIDER_KEY = "provider";

    public static final String PROCESSOR_CONFIG_KEY = "Processor Configuration";
    public static final String CLASS_KEY = "class";
    public static final String SCHEDULING_PERIOD_KEY = "scheduling period";
    public static final String PENALIZATION_PERIOD_KEY = "penalization period";
    public static final String SCHEDULING_STRATEGY_KEY = "scheduling strategy";
    public static final String RUN_DURATION_NANOS_KEY = "run duration nanos";
    public static final String AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY = "auto-terminated relationships list";

    public static final String PROCESSOR_PROPS_KEY = "Properties";

    public static final String CONNECTION_PROPS_KEY = "Connection Properties";
    public static final String MAX_WORK_QUEUE_SIZE_KEY = "max work queue size";
    public static final String MAX_WORK_QUEUE_DATA_SIZE_KEY = "max work queue data size";
    public static final String FLOWFILE_EXPIRATION__KEY = "flowfile expiration";
    public static final String QUEUE_PRIORITIZER_CLASS_KEY = "queue prioritizer class";

    public static final String REMOTE_PROCESSING_GROUP_KEY = "Remote Processing Group";
    public static final String URL_KEY = "url";
    public static final String TIMEOUT_KEY = "timeout";

    public static final String INPUT_PORT_KEY = "Input Port";
    public static final String USE_COMPRESSION_KEY = "use compression";

    public static final String PROVENANCE_REPORTING_KEY = "Provenance Reporting";
    public static final String DESTINATION_URL_KEY = "destination url";
    public static final String PORT_NAME_KEY = "port name";
    public static final String ORIGINATING_URL_KEY = "originating url";
    public static final String BATCH_SIZE_KEY = "batch size";

    public static final String SSL_PROTOCOL_KEY = "ssl protocol";

    // Final util classes should have private constructor
    private ConfigTransformer() {}

    public static void transformConfigFile(String sourceFile, String destPath) throws Exception {
        File ymlConfigFile = new File(sourceFile);
        InputStream ios = new FileInputStream(ymlConfigFile);

        transformConfigFile(ios, destPath);
    }

    public static void transformConfigFile(InputStream sourceStream, String destPath) throws Exception {
        try {
            Yaml yaml = new Yaml();

            // Parse the YAML file
            final Object loadedObject = yaml.load(sourceStream);

            // Verify the parsed object is a Map structure
            if (loadedObject instanceof Map) {
                final Map<String, Object> result = (Map<String, Object>) loadedObject;

                // Create nifi.properties and flow.xml.gz in memory
                ByteArrayOutputStream nifiPropertiesOutputStream = new ByteArrayOutputStream();
                writeNiFiProperties(result, nifiPropertiesOutputStream);

                DOMSource flowXml = createFlowXml(result);

                // Write nifi.properties and flow.xml.gz
                writeNiFiPropertiesFile(nifiPropertiesOutputStream, destPath);

                writeFlowXmlFile(flowXml, destPath);
            } else {
                throw new IllegalArgumentException("Provided YAML configuration is not a Map.");
            }
        } finally {
            if (sourceStream != null) {
                sourceStream.close();
            }
        }
    }

    private static void writeNiFiPropertiesFile(ByteArrayOutputStream nifiPropertiesOutputStream, String destPath) throws IOException {
        try {
            final Path nifiPropertiesPath = Paths.get(destPath, "nifi.properties");
            FileOutputStream nifiProperties = new FileOutputStream(new File(nifiPropertiesPath.toString()));
            nifiProperties.write(nifiPropertiesOutputStream.getUnderlyingBuffer());
        } finally {
            if (nifiPropertiesOutputStream != null){
                nifiPropertiesOutputStream.flush();
                nifiPropertiesOutputStream.close();
            }
        }
    }

    private static void writeFlowXmlFile(DOMSource domSource, String path) throws IOException, TransformerException {

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

    private static void writeNiFiProperties(Map<String, Object> topLevelYaml, OutputStream outputStream) throws FileNotFoundException, UnsupportedEncodingException, ConfigurationChangeException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(outputStream, true);

            Map<String, Object> coreProperties = (Map<String, Object>) topLevelYaml.get(CORE_PROPS_KEY);
            Map<String, Object> flowfileRepo = (Map<String, Object>) topLevelYaml.get(FLOWFILE_REPO_KEY);
            Map<String, Object> swapProperties = (Map<String, Object>) flowfileRepo.get(SWAP_PROPS_KEY);
            Map<String, Object> contentRepo = (Map<String, Object>) topLevelYaml.get(CONTENT_REPO_KEY);
            Map<String, Object> componentStatusRepo = (Map<String, Object>) topLevelYaml.get(COMPONENT_STATUS_REPO_KEY);
            Map<String, Object> securityProperties = (Map<String, Object>) topLevelYaml.get(SECURITY_PROPS_KEY);
            Map<String, Object> sensitiveProperties = (Map<String, Object>) securityProperties.get(SENSITIVE_PROPS_KEY);

            writer.print(PROPERTIES_FILE_APACHE_2_0_LICENSE);
            writer.println("# Core Properties #");
            writer.println();
            writer.println("nifi.version="+NIFI_VERSION);
            writer.println("nifi.flow.configuration.file=./conf/flow.xml.gz");
            writer.println("nifi.flow.configuration.archive.dir=./conf/archive/");
            writer.println("nifi.flowcontroller.autoResumeState=true");
            writer.println("nifi.flowcontroller.graceful.shutdown.period=" + getValueString(coreProperties, FLOW_CONTROLLER_SHUTDOWN_PERIOD_KEY));
            writer.println("nifi.flowservice.writedelay.interval=" + getValueString(coreProperties, FLOW_SERVICE_WRITE_DELAY_INTERVAL_KEY));
            writer.println("nifi.administrative.yield.duration=" + getValueString(coreProperties, ADMINISTRATIVE_YIELD_DURATION_KEY));
            writer.println("# If a component has no work to do (is \"bored\"), how long should we wait before checking again for work?");
            writer.println("nifi.bored.yield.duration=" + getValueString(coreProperties, BORED_YIELD_DURATION_KEY));
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
            writer.println("nifi.flowfile.repository.partitions=" + getValueString(flowfileRepo, PARTITIONS_KEY));
            writer.println("nifi.flowfile.repository.checkpoint.interval=" + getValueString(flowfileRepo,CHECKPOINT_INTERVAL_KEY));
            writer.println("nifi.flowfile.repository.always.sync=" + getValueString(flowfileRepo,ALWAYS_SYNC_KEY));
            writer.println();
            writer.println("nifi.swap.manager.implementation=org.apache.nifi.controller.FileSystemSwapManager");
            writer.println("nifi.queue.swap.threshold=" + getValueString(swapProperties, THRESHOLD_KEY));
            writer.println("nifi.swap.in.period=" + getValueString(swapProperties, IN_PERIOD_KEY));
            writer.println("nifi.swap.in.threads=" + getValueString(swapProperties, IN_THREADS_KEY));
            writer.println("nifi.swap.out.period=" + getValueString(swapProperties, OUT_PERIOD_KEY));
            writer.println("nifi.swap.out.threads=" + getValueString(swapProperties, OUT_THREADS_KEY));
            writer.println();
            writer.println("# Content Repository");
            writer.println("nifi.content.repository.implementation=org.apache.nifi.controller.repository.FileSystemRepository");
            writer.println("nifi.content.claim.max.appendable.size=" + getValueString(contentRepo, CONTENT_CLAIM_MAX_APPENDABLE_SIZE_KEY));
            writer.println("nifi.content.claim.max.flow.files=" + getValueString(contentRepo, CONTENT_CLAIM_MAX_FLOW_FILES_KEY));
            writer.println("nifi.content.repository.archive.max.retention.period=");
            writer.println("nifi.content.repository.archive.max.usage.percentage=");
            writer.println("nifi.content.repository.archive.enabled=false");
            writer.println("nifi.content.repository.directory.default=./content_repository");
            writer.println("nifi.content.repository.always.sync=" + getValueString(contentRepo, ALWAYS_SYNC_KEY));
            writer.println();
            writer.println("# Provenance Repository Properties");
            writer.println("nifi.provenance.repository.implementation=org.apache.nifi.provenance.VolatileProvenanceRepository");
            writer.println();
            writer.println("# Volatile Provenance Respository Properties");
            writer.println("nifi.provenance.repository.buffer.size=100000");
            writer.println();
            writer.println("# Component Status Repository");
            writer.println("nifi.components.status.repository.implementation=org.apache.nifi.controller.status.history.VolatileComponentStatusRepository");
            writer.println("nifi.components.status.repository.buffer.size=" + getValueString(componentStatusRepo, BUFFER_SIZE_KEY));
            writer.println("nifi.components.status.snapshot.frequency=" + getValueString(componentStatusRepo, SNAPSHOT_FREQUENCY_KEY));
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
            writer.println("nifi.sensitive.props.key=" + getValueString(sensitiveProperties, SENSITIVE_PROPS_KEY__KEY));
            writer.println("nifi.sensitive.props.algorithm=" + getValueString(sensitiveProperties, SENSITIVE_PROPS_ALGORITHM_KEY));
            writer.println("nifi.sensitive.props.provider=" + getValueString(sensitiveProperties, SENSITIVE_PROPS_PROVIDER_KEY));
            writer.println();
            writer.println("nifi.security.keystore=" + getValueString(securityProperties, KEYSTORE_KEY));
            writer.println("nifi.security.keystoreType=" + getValueString(securityProperties, KEYSTORE_TYPE_KEY));
            writer.println("nifi.security.keystorePasswd=" + getValueString(securityProperties, KEYSTORE_PASSWORD_KEY));
            writer.println("nifi.security.keyPasswd=" + getValueString(securityProperties, KEY_PASSWORD_KEY));
            writer.println("nifi.security.truststore=" + getValueString(securityProperties, TRUSTSTORE_KEY));
            writer.println("nifi.security.truststoreType=" + getValueString(securityProperties, TRUSTSTORE_TYPE_KEY));
            writer.println("nifi.security.truststorePasswd=" + getValueString(securityProperties, TRUSTSTORE_PASSWORD_KEY));
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
            if (writer != null){
                writer.flush();
                writer.close();
            }
        }
    }
    private static DOMSource createFlowXml(Map<String, Object> topLevelYaml) throws IOException, ConfigurationChangeException {
        try {
            // create a new, empty document
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);

            final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            final Document doc = docBuilder.newDocument();

            // populate document with controller state
            final Element rootNode = doc.createElement("flowController");
            doc.appendChild(rootNode);
            Map<String, Object> processorConfig = (Map<String, Object>) topLevelYaml.get(PROCESSOR_CONFIG_KEY);
            addTextElement(rootNode, "maxTimerDrivenThreadCount", getValueString(processorConfig, MAX_CONCURRENT_TASKS_KEY, "1"));
            addTextElement(rootNode, "maxEventDrivenThreadCount", getValueString(processorConfig, MAX_CONCURRENT_TASKS_KEY, "1"));
            addProcessGroup(rootNode, topLevelYaml, "rootGroup");

            Map<String, Object> securityProps = (Map<String, Object>) topLevelYaml.get(SECURITY_PROPS_KEY);
            if (securityProps != null) {
                String sslAlgorithm = (String) securityProps.get(SSL_PROTOCOL_KEY);
                if (sslAlgorithm != null && !(sslAlgorithm.isEmpty())) {
                    final Element controllerServicesNode = doc.createElement("controllerServices");
                    rootNode.appendChild(controllerServicesNode);
                    addSSLControllerService(controllerServicesNode, securityProps);
                }
            }

            Map<String, Object> provenanceProperties = (Map<String, Object>) topLevelYaml.get(PROVENANCE_REPORTING_KEY);
            if (provenanceProperties.get(SCHEDULING_STRATEGY_KEY) != null) {
                final Element reportingTasksNode = doc.createElement("reportingTasks");
                rootNode.appendChild(reportingTasksNode);
                addProvenanceReportingTask(reportingTasksNode, topLevelYaml);
            }

            return new DOMSource(doc);
        } catch (final ParserConfigurationException | DOMException | TransformerFactoryConfigurationError | IllegalArgumentException e) {
            throw new FlowSerializationException(e);
        } catch (Exception e){
            throw new ConfigurationChangeException("Failed to parse the config YAML while writing the top level of the flow xml", e);
        }
    }

    private static <K> String getValueString(Map<K,Object> map, K key){
        Object value = map.get(key);
        return value == null ? "" : value.toString();
    }

    private static <K> String getValueString(Map<K,Object> map, K key, String theDefault){
        Object value = null;
        if (map != null){
            value = map.get(key);
        }
        return value == null ? theDefault : value.toString();
    }

    private static void addSSLControllerService(final Element element, Map<String, Object> securityProperties) throws ConfigurationChangeException {
        try {
            final Element serviceElement = element.getOwnerDocument().createElement("controllerService");
            addTextElement(serviceElement, "id", "SSL-Context-Service");
            addTextElement(serviceElement, "name", "SSL-Context-Service");
            addTextElement(serviceElement, "comment", "");
            addTextElement(serviceElement, "class", "org.apache.nifi.ssl.StandardSSLContextService");

            addTextElement(serviceElement, "enabled", "true");

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("Keystore Filename", securityProperties.get(KEYSTORE_KEY));
            attributes.put("Keystore Type", securityProperties.get(KEYSTORE_TYPE_KEY));
            attributes.put("Keystore Password", securityProperties.get(KEYSTORE_PASSWORD_KEY));
            attributes.put("Truststore Filename", securityProperties.get(TRUSTSTORE_KEY));
            attributes.put("Truststore Type", securityProperties.get(TRUSTSTORE_TYPE_KEY));
            attributes.put("Truststore Password", securityProperties.get(TRUSTSTORE_PASSWORD_KEY));
            attributes.put("SSL Protocol", securityProperties.get(SSL_PROTOCOL_KEY));

            addConfiguration(serviceElement, attributes);

            element.appendChild(serviceElement);
        } catch (Exception e){
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to create an SSL Controller Service", e);
        }
    }

    private static void addProcessGroup(final Element parentElement, Map<String, Object> topLevelYaml, final String elementName) throws ConfigurationChangeException {
        try {
            Map<String, Object> flowControllerProperties = (Map<String, Object>) topLevelYaml.get(FLOW_CONTROLLER_PROPS_KEY);

            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement(elementName);
            parentElement.appendChild(element);
            addTextElement(element, "id", "Root-Group");
            addTextElement(element, "name", getValueString(flowControllerProperties, NAME_KEY));
            addPosition(element);
            addTextElement(element, "comment", getValueString(flowControllerProperties, COMMENT_KEY));

            Map<String, Object> processorConfig = (Map<String, Object>) topLevelYaml.get(PROCESSOR_CONFIG_KEY);
            addProcessor(element, processorConfig);

            Map<String, Object> remoteProcessingGroup = (Map<String, Object>) topLevelYaml.get(REMOTE_PROCESSING_GROUP_KEY);
            addRemoteProcessGroup(element, remoteProcessingGroup);

            addConnection(element, topLevelYaml);
        } catch (ConfigurationChangeException e){
            throw e;
        } catch (Exception e){
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to creating the root Process Group", e);
        }
    }

    private static void addProcessor(final Element parentElement, Map<String, Object> processorConfig) throws ConfigurationChangeException {

        try {
            if (processorConfig.get(CLASS_KEY) == null) {
                // Only add a processor if it has a class
                return;
            }

            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement("processor");
            parentElement.appendChild(element);
            addTextElement(element, "id", "Processor");
            addTextElement(element, "name", getValueString(processorConfig, NAME_KEY));

            addPosition(element);
            addStyle(element);

            addTextElement(element, "comment", getValueString(processorConfig, COMMENT_KEY));
            addTextElement(element, "class", getValueString(processorConfig, CLASS_KEY));
            addTextElement(element, "maxConcurrentTasks", getValueString(processorConfig, MAX_CONCURRENT_TASKS_KEY));
            addTextElement(element, "schedulingPeriod", getValueString(processorConfig, SCHEDULING_PERIOD_KEY));
            addTextElement(element, "penalizationPeriod", getValueString(processorConfig, PENALIZATION_PERIOD_KEY));
            addTextElement(element, "yieldPeriod", getValueString(processorConfig, YIELD_PERIOD_KEY));
            addTextElement(element, "bulletinLevel", "WARN");
            addTextElement(element, "lossTolerant", "false");
            addTextElement(element, "scheduledState", "RUNNING");
            addTextElement(element, "schedulingStrategy", getValueString(processorConfig, SCHEDULING_STRATEGY_KEY));
            addTextElement(element, "runDurationNanos", getValueString(processorConfig, RUN_DURATION_NANOS_KEY));

            addConfiguration(element, (Map<String, Object>) processorConfig.get(PROCESSOR_PROPS_KEY));

            Collection<String> autoTerminatedRelationships = (Collection<String>) processorConfig.get(AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY);
            if (autoTerminatedRelationships != null) {
                for (String rel : autoTerminatedRelationships) {
                    addTextElement(element, "autoTerminatedRelationship", rel);
                }
            }
        } catch (Exception e){
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the Processor", e);
        }
    }

    private static void addProvenanceReportingTask(final Element element, Map<String, Object> topLevelYaml) throws ConfigurationChangeException {
        try {
            Map<String, Object> provenanceProperties = (Map<String, Object>) topLevelYaml.get(PROVENANCE_REPORTING_KEY);
            final Element taskElement = element.getOwnerDocument().createElement("reportingTask");
            addTextElement(taskElement, "id", "Provenance-Reporting");
            addTextElement(taskElement, "name", "Site-To-Site-Provenance-Reporting");
            addTextElement(taskElement, "comment", getValueString(provenanceProperties, COMMENT_KEY));
            addTextElement(taskElement, "class", "org.apache.nifi.minifi.provenance.reporting.ProvenanceReportingTask");
            addTextElement(taskElement, "schedulingPeriod", getValueString(provenanceProperties, SCHEDULING_PERIOD_KEY));
            addTextElement(taskElement, "scheduledState", "RUNNING");
            addTextElement(taskElement, "schedulingStrategy", getValueString(provenanceProperties, SCHEDULING_STRATEGY_KEY));

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("Destination URL", provenanceProperties.get(DESTINATION_URL_KEY));
            attributes.put("Input Port Name", provenanceProperties.get(PORT_NAME_KEY));
            attributes.put("MiNiFi URL", provenanceProperties.get(ORIGINATING_URL_KEY));
            attributes.put("Compress Events", provenanceProperties.get(USE_COMPRESSION_KEY));
            attributes.put("Batch Size", provenanceProperties.get(BATCH_SIZE_KEY));

            Map<String, Object> securityProps = (Map<String, Object>) topLevelYaml.get(SECURITY_PROPS_KEY);
            String sslAlgorithm = (String) securityProps.get(SSL_PROTOCOL_KEY);
            if (sslAlgorithm != null && !(sslAlgorithm.isEmpty())) {
                attributes.put("SSL Context Service", "SSL-Context-Service");
            }

            addConfiguration(taskElement, attributes);

            element.appendChild(taskElement);
        } catch (Exception e){
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the Provenance Reporting Task", e);
        }
    }

    private static void addConfiguration(final Element element, Map<String, Object> elementConfig) {
        final Document doc = element.getOwnerDocument();
        if (elementConfig == null){
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

    private static void addStyle(final Element parentElement) {
        final Element element = parentElement.getOwnerDocument().createElement("styles");
        parentElement.appendChild(element);
    }

    private static void addRemoteProcessGroup(final Element parentElement, Map<String, Object> remoteProcessingGroup) throws ConfigurationChangeException {
        try {
            if (remoteProcessingGroup.get(URL_KEY) == null) {
                // Only add an an RPG if it has a URL
                return;
            }

            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement("remoteProcessGroup");
            parentElement.appendChild(element);
            addTextElement(element, "id", "Remote-Process-Group");
            addTextElement(element, "name", getValueString(remoteProcessingGroup, NAME_KEY));
            addPosition(element);
            addTextElement(element, "comment", getValueString(remoteProcessingGroup, COMMENT_KEY));
            addTextElement(element, "url", getValueString(remoteProcessingGroup, URL_KEY));
            addTextElement(element, "timeout", getValueString(remoteProcessingGroup, TIMEOUT_KEY));
            addTextElement(element, "yieldPeriod", getValueString(remoteProcessingGroup, YIELD_PERIOD_KEY));
            addTextElement(element, "transmitting", "true");

            Map<String, Object> inputPort = (Map<String, Object>) remoteProcessingGroup.get(INPUT_PORT_KEY);
            addRemoteGroupPort(element, inputPort, "inputPort");

            parentElement.appendChild(element);
        } catch (Exception e){
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the Remote Process Group", e);
        }
    }

    private static void addRemoteGroupPort(final Element parentElement, Map<String, Object> inputPort, final String elementName) throws ConfigurationChangeException {

        try {
            if (inputPort.get(ID_KEY) == null) {
                // Only add an input port if it has an ID
                return;
            }

            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement(elementName);
            parentElement.appendChild(element);
            addTextElement(element, "id", getValueString(inputPort, ID_KEY));
            addTextElement(element, "name", getValueString(inputPort, NAME_KEY));
            addPosition(element);
            addTextElement(element, "comments", getValueString(inputPort, COMMENT_KEY));
            addTextElement(element, "scheduledState", "RUNNING");
            addTextElement(element, "maxConcurrentTasks", getValueString(inputPort, MAX_CONCURRENT_TASKS_KEY));
            addTextElement(element, "useCompression", getValueString(inputPort, USE_COMPRESSION_KEY));

            parentElement.appendChild(element);
        } catch (Exception e){
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the input port of the Remote Process Group", e);
        }
    }

    private static void addConnection(final Element parentElement, Map<String, Object> topLevelYaml) throws ConfigurationChangeException {
        try {
            Map<String, Object> connectionProperties = (Map<String, Object>) topLevelYaml.get(CONNECTION_PROPS_KEY);
            Map<String, Object> remoteProcessingGroup = (Map<String, Object>) topLevelYaml.get(REMOTE_PROCESSING_GROUP_KEY);
            Map<String, Object> inputPort = (Map<String, Object>) remoteProcessingGroup.get(INPUT_PORT_KEY);
            Map<String, Object> processorConfig = (Map<String, Object>) topLevelYaml.get(PROCESSOR_CONFIG_KEY);

            if (inputPort.get(ID_KEY) == null || processorConfig.get(CLASS_KEY) == null) {
                // Only add the connection if the input port and processor config are created
                return;
            }

            final Document doc = parentElement.getOwnerDocument();
            final Element element = doc.createElement("connection");
            parentElement.appendChild(element);
            addTextElement(element, "id", "Connection");
            addTextElement(element, "name", getValueString(connectionProperties, NAME_KEY));

            final Element bendPointsElement = doc.createElement("bendPoints");
            element.appendChild(bendPointsElement);

            addTextElement(element, "labelIndex", "1");
            addTextElement(element, "zIndex", "0");

            addTextElement(element, "sourceId", "Processor");
            addTextElement(element, "sourceGroupId", "Root-Group");
            addTextElement(element, "sourceType", "PROCESSOR");

            addTextElement(element, "destinationId", getValueString(inputPort, ID_KEY));
            addTextElement(element, "destinationGroupId", "Remote-Process-Group");
            addTextElement(element, "destinationType", "REMOTE_INPUT_PORT");

            addTextElement(element, "relationship", "success");

            addTextElement(element, "maxWorkQueueSize", getValueString(connectionProperties, MAX_WORK_QUEUE_SIZE_KEY));
            addTextElement(element, "maxWorkQueueDataSize", getValueString(connectionProperties, MAX_WORK_QUEUE_DATA_SIZE_KEY));

            addTextElement(element, "flowFileExpiration", getValueString(connectionProperties, FLOWFILE_EXPIRATION__KEY));
            addTextElement(element, "queuePrioritizerClass", getValueString(connectionProperties, QUEUE_PRIORITIZER_CLASS_KEY));

            parentElement.appendChild(element);
        } catch (Exception e){
            throw new ConfigurationChangeException("Failed to parse the config YAML while trying to add the connection from the Processor to the input port of the Remote Process Group", e);
        }
    }

    private static void addPosition(final Element parentElement) {
        final Element element = parentElement.getOwnerDocument().createElement("position");
        element.setAttribute("x", String.valueOf("0"));
        element.setAttribute("y", String.valueOf("0"));
        parentElement.appendChild(element);
    }

    private static void addTextElement(final Element element, final String name, final String value) {
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
