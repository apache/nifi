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
package org.apache.nifi.controller;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.controller.serialization.StandardFlowSerializer;
import org.apache.nifi.controller.service.ControllerServiceLoader;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.fingerprint.FingerprintException;
import org.apache.nifi.fingerprint.FingerprintFactory;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.util.LoggingXmlParserErrorHandler;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 */
public class StandardFlowSynchronizer implements FlowSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(StandardFlowSynchronizer.class);
    public static final URL FLOW_XSD_RESOURCE = StandardFlowSynchronizer.class.getResource("/FlowConfiguration.xsd");
    private final StringEncryptor encryptor;
    private final boolean autoResumeState;
    private final NiFiProperties nifiProperties;
    private final ExtensionManager extensionManager;

    public StandardFlowSynchronizer(final StringEncryptor encryptor, final NiFiProperties nifiProperties, final ExtensionManager extensionManager) {
        this.encryptor = encryptor;
        this.autoResumeState = nifiProperties.getAutoResumeState();
        this.nifiProperties = nifiProperties;
        this.extensionManager = extensionManager;
    }

    public static boolean isEmpty(final DataFlow dataFlow) {
        if (dataFlow == null || dataFlow.getFlow() == null || dataFlow.getFlow().length == 0) {
            return true;
        }

        final Document document = parseFlowBytes(dataFlow.getFlow());
        final Element rootElement = document.getDocumentElement();

        final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
        final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootGroupElement);
        final ProcessGroupDTO rootGroupDto = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, null, encodingVersion);

        final NodeList reportingTasks = rootElement.getElementsByTagName("reportingTask");
        final ReportingTaskDTO reportingTaskDTO = reportingTasks.getLength() == 0 ? null : FlowFromDOMFactory.getReportingTask((Element)reportingTasks.item(0),null);

        final NodeList controllerServices = rootElement.getElementsByTagName("controllerService");
        final ControllerServiceDTO controllerServiceDTO = controllerServices.getLength() == 0 ? null : FlowFromDOMFactory.getControllerService((Element)controllerServices.item(0),null);

        return isEmpty(rootGroupDto) && isEmpty(reportingTaskDTO) && isEmpty(controllerServiceDTO);
    }

    @Override
    public void sync(final FlowController controller, final DataFlow proposedFlow, final StringEncryptor encryptor)
            throws FlowSerializationException, UninheritableFlowException, FlowSynchronizationException, MissingBundleException {

        final FlowManager flowManager = controller.getFlowManager();
        final ProcessGroup root = flowManager.getRootGroup();

        // handle corner cases involving no proposed flow
        if (proposedFlow == null) {
            if (root.isEmpty()) {
                return;  // no sync to perform
            } else {
                throw new UninheritableFlowException("Proposed configuration is empty, but the controller contains a data flow.");
            }
        }

        // determine if the controller already had flow sync'd to it
        final boolean flowAlreadySynchronized = controller.isFlowSynchronized();
        logger.debug("Synching FlowController with proposed flow: Controller is Already Synchronized = {}", flowAlreadySynchronized);

        // serialize controller state to bytes
        final byte[] existingFlow;
        final boolean existingFlowEmpty;
        try {
            if (flowAlreadySynchronized) {
                existingFlow = toBytes(controller);
                existingFlowEmpty = root.isEmpty()
                    && flowManager.getAllReportingTasks().isEmpty()
                    && flowManager.getAllControllerServices().isEmpty()
                    && controller.getFlowRegistryClient().getRegistryIdentifiers().isEmpty();
            } else {
                existingFlow = readFlowFromDisk();
                if (existingFlow == null || existingFlow.length == 0) {
                    existingFlowEmpty = true;
                } else {
                    final Document document = parseFlowBytes(existingFlow);
                    final Element rootElement = document.getDocumentElement();
                    final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootElement);

                    logger.trace("Setting controller thread counts");
                    final Integer maxThreadCount = getInteger(rootElement, "maxThreadCount");
                    if (maxThreadCount == null) {
                        controller.setMaxTimerDrivenThreadCount(getInt(rootElement, "maxTimerDrivenThreadCount"));
                        controller.setMaxEventDrivenThreadCount(getInt(rootElement, "maxEventDrivenThreadCount"));
                    } else {
                        controller.setMaxTimerDrivenThreadCount(maxThreadCount * 2 / 3);
                        controller.setMaxEventDrivenThreadCount(maxThreadCount / 3);
                    }

                    final Element reportingTasksElement = DomUtils.getChild(rootElement, "reportingTasks");
                    final List<Element> taskElements;
                    if (reportingTasksElement == null) {
                        taskElements = Collections.emptyList();
                    } else {
                        taskElements = DomUtils.getChildElementsByTagName(reportingTasksElement, "reportingTask");
                    }

                    final Element controllerServicesElement = DomUtils.getChild(rootElement, "controllerServices");
                    final List<Element> unrootedControllerServiceElements;
                    if (controllerServicesElement == null) {
                        unrootedControllerServiceElements = Collections.emptyList();
                    } else {
                        unrootedControllerServiceElements = DomUtils.getChildElementsByTagName(controllerServicesElement, "controllerService");
                    }

                    final boolean registriesPresent;
                    final Element registriesElement = DomUtils.getChild(rootElement, "registries");
                    if (registriesElement == null) {
                        registriesPresent = false;
                    } else {
                        final List<Element> flowRegistryElems = DomUtils.getChildElementsByTagName(registriesElement, "flowRegistry");
                        registriesPresent = !flowRegistryElems.isEmpty();
                    }

                    logger.trace("Parsing process group from DOM");
                    final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
                    final ProcessGroupDTO rootGroupDto = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, encryptor, encodingVersion);
                    existingFlowEmpty = taskElements.isEmpty()
                        && unrootedControllerServiceElements.isEmpty()
                        && isEmpty(rootGroupDto)
                        && !registriesPresent;
                    logger.debug("Existing Flow Empty = {}", existingFlowEmpty);
                }
            }
        } catch (final IOException e) {
            throw new FlowSerializationException(e);
        }

        logger.trace("Exporting snippets from controller");
        final byte[] existingSnippets = controller.getSnippetManager().export();

        logger.trace("Getting Authorizer fingerprint from controller");

        final byte[] existingAuthFingerprint;
        final ManagedAuthorizer managedAuthorizer;
        final Authorizer authorizer = controller.getAuthorizer();

        if (AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            managedAuthorizer = (ManagedAuthorizer) authorizer;
            existingAuthFingerprint = managedAuthorizer.getFingerprint().getBytes(StandardCharsets.UTF_8);
        } else {
            existingAuthFingerprint = null;
            managedAuthorizer = null;
        }

        final Set<String> missingComponents = new HashSet<>();
        flowManager.getAllControllerServices().stream().filter(ComponentNode::isExtensionMissing).forEach(cs -> missingComponents.add(cs.getIdentifier()));
        flowManager.getAllReportingTasks().stream().filter(ComponentNode::isExtensionMissing).forEach(r -> missingComponents.add(r.getIdentifier()));
        root.findAllProcessors().stream().filter(AbstractComponentNode::isExtensionMissing).forEach(p -> missingComponents.add(p.getIdentifier()));

        final DataFlow existingDataFlow = new StandardDataFlow(existingFlow, existingSnippets, existingAuthFingerprint, missingComponents);

        Document configuration = null;

        // check that the proposed flow is inheritable by the controller
        try {
            if (existingFlowEmpty) {
                configuration = parseFlowBytes(proposedFlow.getFlow());
                if (configuration != null) {
                    logger.trace("Checking bundle compatibility");
                    checkBundleCompatibility(configuration);
                }
            } else {
                logger.trace("Checking flow inheritability");
                final String problemInheritingFlow = checkFlowInheritability(existingDataFlow, proposedFlow, controller);
                if (problemInheritingFlow != null) {
                    throw new UninheritableFlowException("Proposed configuration is not inheritable by the flow controller because of flow differences: " + problemInheritingFlow);
                }
            }
        } catch (final FingerprintException fe) {
            throw new FlowSerializationException("Failed to generate flow fingerprints", fe);
        }

        logger.trace("Checking missing component inheritability");

        final String problemInheritingMissingComponents = checkMissingComponentsInheritability(existingDataFlow, proposedFlow);
        if (problemInheritingMissingComponents != null) {
            throw new UninheritableFlowException("Proposed Flow is not inheritable by the flow controller because of differences in missing components: " + problemInheritingMissingComponents);
        }

        logger.trace("Checking authorizer inheritability");

        final AuthorizerInheritability authInheritability = checkAuthorizerInheritability(authorizer, existingDataFlow, proposedFlow);
        if (!authInheritability.isInheritable() && authInheritability.getReason() != null) {
            throw new UninheritableFlowException("Proposed Authorizer is not inheritable by the flow controller because of Authorizer differences: " + authInheritability.getReason());
        }

        // create document by parsing proposed flow bytes
        logger.trace("Parsing proposed flow bytes as DOM document");
        if (configuration == null) {
            configuration = parseFlowBytes(proposedFlow.getFlow());
        }

        // attempt to sync controller with proposed flow
        try {
            if (configuration != null) {
                synchronized (configuration) {
                    // get the root element
                    final Element rootElement = (Element) configuration.getElementsByTagName("flowController").item(0);
                    final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootElement);

                    // set controller config
                    logger.trace("Updating flow config");
                    final Integer maxThreadCount = getInteger(rootElement, "maxThreadCount");
                    if (maxThreadCount == null) {
                        controller.setMaxTimerDrivenThreadCount(getInt(rootElement, "maxTimerDrivenThreadCount"));
                        controller.setMaxEventDrivenThreadCount(getInt(rootElement, "maxEventDrivenThreadCount"));
                    } else {
                        controller.setMaxTimerDrivenThreadCount(maxThreadCount * 2 / 3);
                        controller.setMaxEventDrivenThreadCount(maxThreadCount / 3);
                    }

                    // get the root group XML element
                    final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);

                    if (!flowAlreadySynchronized || existingFlowEmpty) {
                        final Element registriesElement = DomUtils.getChild(rootElement, "registries");
                        if (registriesElement != null) {
                            final List<Element> flowRegistryElems = DomUtils.getChildElementsByTagName(registriesElement, "flowRegistry");
                            for (final Element flowRegistryElement : flowRegistryElems) {
                                final String registryId = getString(flowRegistryElement, "id");
                                final String registryName = getString(flowRegistryElement, "name");
                                final String registryUrl = getString(flowRegistryElement, "url");
                                final String description = getString(flowRegistryElement, "description");

                                final FlowRegistryClient client = controller.getFlowRegistryClient();
                                client.addFlowRegistry(registryId, registryName, registryUrl, description);
                            }
                        }
                    }

                    // if this controller isn't initialized or its empty, add the root group, otherwise update
                    final ProcessGroup rootGroup;
                    if (!flowAlreadySynchronized || existingFlowEmpty) {
                        logger.trace("Adding root process group");
                        rootGroup = addProcessGroup(controller, /* parent group */ null, rootGroupElement, encryptor, encodingVersion);
                    } else {
                        logger.trace("Updating root process group");
                        rootGroup = updateProcessGroup(controller, /* parent group */ null, rootGroupElement, encryptor, encodingVersion);
                    }

                    rootGroup.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);

                    // If there are any Templates that do not exist in the Proposed Flow that do exist in the 'existing flow', we need
                    // to ensure that we also add those to the appropriate Process Groups, so that we don't lose them.
                    final Document existingFlowConfiguration = parseFlowBytes(existingFlow);
                    if (existingFlowConfiguration != null) {
                        final Element existingRootElement = (Element) existingFlowConfiguration.getElementsByTagName("flowController").item(0);
                        if (existingRootElement != null) {
                            final Element existingRootGroupElement = (Element) existingRootElement.getElementsByTagName("rootGroup").item(0);
                            if (existingRootElement != null) {
                                final FlowEncodingVersion existingEncodingVersion = FlowEncodingVersion.parse(existingFlowConfiguration.getDocumentElement());
                                addLocalTemplates(existingRootGroupElement, rootGroup, existingEncodingVersion);
                            }
                        }
                    }

                    // get all the reporting task elements
                    final Element reportingTasksElement = DomUtils.getChild(rootElement, "reportingTasks");
                    final List<Element> reportingTaskElements = new ArrayList<>();
                    if (reportingTasksElement != null) {
                        reportingTaskElements.addAll(DomUtils.getChildElementsByTagName(reportingTasksElement, "reportingTask"));
                    }

                    // get/create all the reporting task nodes and DTOs, but don't apply their scheduled state yet
                    final Map<ReportingTaskNode, ReportingTaskDTO> reportingTaskNodesToDTOs = new HashMap<>();
                    for (final Element taskElement : reportingTaskElements) {
                        final ReportingTaskDTO dto = FlowFromDOMFactory.getReportingTask(taskElement, encryptor);
                        final ReportingTaskNode reportingTask = getOrCreateReportingTask(controller, dto, flowAlreadySynchronized, existingFlowEmpty);
                        reportingTaskNodesToDTOs.put(reportingTask, dto);
                    }

                    final Element controllerServicesElement = DomUtils.getChild(rootElement, "controllerServices");
                    if (controllerServicesElement != null) {
                        final List<Element> serviceElements = DomUtils.getChildElementsByTagName(controllerServicesElement, "controllerService");

                        if (!flowAlreadySynchronized || existingFlowEmpty) {
                            // If the encoding version is null, we are loading a flow from NiFi 0.x, where Controller
                            // Services could not be scoped by Process Group. As a result, we want to move the Process Groups
                            // to the root Group. Otherwise, we want to use a null group, which indicates a Controller-level
                            // Controller Service.
                            final ProcessGroup group = (encodingVersion == null) ? rootGroup : null;
                            final Map<ControllerServiceNode, Element> controllerServices = ControllerServiceLoader.loadControllerServices(serviceElements, controller, group, encryptor);

                            // If we are moving controller services to the root group we also need to see if any reporting tasks
                            // reference them, and if so we need to clone the CS and update the reporting task reference
                            if (group != null) {
                                // find all the controller service ids referenced by reporting tasks
                                final Set<String> controllerServicesInReportingTasks = reportingTaskNodesToDTOs.keySet().stream()
                                        .flatMap(r -> r.getProperties().entrySet().stream())
                                        .filter(e -> e.getKey().getControllerServiceDefinition() != null)
                                        .map(Map.Entry::getValue)
                                        .collect(Collectors.toSet());

                                // find the controller service nodes for each id referenced by a reporting task
                                final Set<ControllerServiceNode> controllerServicesToClone = controllerServices.keySet().stream()
                                        .filter(cs -> controllerServicesInReportingTasks.contains(cs.getIdentifier()))
                                        .collect(Collectors.toSet());

                                // clone the controller services and map the original id to the clone
                                final Map<String, ControllerServiceNode> controllerServiceMapping = new HashMap<>();
                                for (ControllerServiceNode controllerService : controllerServicesToClone) {
                                    final ControllerServiceNode clone = ControllerServiceLoader.cloneControllerService(controller, controllerService);
                                    flowManager.addRootControllerService(clone);
                                    controllerServiceMapping.put(controllerService.getIdentifier(), clone);
                                }

                                // update the reporting tasks to reference the cloned controller services
                                updateReportingTaskControllerServices(reportingTaskNodesToDTOs.keySet(), controllerServiceMapping);

                                // enable all the cloned controller services
                                ControllerServiceLoader.enableControllerServices(controllerServiceMapping.values(), controller, autoResumeState);
                            }

                            // enable all the original controller services
                            ControllerServiceLoader.enableControllerServices(controllerServices, controller, encryptor, autoResumeState);
                        }
                    }

                    scaleRootGroup(rootGroup, encodingVersion);

                    // now that controller services are loaded and enabled we can apply the scheduled state to each reporting task
                    for (Map.Entry<ReportingTaskNode, ReportingTaskDTO> entry : reportingTaskNodesToDTOs.entrySet()) {
                        applyReportingTaskScheduleState(controller, entry.getValue(), entry.getKey(), flowAlreadySynchronized, existingFlowEmpty);
                    }
                }
            }

            // clear the snippets that are currently in memory
            logger.trace("Clearing existing snippets");
            final SnippetManager snippetManager = controller.getSnippetManager();
            snippetManager.clear();

            // if proposed flow has any snippets, load them
            logger.trace("Loading proposed snippets");
            final byte[] proposedSnippets = proposedFlow.getSnippets();
            if (proposedSnippets != null && proposedSnippets.length > 0) {
                for (final StandardSnippet snippet : SnippetManager.parseBytes(proposedSnippets)) {
                    snippetManager.addSnippet(snippet);
                }
            }

            // if auths are inheritable and we have a policy based authorizer, then inherit
            if (authInheritability.isInheritable() && managedAuthorizer != null) {
                logger.trace("Inheriting authorizations");
                final String proposedAuthFingerprint = new String(proposedFlow.getAuthorizerFingerprint(), StandardCharsets.UTF_8);
                managedAuthorizer.inheritFingerprint(proposedAuthFingerprint);
            }

            logger.debug("Finished syncing flows");
        } catch (final Exception ex) {
            throw new FlowSynchronizationException(ex);
        }
    }

    private void checkBundleCompatibility(final Document configuration) {
        final NodeList bundleNodes = configuration.getElementsByTagName("bundle");
        for (int i = 0; i < bundleNodes.getLength(); i++) {
            final Node bundleNode = bundleNodes.item(i);
            if (bundleNode instanceof Element) {
                final Element bundleElement = (Element) bundleNode;

                final Node componentNode = bundleElement.getParentNode();
                if (componentNode instanceof Element) {
                    final Element componentElement = (Element) componentNode;
                    if (!withinTemplate(componentElement)) {
                        final String componentType = DomUtils.getChildText(componentElement, "class");
                        try {
                            BundleUtils.getBundle(extensionManager, componentType, FlowFromDOMFactory.getBundle(bundleElement));
                        } catch (IllegalStateException e) {
                            throw new MissingBundleException(e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    private boolean withinTemplate(final Element element) {
        if ("template".equals(element.getTagName())) {
            return true;
        } else {
            final Node parentNode = element.getParentNode();
            if (parentNode instanceof Element) {
                return withinTemplate((Element) parentNode);
            } else {
                return false;
            }
        }
    }

    private void updateReportingTaskControllerServices(final Set<ReportingTaskNode> reportingTasks, final Map<String, ControllerServiceNode> controllerServiceMapping) {
        for (ReportingTaskNode reportingTask : reportingTasks) {
            if (reportingTask.getProperties() != null) {
                reportingTask.pauseValidationTrigger();
                try {
                    final Set<Map.Entry<PropertyDescriptor, String>> propertyDescriptors = reportingTask.getProperties().entrySet().stream()
                            .filter(e -> e.getKey().getControllerServiceDefinition() != null)
                            .filter(e -> controllerServiceMapping.containsKey(e.getValue()))
                            .collect(Collectors.toSet());

                    final Map<String,String> controllerServiceProps = new HashMap<>();

                    for (Map.Entry<PropertyDescriptor, String> propEntry : propertyDescriptors) {
                        final PropertyDescriptor propertyDescriptor = propEntry.getKey();
                        final ControllerServiceNode clone = controllerServiceMapping.get(propEntry.getValue());
                        controllerServiceProps.put(propertyDescriptor.getName(), clone.getIdentifier());
                    }

                    reportingTask.setProperties(controllerServiceProps);
                } finally {
                    reportingTask.resumeValidationTrigger();
                }
            }
        }
    }

    private void addLocalTemplates(final Element processGroupElement, final ProcessGroup processGroup, final FlowEncodingVersion encodingVersion) {
        // Replace the templates with those from the proposed flow
        final List<Element> templateNodeList = getChildrenByTagName(processGroupElement, "template");
        if (templateNodeList != null) {
            for (final Element templateElement : templateNodeList) {
                final TemplateDTO templateDto = TemplateUtils.parseDto(templateElement);
                final Template template = new Template(templateDto);

                // If the Process Group does not have the template, add it.
                if (processGroup.getTemplate(template.getIdentifier()) == null) {
                    processGroup.addTemplate(template);
                }
            }
        }

        final List<Element> childGroupElements = getChildrenByTagName(processGroupElement, "processGroup");
        for (final Element childGroupElement : childGroupElements) {
            final String childGroupId = getString(childGroupElement, "id");
            final ProcessGroup childGroup = processGroup.getProcessGroup(childGroupId);
            addLocalTemplates(childGroupElement, childGroup, encodingVersion);
        }
    }

    void scaleRootGroup(final ProcessGroup rootGroup, final FlowEncodingVersion encodingVersion) {
        if (encodingVersion == null || encodingVersion.getMajorVersion() < 1) {
            // Calculate new Positions if the encoding version of the flow is older than 1.0.
            PositionScaler.scale(rootGroup, 1.5, 1.34);
        }
    }

    private static boolean isEmpty(final ProcessGroupDTO dto) {
        if (dto == null) {
            return true;
        }

        final FlowSnippetDTO contents = dto.getContents();
        if (contents == null) {
            return true;
        }

        return CollectionUtils.isEmpty(contents.getProcessors())
                && CollectionUtils.isEmpty(contents.getConnections())
                && CollectionUtils.isEmpty(contents.getFunnels())
                && CollectionUtils.isEmpty(contents.getLabels())
                && CollectionUtils.isEmpty(contents.getOutputPorts())
                && CollectionUtils.isEmpty(contents.getProcessGroups())
                && CollectionUtils.isEmpty(contents.getProcessors())
                && CollectionUtils.isEmpty(contents.getRemoteProcessGroups());
    }

    private static boolean isEmpty(final ReportingTaskDTO reportingTaskDTO){

       return reportingTaskDTO == null || StringUtils.isEmpty(reportingTaskDTO.getName()) ;

    }

    private static boolean isEmpty(final ControllerServiceDTO controllerServiceDTO){

        return controllerServiceDTO == null || StringUtils.isEmpty(controllerServiceDTO.getName());

    }

    private static Document parseFlowBytes(final byte[] flow) throws FlowSerializationException {
        // create document by parsing proposed flow bytes
        try {
            // create validating document builder
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            final Schema schema = schemaFactory.newSchema(FLOW_XSD_RESOURCE);
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            docFactory.setSchema(schema);

            final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            docBuilder.setErrorHandler(new LoggingXmlParserErrorHandler("Flow Configuration", logger));

            // parse flow
            return (flow == null || flow.length == 0) ? null : docBuilder.parse(new ByteArrayInputStream(flow));
        } catch (final SAXException | ParserConfigurationException | IOException ex) {
            throw new FlowSerializationException(ex);
        }
    }

    private byte[] readFlowFromDisk() throws IOException {
        final Path flowPath = nifiProperties.getFlowConfigurationFile().toPath();
        if (!Files.exists(flowPath) || Files.size(flowPath) == 0) {
            return new byte[0];
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final InputStream in = Files.newInputStream(flowPath, StandardOpenOption.READ);
                final InputStream gzipIn = new GZIPInputStream(in)) {
            FileUtils.copy(gzipIn, baos);
        }

        return baos.toByteArray();
    }


    private ReportingTaskNode getOrCreateReportingTask(final FlowController controller, final ReportingTaskDTO dto, final boolean controllerInitialized, final boolean existingFlowEmpty)
            throws ReportingTaskInstantiationException {
        // create a new reporting task node when the controller is not initialized or the flow is empty
        if (!controllerInitialized || existingFlowEmpty) {
            BundleCoordinate coordinate;
            try {
                coordinate = BundleUtils.getCompatibleBundle(extensionManager, dto.getType(), dto.getBundle());
            } catch (final IllegalStateException e) {
                final BundleDTO bundleDTO = dto.getBundle();
                if (bundleDTO == null) {
                    coordinate = BundleCoordinate.UNKNOWN_COORDINATE;
                } else {
                    coordinate = new BundleCoordinate(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
                }
            }

            final ReportingTaskNode reportingTask = controller.createReportingTask(dto.getType(), dto.getId(), coordinate, false);
            reportingTask.setName(dto.getName());
            reportingTask.setComments(dto.getComments());
            reportingTask.setSchedulingPeriod(dto.getSchedulingPeriod());
            reportingTask.setSchedulingStrategy(SchedulingStrategy.valueOf(dto.getSchedulingStrategy()));

            reportingTask.setAnnotationData(dto.getAnnotationData());
            reportingTask.setProperties(dto.getProperties());
            return reportingTask;
        } else {
            // otherwise return the existing reporting task node
            return controller.getReportingTaskNode(dto.getId());
        }
    }

    private void applyReportingTaskScheduleState(final FlowController controller, final ReportingTaskDTO dto, final ReportingTaskNode reportingTask,
            final boolean controllerInitialized, final boolean existingFlowEmpty) {
        if (!controllerInitialized || existingFlowEmpty) {
            applyNewReportingTaskScheduleState(controller, dto, reportingTask);
        } else {
            applyExistingReportingTaskScheduleState(controller, dto, reportingTask);
        }
    }

    private void applyNewReportingTaskScheduleState(final FlowController controller, final ReportingTaskDTO dto, final ReportingTaskNode reportingTask) {
        if (autoResumeState) {
            if (ScheduledState.RUNNING.name().equals(dto.getState())) {
                try {
                    controller.startReportingTask(reportingTask);
                } catch (final Exception e) {
                    logger.error("Failed to start {} due to {}", reportingTask, e);
                    if (logger.isDebugEnabled()) {
                        logger.error("", e);
                    }
                    controller.getBulletinRepository().addBulletin(BulletinFactory.createBulletin(
                            "Reporting Tasks", Severity.ERROR.name(), "Failed to start " + reportingTask + " due to " + e));
                }
            } else if (ScheduledState.DISABLED.name().equals(dto.getState())) {
                try {
                    controller.disableReportingTask(reportingTask);
                } catch (final Exception e) {
                    logger.error("Failed to mark {} as disabled due to {}", reportingTask, e);
                    if (logger.isDebugEnabled()) {
                        logger.error("", e);
                    }
                    controller.getBulletinRepository().addBulletin(BulletinFactory.createBulletin(
                            "Reporting Tasks", Severity.ERROR.name(), "Failed to mark " + reportingTask + " as disabled due to " + e));
                }
            }
        }
    }

    private void applyExistingReportingTaskScheduleState(final FlowController controller, final ReportingTaskDTO dto, final ReportingTaskNode taskNode) {
        if (!taskNode.getScheduledState().name().equals(dto.getState())) {
            try {
                switch (ScheduledState.valueOf(dto.getState())) {
                    case DISABLED:
                        if (taskNode.isRunning()) {
                            controller.stopReportingTask(taskNode);
                        }
                        controller.disableReportingTask(taskNode);
                        break;
                    case RUNNING:
                        if (taskNode.getScheduledState() == ScheduledState.DISABLED) {
                            controller.enableReportingTask(taskNode);
                        }
                        controller.startReportingTask(taskNode);
                        break;
                    case STOPPED:
                        if (taskNode.getScheduledState() == ScheduledState.DISABLED) {
                            controller.enableReportingTask(taskNode);
                        } else if (taskNode.getScheduledState() == ScheduledState.RUNNING) {
                            controller.stopReportingTask(taskNode);
                        }
                        break;
                }
            } catch (final IllegalStateException ise) {
                logger.error("Failed to change Scheduled State of {} from {} to {} due to {}", taskNode, taskNode.getScheduledState().name(), dto.getState(), ise.toString());
                logger.error("", ise);

                // create bulletin for the Processor Node
                controller.getBulletinRepository().addBulletin(BulletinFactory.createBulletin("Node Reconnection", Severity.ERROR.name(),
                        "Failed to change Scheduled State of " + taskNode + " from " + taskNode.getScheduledState().name() + " to " + dto.getState() + " due to " + ise.toString()));

                // create bulletin at Controller level.
                controller.getBulletinRepository().addBulletin(BulletinFactory.createBulletin("Node Reconnection", Severity.ERROR.name(),
                        "Failed to change Scheduled State of " + taskNode + " from " + taskNode.getScheduledState().name() + " to " + dto.getState() + " due to " + ise.toString()));
            }
        }
    }

    private ControllerServiceState getFinalTransitionState(final ControllerServiceState state) {
        switch (state) {
            case DISABLED:
            case DISABLING:
                return ControllerServiceState.DISABLED;
            case ENABLED:
            case ENABLING:
                return ControllerServiceState.ENABLED;
            default:
                throw new AssertionError();
        }
    }

    private ProcessGroup updateProcessGroup(final FlowController controller, final ProcessGroup parentGroup, final Element processGroupElement,
            final StringEncryptor encryptor, final FlowEncodingVersion encodingVersion) {

        // get the parent group ID
        final String parentId = (parentGroup == null) ? null : parentGroup.getIdentifier();

        // get the process group
        final ProcessGroupDTO processGroupDto = FlowFromDOMFactory.getProcessGroup(parentId, processGroupElement, encryptor, encodingVersion);
        final FlowManager flowManager = controller.getFlowManager();

        // update the process group
        if (parentId == null) {

            /*
             * Labels are not included in the "inherit flow" algorithm, so we cannot
             * blindly update them because they may not exist in the current flow.
             * Therefore, we first remove all labels, and then let the updating
             * process add labels defined in the new flow.
             */
            final ProcessGroup root = flowManager.getRootGroup();
            for (final Label label : root.findAllLabels()) {
                label.getProcessGroup().removeLabel(label);
            }
        }

        // update the process group
        final ProcessGroup group = flowManager.getGroup(processGroupDto.getId());
        if (group == null) {
            throw new IllegalStateException("No Group with ID " + processGroupDto.getId() + " exists");
        }

        updateProcessGroup(group, processGroupDto);

        // get the real process group and ID
        final ProcessGroup processGroup = flowManager.getGroup(processGroupDto.getId());

        // determine the scheduled state of all of the Controller Service
        final List<Element> controllerServiceNodeList = getChildrenByTagName(processGroupElement, "controllerService");
        final Set<ControllerServiceNode> toDisable = new HashSet<>();
        final Set<ControllerServiceNode> toEnable = new HashSet<>();

        for (final Element serviceElement : controllerServiceNodeList) {
            final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(serviceElement, encryptor);
            final ControllerServiceNode serviceNode = processGroup.getControllerService(dto.getId());

            // Check if the controller service is in the correct state. We consider it the correct state if
            // we are in a transitional state and heading in the right direction or already in the correct state.
            // E.g., it is the correct state if it should be 'DISABLED' and it is either DISABLED or DISABLING.
            final ControllerServiceState serviceState = getFinalTransitionState(serviceNode.getState());
            final ControllerServiceState clusterState = getFinalTransitionState(ControllerServiceState.valueOf(dto.getState()));

            if (serviceState != clusterState) {
                switch (clusterState) {
                    case DISABLED:
                        toDisable.add(serviceNode);
                        break;
                    case ENABLED:
                        toEnable.add(serviceNode);
                        break;
                }
            }
        }

        // Ensure that all services have been validated, so that we don't attempt to enable a service that is still in a 'validating' state
        toEnable.forEach(ControllerServiceNode::performValidation);

        final ControllerServiceProvider serviceProvider = controller.getControllerServiceProvider();
        serviceProvider.disableControllerServicesAsync(toDisable);
        serviceProvider.enableControllerServices(toEnable);

        // processors & ports cannot be updated - they must be the same. Except for the scheduled state.
        final List<Element> processorNodeList = getChildrenByTagName(processGroupElement, "processor");
        for (final Element processorElement : processorNodeList) {
            final ProcessorDTO dto = FlowFromDOMFactory.getProcessor(processorElement, encryptor);
            final ProcessorNode procNode = processGroup.getProcessor(dto.getId());

            final ScheduledState procState = getScheduledState(procNode, controller);
            updateNonFingerprintedProcessorSettings(procNode, dto);

            if (!procState.name().equals(dto.getState())) {
                try {
                    switch (ScheduledState.valueOf(dto.getState())) {
                        case DISABLED:
                            // switch processor do disabled. This means we have to stop it (if it's already stopped, this method does nothing),
                            // and then we have to disable it.
                            controller.stopProcessor(procNode.getProcessGroupIdentifier(), procNode.getIdentifier());
                            procNode.getProcessGroup().disableProcessor(procNode);
                            break;
                        case RUNNING:
                            // we want to run now. Make sure processor is not disabled and then start it.
                            procNode.performValidation();
                            procNode.getProcessGroup().enableProcessor(procNode);
                            controller.startProcessor(procNode.getProcessGroupIdentifier(), procNode.getIdentifier(), false);
                            break;
                        case STOPPED:
                            if (procState == ScheduledState.DISABLED) {
                                procNode.getProcessGroup().enableProcessor(procNode);
                            } else if (procState == ScheduledState.RUNNING) {
                                controller.stopProcessor(procNode.getProcessGroupIdentifier(), procNode.getIdentifier());
                            }
                            break;
                    }
                } catch (final IllegalStateException ise) {
                    logger.error("Failed to change Scheduled State of {} from {} to {} due to {}", procNode, procNode.getScheduledState().name(), dto.getState(), ise.toString());
                    logger.error("", ise);

                    // create bulletin for the Processor Node
                    controller.getBulletinRepository().addBulletin(BulletinFactory.createBulletin(procNode, "Node Reconnection", Severity.ERROR.name(),
                            "Failed to change Scheduled State of " + procNode + " from " + procNode.getScheduledState().name() + " to " + dto.getState() + " due to " + ise.toString()));

                    // create bulletin at Controller level.
                    controller.getBulletinRepository().addBulletin(BulletinFactory.createBulletin("Node Reconnection", Severity.ERROR.name(),
                            "Failed to change Scheduled State of " + procNode + " from " + procNode.getScheduledState().name() + " to " + dto.getState() + " due to " + ise.toString()));
                }
            }
        }

        final List<Element> inputPortList = getChildrenByTagName(processGroupElement, "inputPort");
        for (final Element portElement : inputPortList) {
            final PortDTO dto = FlowFromDOMFactory.getPort(portElement);
            final Port port = processGroup.getInputPort(dto.getId());

            final ScheduledState portState = getScheduledState(port, controller);

            if (!portState.name().equals(dto.getState())) {
                switch (ScheduledState.valueOf(dto.getState())) {
                    case DISABLED:
                        // switch processor do disabled. This means we have to stop it (if it's already stopped, this method does nothing),
                        // and then we have to disable it.
                        controller.stopConnectable(port);
                        port.getProcessGroup().disableInputPort(port);
                        break;
                    case RUNNING:
                        // we want to run now. Make sure processor is not disabled and then start it.
                        port.getProcessGroup().enableInputPort(port);
                        controller.startConnectable(port);
                        break;
                    case STOPPED:
                        if (portState == ScheduledState.DISABLED) {
                            port.getProcessGroup().enableInputPort(port);
                        } else if (portState == ScheduledState.RUNNING) {
                            controller.stopConnectable(port);
                        }
                        break;
                }
            }
        }

        final List<Element> outputPortList = getChildrenByTagName(processGroupElement, "outputPort");
        for (final Element portElement : outputPortList) {
            final PortDTO dto = FlowFromDOMFactory.getPort(portElement);
            final Port port = processGroup.getOutputPort(dto.getId());

            final ScheduledState portState = getScheduledState(port, controller);

            if (!portState.name().equals(dto.getState())) {
                switch (ScheduledState.valueOf(dto.getState())) {
                    case DISABLED:
                        // switch processor do disabled. This means we have to stop it (if it's already stopped, this method does nothing),
                        // and then we have to disable it.
                        controller.stopConnectable(port);
                        port.getProcessGroup().disableOutputPort(port);
                        break;
                    case RUNNING:
                        // we want to run now. Make sure processor is not disabled and then start it.
                        port.getProcessGroup().enableOutputPort(port);
                        controller.startConnectable(port);
                        break;
                    case STOPPED:
                        if (portState == ScheduledState.DISABLED) {
                            port.getProcessGroup().enableOutputPort(port);
                        } else if (portState == ScheduledState.RUNNING) {
                            controller.stopConnectable(port);
                        }
                        break;
                }
            }
        }

        // Update scheduled state of Remote Group Ports
        final List<Element> remoteProcessGroupList = getChildrenByTagName(processGroupElement, "remoteProcessGroup");
        for (final Element remoteGroupElement : remoteProcessGroupList) {
            final RemoteProcessGroupDTO remoteGroupDto = FlowFromDOMFactory.getRemoteProcessGroup(remoteGroupElement, encryptor);
            final RemoteProcessGroup rpg = processGroup.getRemoteProcessGroup(remoteGroupDto.getId());

            // input ports
            final List<Element> inputPortElements = getChildrenByTagName(remoteGroupElement, "inputPort");
            for (final Element inputPortElement : inputPortElements) {
                final RemoteProcessGroupPortDescriptor portDescriptor = FlowFromDOMFactory.getRemoteProcessGroupPort(inputPortElement);
                final String inputPortId = portDescriptor.getId();
                final RemoteGroupPort inputPort = rpg.getInputPort(inputPortId);
                if (inputPort == null) {
                    continue;
                }

                final ScheduledState portState = getScheduledState(inputPort, controller);

                if (portDescriptor.isTransmitting()) {
                    if (portState != ScheduledState.RUNNING && portState != ScheduledState.STARTING) {
                        controller.startTransmitting(inputPort);
                    }
                } else if (portState != ScheduledState.STOPPED && portState != ScheduledState.STOPPING) {
                    controller.stopTransmitting(inputPort);
                }
            }

            // output ports
            final List<Element> outputPortElements = getChildrenByTagName(remoteGroupElement, "outputPort");
            for (final Element outputPortElement : outputPortElements) {
                final RemoteProcessGroupPortDescriptor portDescriptor = FlowFromDOMFactory.getRemoteProcessGroupPort(outputPortElement);
                final String outputPortId = portDescriptor.getId();
                final RemoteGroupPort outputPort = rpg.getOutputPort(outputPortId);
                if (outputPort == null) {
                    continue;
                }

                final ScheduledState portState = getScheduledState(outputPort, controller);

                if (portDescriptor.isTransmitting()) {
                    if (portState != ScheduledState.RUNNING && portState != ScheduledState.STARTING) {
                        controller.startTransmitting(outputPort);
                    }
                } else if (portState != ScheduledState.STOPPED && portState != ScheduledState.STOPPING) {
                    controller.stopTransmitting(outputPort);
                }
            }
        }

        // add labels
        final List<Element> labelNodeList = getChildrenByTagName(processGroupElement, "label");
        for (final Element labelElement : labelNodeList) {
            final LabelDTO labelDTO = FlowFromDOMFactory.getLabel(labelElement);
            final Label label = flowManager.createLabel(labelDTO.getId(), labelDTO.getLabel());
            label.setStyle(labelDTO.getStyle());
            label.setPosition(new Position(labelDTO.getPosition().getX(), labelDTO.getPosition().getY()));
            label.setVersionedComponentId(labelDTO.getVersionedComponentId());
            if (labelDTO.getWidth() != null && labelDTO.getHeight() != null) {
                label.setSize(new Size(labelDTO.getWidth(), labelDTO.getHeight()));
            }

            processGroup.addLabel(label);
        }

        // update nested process groups (recursively)
        final List<Element> nestedProcessGroupNodeList = getChildrenByTagName(processGroupElement, "processGroup");
        for (final Element nestedProcessGroupElement : nestedProcessGroupNodeList) {
            updateProcessGroup(controller, processGroup, nestedProcessGroupElement, encryptor, encodingVersion);
        }

        // update connections
        final List<Element> connectionNodeList = getChildrenByTagName(processGroupElement, "connection");
        for (final Element connectionElement : connectionNodeList) {
            final ConnectionDTO dto = FlowFromDOMFactory.getConnection(connectionElement);

            final Connection connection = processGroup.getConnection(dto.getId());
            connection.setName(dto.getName());
            connection.setProcessGroup(processGroup);

            if (dto.getLabelIndex() != null) {
                connection.setLabelIndex(dto.getLabelIndex());
            }
            if (dto.getzIndex() != null) {
                connection.setZIndex(dto.getzIndex());
            }

            final List<Position> bendPoints = new ArrayList<>();
            for (final PositionDTO bend : dto.getBends()) {
                bendPoints.add(new Position(bend.getX(), bend.getY()));
            }
            connection.setBendPoints(bendPoints);

            List<FlowFilePrioritizer> newPrioritizers = null;
            final List<String> prioritizers = dto.getPrioritizers();
            if (prioritizers != null) {
                final List<String> newPrioritizersClasses = new ArrayList<>(prioritizers);
                newPrioritizers = new ArrayList<>();
                for (final String className : newPrioritizersClasses) {
                    try {
                        newPrioritizers.add(flowManager.createPrioritizer(className));
                    } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                        throw new IllegalArgumentException("Unable to set prioritizer " + className + ": " + e);
                    }
                }
            }

            if (newPrioritizers != null) {
                connection.getFlowFileQueue().setPriorities(newPrioritizers);
            }

            if (dto.getBackPressureObjectThreshold() != null) {
                connection.getFlowFileQueue().setBackPressureObjectThreshold(dto.getBackPressureObjectThreshold());
            }

            if (dto.getBackPressureDataSizeThreshold() != null && !dto.getBackPressureDataSizeThreshold().trim().isEmpty()) {
                connection.getFlowFileQueue().setBackPressureDataSizeThreshold(dto.getBackPressureDataSizeThreshold());
            }

            if (dto.getFlowFileExpiration() != null) {
                connection.getFlowFileQueue().setFlowFileExpiration(dto.getFlowFileExpiration());
            }
        }

        // Replace the templates with those from the proposed flow
        final List<Element> templateNodeList = getChildrenByTagName(processGroupElement, "template");
        for (final Element templateElement : templateNodeList) {
            final TemplateDTO templateDto = TemplateUtils.parseDto(templateElement);
            final Template template = new Template(templateDto);

            // If the Process Group already has the template, remove it and add it again. We do this
            // to ensure that all of the nodes have the same view of the template. Templates are immutable,
            // so any two nodes that have a template with the same ID should have the exact same template.
            // This just makes sure that they do.
            if (processGroup.getTemplate(template.getIdentifier()) != null) {
                processGroup.removeTemplate(template);
            }
            processGroup.addTemplate(template);
        }

        return processGroup;
    }

    /**
     * Updates the process group corresponding to the specified DTO. Any field
     * in DTO that is <code>null</code> (with the exception of the required ID)
     * will be ignored.
     *
     * @throws IllegalStateException if no process group can be found with the
     * ID of DTO or with the ID of the DTO's parentGroupId, if the template ID
     * specified is invalid, or if the DTO's Parent Group ID changes but the
     * parent group has incoming or outgoing connections
     *
     * @throws NullPointerException if the DTO or its ID is null
     */
    private void updateProcessGroup(final ProcessGroup group, final ProcessGroupDTO dto) {
        final String name = dto.getName();
        final PositionDTO position = dto.getPosition();
        final String comments = dto.getComments();

        if (name != null) {
            group.setName(name);
        }
        if (position != null) {
            group.setPosition(toPosition(position));
        }
        if (comments != null) {
            group.setComments(comments);
        }
    }

    private <T extends Connectable & Triggerable> ScheduledState getScheduledState(final T component, final FlowController flowController) {
        final ScheduledState componentState = component.getScheduledState();
        if (componentState == ScheduledState.STOPPED) {
            if (flowController.isStartAfterInitialization(component)) {
                return ScheduledState.RUNNING;
            }
        }

        return componentState;
    }

    private Position toPosition(final PositionDTO dto) {
        return new Position(dto.getX(), dto.getY());
    }

    private void updateProcessor(final ProcessorNode procNode, final ProcessorDTO processorDTO, final ProcessGroup processGroup, final FlowController controller) {

        procNode.pauseValidationTrigger();
        try {
            final ProcessorConfigDTO config = processorDTO.getConfig();
            procNode.setProcessGroup(processGroup);
            procNode.setLossTolerant(config.isLossTolerant());
            procNode.setPenalizationPeriod(config.getPenaltyDuration());
            procNode.setYieldPeriod(config.getYieldDuration());
            procNode.setBulletinLevel(LogLevel.valueOf(config.getBulletinLevel()));
            updateNonFingerprintedProcessorSettings(procNode, processorDTO);

            if (config.getSchedulingStrategy() != null) {
                procNode.setSchedulingStrategy(SchedulingStrategy.valueOf(config.getSchedulingStrategy()));
            }

            if (config.getExecutionNode() != null) {
                procNode.setExecutionNode(ExecutionNode.valueOf(config.getExecutionNode()));
            }

            // must set scheduling strategy before these two
            procNode.setMaxConcurrentTasks(config.getConcurrentlySchedulableTaskCount());
            procNode.setScheduldingPeriod(config.getSchedulingPeriod());
            if (config.getRunDurationMillis() != null) {
                procNode.setRunDuration(config.getRunDurationMillis(), TimeUnit.MILLISECONDS);
            }

            procNode.setAnnotationData(config.getAnnotationData());

            if (config.getAutoTerminatedRelationships() != null) {
                final Set<Relationship> relationships = new HashSet<>();
                for (final String rel : config.getAutoTerminatedRelationships()) {
                    relationships.add(procNode.getRelationship(rel));
                }
                procNode.setAutoTerminatedRelationships(relationships);
            }

            procNode.setProperties(config.getProperties());

            final ScheduledState scheduledState = ScheduledState.valueOf(processorDTO.getState());
            if (ScheduledState.RUNNING.equals(scheduledState)) {
                procNode.performValidation(); // ensure that processor has been validated
                controller.startProcessor(processGroup.getIdentifier(), procNode.getIdentifier());
            } else if (ScheduledState.DISABLED.equals(scheduledState)) {
                processGroup.disableProcessor(procNode);
            } else if (ScheduledState.STOPPED.equals(scheduledState)) {
                controller.stopProcessor(processGroup.getIdentifier(), procNode.getIdentifier());
            }
        } finally {
            procNode.resumeValidationTrigger();
        }
    }

    private void updateNonFingerprintedProcessorSettings(final ProcessorNode procNode, final ProcessorDTO processorDTO) {
        procNode.setName(processorDTO.getName());
        procNode.setPosition(toPosition(processorDTO.getPosition()));
        procNode.setStyle(processorDTO.getStyle());
        procNode.setComments(processorDTO.getConfig().getComments());
    }

    private ProcessGroup addProcessGroup(final FlowController controller, final ProcessGroup parentGroup, final Element processGroupElement,
            final StringEncryptor encryptor, final FlowEncodingVersion encodingVersion) {

        // get the parent group ID
        final String parentId = (parentGroup == null) ? null : parentGroup.getIdentifier();
        final FlowManager flowManager = controller.getFlowManager();

        // add the process group
        final ProcessGroupDTO processGroupDTO = FlowFromDOMFactory.getProcessGroup(parentId, processGroupElement, encryptor, encodingVersion);
        final ProcessGroup processGroup = flowManager.createProcessGroup(processGroupDTO.getId());
        processGroup.setComments(processGroupDTO.getComments());
        processGroup.setVersionedComponentId(processGroupDTO.getVersionedComponentId());
        processGroup.setPosition(toPosition(processGroupDTO.getPosition()));
        processGroup.setName(processGroupDTO.getName());
        processGroup.setParent(parentGroup);
        if (parentGroup == null) {
            controller.setRootGroup(processGroup);
        } else {
            parentGroup.addProcessGroup(processGroup);
        }

        // Set the variables for the variable registry
        final Map<String, String> variables = new HashMap<>();
        final List<Element> variableElements = getChildrenByTagName(processGroupElement, "variable");
        for (final Element variableElement : variableElements) {
            final String variableName = variableElement.getAttribute("name");
            final String variableValue = variableElement.getAttribute("value");
            if (variableName == null || variableValue == null) {
                continue;
            }

            variables.put(variableName, variableValue);
        }

        processGroup.setVariables(variables);

        final VersionControlInformationDTO versionControlInfoDto = processGroupDTO.getVersionControlInformation();
        if (versionControlInfoDto != null) {
            final FlowRegistry flowRegistry = controller.getFlowRegistryClient().getFlowRegistry(versionControlInfoDto.getRegistryId());
            final String registryName = flowRegistry == null ? versionControlInfoDto.getRegistryId() : flowRegistry.getName();

            versionControlInfoDto.setState(VersionedFlowState.SYNC_FAILURE.name());
            versionControlInfoDto.setStateExplanation("Process Group has not yet been synchronized with the Flow Registry");
            final StandardVersionControlInformation versionControlInformation = StandardVersionControlInformation.Builder.fromDto(versionControlInfoDto)
                .registryName(registryName)
                .build();

            // pass empty map for the version control mapping because the VersionedComponentId has already been set on the components
            processGroup.setVersionControlInformation(versionControlInformation, Collections.emptyMap());
        }

        // Add Controller Services
        final List<Element> serviceNodeList = getChildrenByTagName(processGroupElement, "controllerService");
        if (!serviceNodeList.isEmpty()) {
            final Map<ControllerServiceNode, Element> controllerServices = ControllerServiceLoader.loadControllerServices(serviceNodeList, controller, processGroup, encryptor);
            ControllerServiceLoader.enableControllerServices(controllerServices, controller, encryptor, autoResumeState);
        }

        // add processors
        final List<Element> processorNodeList = getChildrenByTagName(processGroupElement, "processor");
        for (final Element processorElement : processorNodeList) {
            final ProcessorDTO processorDTO = FlowFromDOMFactory.getProcessor(processorElement, encryptor);

            BundleCoordinate coordinate;
            try {
                coordinate = BundleUtils.getCompatibleBundle(extensionManager, processorDTO.getType(), processorDTO.getBundle());
            } catch (final IllegalStateException e) {
                final BundleDTO bundleDTO = processorDTO.getBundle();
                if (bundleDTO == null) {
                    coordinate = BundleCoordinate.UNKNOWN_COORDINATE;
                } else {
                    coordinate = new BundleCoordinate(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
                }
            }

            final ProcessorNode procNode = flowManager.createProcessor(processorDTO.getType(), processorDTO.getId(), coordinate, false);
            procNode.setVersionedComponentId(processorDTO.getVersionedComponentId());
            processGroup.addProcessor(procNode);
            updateProcessor(procNode, processorDTO, processGroup, controller);
        }

        // add input ports
        final List<Element> inputPortNodeList = getChildrenByTagName(processGroupElement, "inputPort");
        for (final Element inputPortElement : inputPortNodeList) {
            final PortDTO portDTO = FlowFromDOMFactory.getPort(inputPortElement);

            final Port port;
            if (processGroup.isRootGroup()) {
                port = flowManager.createRemoteInputPort(portDTO.getId(), portDTO.getName());
            } else {
                port = flowManager.createLocalInputPort(portDTO.getId(), portDTO.getName());
            }

            port.setVersionedComponentId(portDTO.getVersionedComponentId());
            port.setPosition(toPosition(portDTO.getPosition()));
            port.setComments(portDTO.getComments());
            port.setProcessGroup(processGroup);

            final Set<String> userControls = portDTO.getUserAccessControl();
            if (userControls != null && !userControls.isEmpty()) {
                if (!(port instanceof RootGroupPort)) {
                    throw new IllegalStateException("Attempting to add User Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((RootGroupPort) port).setUserAccessControl(userControls);
            }
            final Set<String> groupControls = portDTO.getGroupAccessControl();
            if (groupControls != null && !groupControls.isEmpty()) {
                if (!(port instanceof RootGroupPort)) {
                    throw new IllegalStateException("Attempting to add Group Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((RootGroupPort) port).setGroupAccessControl(groupControls);
            }

            processGroup.addInputPort(port);
            if (portDTO.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
            }

            final ScheduledState scheduledState = ScheduledState.valueOf(portDTO.getState());
            if (ScheduledState.RUNNING.equals(scheduledState)) {
                controller.startConnectable(port);
            } else if (ScheduledState.DISABLED.equals(scheduledState)) {
                processGroup.disableInputPort(port);
            }
        }

        // add output ports
        final List<Element> outputPortNodeList = getChildrenByTagName(processGroupElement, "outputPort");
        for (final Element outputPortElement : outputPortNodeList) {
            final PortDTO portDTO = FlowFromDOMFactory.getPort(outputPortElement);

            final Port port;
            if (processGroup.isRootGroup()) {
                port = flowManager.createRemoteOutputPort(portDTO.getId(), portDTO.getName());
            } else {
                port = flowManager.createLocalOutputPort(portDTO.getId(), portDTO.getName());
            }

            port.setVersionedComponentId(portDTO.getVersionedComponentId());
            port.setPosition(toPosition(portDTO.getPosition()));
            port.setComments(portDTO.getComments());
            port.setProcessGroup(processGroup);

            final Set<String> userControls = portDTO.getUserAccessControl();
            if (userControls != null && !userControls.isEmpty()) {
                if (!(port instanceof RootGroupPort)) {
                    throw new IllegalStateException("Attempting to add User Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((RootGroupPort) port).setUserAccessControl(userControls);
            }
            final Set<String> groupControls = portDTO.getGroupAccessControl();
            if (groupControls != null && !groupControls.isEmpty()) {
                if (!(port instanceof RootGroupPort)) {
                    throw new IllegalStateException("Attempting to add Group Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((RootGroupPort) port).setGroupAccessControl(groupControls);
            }

            processGroup.addOutputPort(port);
            if (portDTO.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
            }

            final ScheduledState scheduledState = ScheduledState.valueOf(portDTO.getState());
            if (ScheduledState.RUNNING.equals(scheduledState)) {
                controller.startConnectable(port);
            } else if (ScheduledState.DISABLED.equals(scheduledState)) {
                processGroup.disableOutputPort(port);
            }
        }

        // add funnels
        final List<Element> funnelNodeList = getChildrenByTagName(processGroupElement, "funnel");
        for (final Element funnelElement : funnelNodeList) {
            final FunnelDTO funnelDTO = FlowFromDOMFactory.getFunnel(funnelElement);
            final Funnel funnel = flowManager.createFunnel(funnelDTO.getId());
            funnel.setVersionedComponentId(funnelDTO.getVersionedComponentId());
            funnel.setPosition(toPosition(funnelDTO.getPosition()));

            // Since this is called during startup, we want to add the funnel without enabling it
            // and then tell the controller to enable it. This way, if the controller is not fully
            // initialized, the starting of the funnel is delayed until the controller is ready.
            processGroup.addFunnel(funnel, false);
            controller.startConnectable(funnel);
        }

        // add labels
        final List<Element> labelNodeList = getChildrenByTagName(processGroupElement, "label");
        for (final Element labelElement : labelNodeList) {
            final LabelDTO labelDTO = FlowFromDOMFactory.getLabel(labelElement);
            final Label label = flowManager.createLabel(labelDTO.getId(), labelDTO.getLabel());
            label.setVersionedComponentId(labelDTO.getVersionedComponentId());
            label.setStyle(labelDTO.getStyle());

            label.setPosition(toPosition(labelDTO.getPosition()));
            label.setSize(new Size(labelDTO.getWidth(), labelDTO.getHeight()));
            processGroup.addLabel(label);
        }

        // add nested process groups (recursively)
        final List<Element> nestedProcessGroupNodeList = getChildrenByTagName(processGroupElement, "processGroup");
        for (final Element nestedProcessGroupElement : nestedProcessGroupNodeList) {
            addProcessGroup(controller, processGroup, nestedProcessGroupElement, encryptor, encodingVersion);
        }

        // add remote process group
        final List<Element> remoteProcessGroupNodeList = getChildrenByTagName(processGroupElement, "remoteProcessGroup");
        for (final Element remoteProcessGroupElement : remoteProcessGroupNodeList) {
            final RemoteProcessGroupDTO remoteGroupDto = FlowFromDOMFactory.getRemoteProcessGroup(remoteProcessGroupElement, encryptor);
            final RemoteProcessGroup remoteGroup = flowManager.createRemoteProcessGroup(remoteGroupDto.getId(), remoteGroupDto.getTargetUris());
            remoteGroup.setVersionedComponentId(remoteGroupDto.getVersionedComponentId());
            remoteGroup.setComments(remoteGroupDto.getComments());
            remoteGroup.setPosition(toPosition(remoteGroupDto.getPosition()));
            final String name = remoteGroupDto.getName();
            if (name != null && !name.trim().isEmpty()) {
                remoteGroup.setName(name);
            }
            remoteGroup.setProcessGroup(processGroup);
            remoteGroup.setCommunicationsTimeout(remoteGroupDto.getCommunicationsTimeout());

            if (remoteGroupDto.getYieldDuration() != null) {
                remoteGroup.setYieldDuration(remoteGroupDto.getYieldDuration());
            }

            final String transportProtocol = remoteGroupDto.getTransportProtocol();
            if (transportProtocol != null && !transportProtocol.trim().isEmpty()) {
                remoteGroup.setTransportProtocol(SiteToSiteTransportProtocol.valueOf(transportProtocol.toUpperCase()));
            }

            if (remoteGroupDto.getProxyHost() != null) {
                remoteGroup.setProxyHost(remoteGroupDto.getProxyHost());
            }

            if (remoteGroupDto.getProxyPort() != null) {
                remoteGroup.setProxyPort(remoteGroupDto.getProxyPort());
            }

            if (remoteGroupDto.getProxyUser() != null) {
                remoteGroup.setProxyUser(remoteGroupDto.getProxyUser());
            }

            if (remoteGroupDto.getProxyPassword() != null) {
                remoteGroup.setProxyPassword(remoteGroupDto.getProxyPassword());
            }

            if (StringUtils.isBlank(remoteGroupDto.getLocalNetworkInterface())) {
                remoteGroup.setNetworkInterface(null);
            } else {
                remoteGroup.setNetworkInterface(remoteGroupDto.getLocalNetworkInterface());
            }

            final Set<RemoteProcessGroupPortDescriptor> inputPorts = new HashSet<>();
            for (final Element portElement : getChildrenByTagName(remoteProcessGroupElement, "inputPort")) {
                inputPorts.add(FlowFromDOMFactory.getRemoteProcessGroupPort(portElement));
            }
            remoteGroup.setInputPorts(inputPorts, false);

            final Set<RemoteProcessGroupPortDescriptor> outputPorts = new HashSet<>();
            for (final Element portElement : getChildrenByTagName(remoteProcessGroupElement, "outputPort")) {
                outputPorts.add(FlowFromDOMFactory.getRemoteProcessGroupPort(portElement));
            }
            remoteGroup.setOutputPorts(outputPorts, false);
            processGroup.addRemoteProcessGroup(remoteGroup);

            for (final RemoteProcessGroupPortDescriptor remoteGroupPortDTO : outputPorts) {
                final RemoteGroupPort port = remoteGroup.getOutputPort(remoteGroupPortDTO.getId());
                if (Boolean.TRUE.equals(remoteGroupPortDTO.isTransmitting())) {
                    controller.startTransmitting(port);
                }
            }
            for (final RemoteProcessGroupPortDescriptor remoteGroupPortDTO : inputPorts) {
                final RemoteGroupPort port = remoteGroup.getInputPort(remoteGroupPortDTO.getId());
                if (Boolean.TRUE.equals(remoteGroupPortDTO.isTransmitting())) {
                    controller.startTransmitting(port);
                }
            }
        }

        // add connections
        final List<Element> connectionNodeList = getChildrenByTagName(processGroupElement, "connection");
        for (final Element connectionElement : connectionNodeList) {
            final ConnectionDTO dto = FlowFromDOMFactory.getConnection(connectionElement);

            final Connectable source;
            final ConnectableDTO sourceDto = dto.getSource();
            if (ConnectableType.REMOTE_OUTPUT_PORT.name().equals(sourceDto.getType())) {
                final RemoteProcessGroup remoteGroup = processGroup.getRemoteProcessGroup(sourceDto.getGroupId());
                source = remoteGroup.getOutputPort(sourceDto.getId());
            } else {
                final ProcessGroup sourceGroup = flowManager.getGroup(sourceDto.getGroupId());
                if (sourceGroup == null) {
                    throw new RuntimeException("Found Invalid ProcessGroup ID for Source: " + dto.getSource().getGroupId());
                }

                source = sourceGroup.getConnectable(sourceDto.getId());
            }
            if (source == null) {
                throw new RuntimeException("Found Invalid Connectable ID for Source: " + dto.getSource().getId());
            }

            final Connectable destination;
            final ConnectableDTO destinationDto = dto.getDestination();
            if (ConnectableType.REMOTE_INPUT_PORT.name().equals(destinationDto.getType())) {
                final RemoteProcessGroup remoteGroup = processGroup.getRemoteProcessGroup(destinationDto.getGroupId());
                destination = remoteGroup.getInputPort(destinationDto.getId());
            } else {
                final ProcessGroup destinationGroup = flowManager.getGroup(destinationDto.getGroupId());
                if (destinationGroup == null) {
                    throw new RuntimeException("Found Invalid ProcessGroup ID for Destination: " + dto.getDestination().getGroupId());
                }

                destination = destinationGroup.getConnectable(destinationDto.getId());
            }
            if (destination == null) {
                throw new RuntimeException("Found Invalid Connectable ID for Destination: " + dto.getDestination().getId());
            }

            final Connection connection = flowManager.createConnection(dto.getId(), dto.getName(), source, destination, dto.getSelectedRelationships());
            connection.setVersionedComponentId(dto.getVersionedComponentId());
            connection.setProcessGroup(processGroup);

            final List<Position> bendPoints = new ArrayList<>();
            for (final PositionDTO bend : dto.getBends()) {
                bendPoints.add(new Position(bend.getX(), bend.getY()));
            }
            connection.setBendPoints(bendPoints);

            final Long zIndex = dto.getzIndex();
            if (zIndex != null) {
                connection.setZIndex(zIndex);
            }

            if (dto.getLabelIndex() != null) {
                connection.setLabelIndex(dto.getLabelIndex());
            }

            List<FlowFilePrioritizer> newPrioritizers = null;
            final List<String> prioritizers = dto.getPrioritizers();
            if (prioritizers != null) {
                final List<String> newPrioritizersClasses = new ArrayList<>(prioritizers);
                newPrioritizers = new ArrayList<>();
                for (final String className : newPrioritizersClasses) {
                    try {
                        newPrioritizers.add(flowManager.createPrioritizer(className));
                    } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                        throw new IllegalArgumentException("Unable to set prioritizer " + className + ": " + e);
                    }
                }
            }
            if (newPrioritizers != null) {
                connection.getFlowFileQueue().setPriorities(newPrioritizers);
            }

            if (dto.getBackPressureObjectThreshold() != null) {
                connection.getFlowFileQueue().setBackPressureObjectThreshold(dto.getBackPressureObjectThreshold());
            }
            if (dto.getBackPressureDataSizeThreshold() != null) {
                connection.getFlowFileQueue().setBackPressureDataSizeThreshold(dto.getBackPressureDataSizeThreshold());
            }
            if (dto.getFlowFileExpiration() != null) {
                connection.getFlowFileQueue().setFlowFileExpiration(dto.getFlowFileExpiration());
            }

            if (dto.getLoadBalanceStrategy() != null) {
                connection.getFlowFileQueue().setLoadBalanceStrategy(LoadBalanceStrategy.valueOf(dto.getLoadBalanceStrategy()), dto.getLoadBalancePartitionAttribute());
            }

            if (dto.getLoadBalanceCompression() != null) {
                connection.getFlowFileQueue().setLoadBalanceCompression(LoadBalanceCompression.valueOf(dto.getLoadBalanceCompression()));
            }

            processGroup.addConnection(connection);
        }

        final List<Element> templateNodeList = getChildrenByTagName(processGroupElement, "template");
        for (final Element templateNode : templateNodeList) {
            final TemplateDTO templateDTO = TemplateUtils.parseDto(templateNode);
            final Template template = new Template(templateDTO);
            processGroup.addTemplate(template);
        }

        return processGroup;
    }

    public String checkMissingComponentsInheritability(final DataFlow existingFlow, final DataFlow proposedFlow) {
        if (existingFlow == null) {
            return null;  // no existing flow, so equivalent to proposed flow
        }

        final Set<String> existingMissingComponents = new HashSet<>(existingFlow.getMissingComponents());
        existingMissingComponents.removeAll(proposedFlow.getMissingComponents());

        if (existingMissingComponents.size() > 0) {
            final String missingIds = StringUtils.join(existingMissingComponents, ",");
            return "Current flow has missing components that are not considered missing in the proposed flow (" + missingIds + ")";
        }

        final Set<String> proposedMissingComponents = new HashSet<>(proposedFlow.getMissingComponents());
        proposedMissingComponents.removeAll(existingFlow.getMissingComponents());

        if (proposedMissingComponents.size() > 0) {
            final String missingIds = StringUtils.join(proposedMissingComponents, ",");
            return "Proposed flow has missing components that are not considered missing in the current flow (" + missingIds + ")";
        }

        return null;
    }

    /**
     * If both authorizers are external authorizers, or if the both are internal
     * authorizers with equal fingerprints, then an uniheritable result with no
     * reason is returned to indicate nothing to do.
     *
     * If both are internal authorizers and the current authorizer is empty,
     * then an inheritable result is returned.
     *
     * All other cases return uninheritable with a reason which indicates to
     * throw an exception.
     *
     * @param existingFlow the existing DataFlow
     * @param proposedFlow the proposed DataFlow
     * @return the AuthorizerInheritability result
     */
    private AuthorizerInheritability checkAuthorizerInheritability(final Authorizer authorizer, final DataFlow existingFlow, final DataFlow proposedFlow) {
        final byte[] existing = existingFlow.getAuthorizerFingerprint();
        final byte[] proposed = proposedFlow.getAuthorizerFingerprint();

        // both are using external authorizers so nothing to inherit, but we don't want to throw an exception
        if (existing == null && proposed == null) {
            return AuthorizerInheritability.uninheritable(null);
        }

        // current is external, but proposed is internal
        if (existing == null && proposed != null) {
            return AuthorizerInheritability.uninheritable(
                    "Current Authorizer is an external Authorizer, but proposed Authorizer is an internal Authorizer");
        }

        // current is internal, but proposed is external
        if (existing != null && proposed == null) {
            return AuthorizerInheritability.uninheritable(
                    "Current Authorizer is an internal Authorizer, but proposed Authorizer is an external Authorizer");
        }

        // both are internal, but not the same
        if (!Arrays.equals(existing, proposed)) {
            if (AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
                final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;

                try {
                    // if the configurations are not equal, see if the manager indicates the proposed configuration is inheritable
                    managedAuthorizer.checkInheritability(new String(proposed, StandardCharsets.UTF_8));
                    return AuthorizerInheritability.inheritable();
                } catch (final UninheritableAuthorizationsException e) {
                    return AuthorizerInheritability.uninheritable("Proposed Authorizations do not match current Authorizations: " + e.getMessage());
                }
            } else {
                // should never hit since the existing is only null when authorizer is not managed
                return AuthorizerInheritability.uninheritable(
                        "Proposed Authorizations do not match current Authorizations and are not configured with an internal Authorizer");
            }
        }

        // both are internal and equal
        return AuthorizerInheritability.uninheritable(null);
    }

    /**
     * Returns true if the given controller can inherit the proposed flow
     * without orphaning flow files.
     *
     * @param existingFlow flow
     * @param controller the running controller
     * @param proposedFlow the flow to inherit
     *
     * @return null if the controller can inherit the specified flow, an
     * explanation of why it cannot be inherited otherwise
     *
     * @throws FingerprintException if flow fingerprints could not be generated
     */
    public String checkFlowInheritability(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController controller) throws FingerprintException {
        if (existingFlow == null) {
            return null;  // no existing flow, so equivalent to proposed flow
        }

        return checkFlowInheritability(existingFlow.getFlow(), proposedFlow.getFlow(), controller);
    }

    private String checkFlowInheritability(final byte[] existingFlow, final byte[] proposedFlow, final FlowController controller) {
        if (existingFlow == null) {
            return null; // no existing flow, so equivalent to proposed flow
        }

        // check if the Flow is inheritable
        final FingerprintFactory fingerprintFactory = new FingerprintFactory(encryptor, extensionManager);
        final String existingFlowFingerprintBeforeHash = fingerprintFactory.createFingerprint(existingFlow, controller);
        if (existingFlowFingerprintBeforeHash.trim().isEmpty()) {
            return null;  // no existing flow, so equivalent to proposed flow
        }

        if (proposedFlow == null || proposedFlow.length == 0) {
            return "Proposed Flow was empty but Current Flow is not";  // existing flow is not empty and proposed flow is empty (we could orphan flowfiles)
        }

        final String proposedFlowFingerprintBeforeHash = fingerprintFactory.createFingerprint(proposedFlow, controller);
        if (proposedFlowFingerprintBeforeHash.trim().isEmpty()) {
            return "Proposed Flow was empty but Current Flow is not";  // existing flow is not empty and proposed flow is empty (we could orphan flowfiles)
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Local Fingerprint Before Hash = {}", new Object[] {existingFlowFingerprintBeforeHash});
            logger.trace("Proposed Fingerprint Before Hash = {}", new Object[] {proposedFlowFingerprintBeforeHash});
        }

        final boolean inheritable = existingFlowFingerprintBeforeHash.equals(proposedFlowFingerprintBeforeHash);
        if (!inheritable) {
            return findFirstDiscrepancy(existingFlowFingerprintBeforeHash, proposedFlowFingerprintBeforeHash, "Flows");
        }

        return null;
    }

    private String findFirstDiscrepancy(final String existing, final String proposed, final String comparisonDescription) {
        final int shortestFileLength = Math.min(existing.length(), proposed.length());
        for (int i = 0; i < shortestFileLength; i++) {
            if (existing.charAt(i) != proposed.charAt(i)) {
                final String formattedExistingDelta = formatFlowDiscrepancy(existing, i, 100);
                final String formattedProposedDelta = formatFlowDiscrepancy(proposed, i, 100);
                return String.format("Found difference in %s:\nLocal Fingerprint:   %s\nCluster Fingerprint: %s", comparisonDescription, formattedExistingDelta, formattedProposedDelta);
            }
        }

        // existing must startWith proposed or proposed must startWith existing
        if (existing.length() > proposed.length()) {
            final String formattedExistingDelta = existing.substring(proposed.length(), Math.min(existing.length(), proposed.length() + 200));
            return String.format("Found difference in %s:\nLocal Fingerprint contains additional configuration from Cluster Fingerprint: %s", comparisonDescription, formattedExistingDelta);
        } else if (proposed.length() > existing.length()) {
            final String formattedProposedDelta = proposed.substring(existing.length(), Math.min(proposed.length(), existing.length() + 200));
            return String.format("Found difference in %s:\nCluster Fingerprint contains additional configuration from Local Fingerprint: %s", comparisonDescription, formattedProposedDelta);
        }

        return "Unable to find any discrepancies between fingerprints. Please contact the NiFi support team";
    }

    private byte[] toBytes(final FlowController flowController) throws FlowSerializationException {
        final ByteArrayOutputStream result = new ByteArrayOutputStream();
        final StandardFlowSerializer flowSerializer = new StandardFlowSerializer(encryptor);
        flowController.serialize(flowSerializer, result);
        return result.toByteArray();
    }

    private static String getString(final Element element, final String childElementName) {
        final List<Element> nodeList = getChildrenByTagName(element, childElementName);
        if (nodeList == null || nodeList.isEmpty()) {
            return "";
        }
        final Element childElement = nodeList.get(0);
        return childElement.getTextContent();
    }

    private static int getInt(final Element element, final String childElementName) {
        return Integer.parseInt(getString(element, childElementName));
    }

    private static Integer getInteger(final Element element, final String childElementName) {
        final String value = getString(element, childElementName);
        return (value == null || value.trim().equals("") ? null : Integer.parseInt(value));
    }

    private static List<Element> getChildrenByTagName(final Element element, final String tagName) {
        final List<Element> matches = new ArrayList<>();
        final NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            final Node node = nodeList.item(i);
            if (!(node instanceof Element)) {
                continue;
            }

            final Element child = (Element) nodeList.item(i);
            if (child.getNodeName().equals(tagName)) {
                matches.add(child);
            }
        }

        return matches;
    }

    private String formatFlowDiscrepancy(final String flowFingerprint, final int deltaIndex, final int deltaPad) {
        return flowFingerprint.substring(Math.max(0, deltaIndex - deltaPad), Math.min(flowFingerprint.length(), deltaIndex + deltaPad));
    }

    /**
     * Holder for the result of determining if a proposed Authorizer is
     * inheritable.
     */
    private static final class AuthorizerInheritability {

        private final boolean inheritable;
        private final String reason;

        public AuthorizerInheritability(boolean inheritable, String reason) {
            this.inheritable = inheritable;
            this.reason = reason;
        }

        public boolean isInheritable() {
            return inheritable;
        }

        public String getReason() {
            return reason;
        }

        public static AuthorizerInheritability uninheritable(String reason) {
            return new AuthorizerInheritability(false, reason);
        }

        public static AuthorizerInheritability inheritable() {
            return new AuthorizerInheritability(true, null);
        }

    }

}
