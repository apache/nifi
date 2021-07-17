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
import org.apache.nifi.controller.inheritance.AuthorizerCheck;
import org.apache.nifi.controller.inheritance.BundleCompatibilityCheck;
import org.apache.nifi.controller.inheritance.ConnectionMissingCheck;
import org.apache.nifi.controller.inheritance.FlowFingerprintCheck;
import org.apache.nifi.controller.inheritance.FlowInheritability;
import org.apache.nifi.controller.inheritance.FlowInheritabilityCheck;
import org.apache.nifi.controller.inheritance.MissingComponentsCheck;
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
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.FlowFileConcurrency;
import org.apache.nifi.groups.FlowFileOutboundPolicy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 */
public class StandardFlowSynchronizer implements FlowSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(StandardFlowSynchronizer.class);
    private final PropertyEncryptor encryptor;
    private final boolean autoResumeState;
    private final NiFiProperties nifiProperties;
    private final ExtensionManager extensionManager;

    public StandardFlowSynchronizer(final PropertyEncryptor encryptor, final NiFiProperties nifiProperties, final ExtensionManager extensionManager) {
        this.encryptor = encryptor;
        this.autoResumeState = nifiProperties.getAutoResumeState();
        this.nifiProperties = nifiProperties;
        this.extensionManager = extensionManager;
    }

    public static boolean isEmpty(final DataFlow dataFlow) {
        if (dataFlow == null || dataFlow.getFlow() == null || dataFlow.getFlow().length == 0) {
            return true;
        }

        return isFlowEmpty(dataFlow.getFlowDocument());
    }

    @Override
    public void sync(final FlowController controller, final DataFlow proposedFlow, final PropertyEncryptor encryptor, final FlowService flowService)
            throws FlowSerializationException, UninheritableFlowException, FlowSynchronizationException {

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
        final DataFlow existingDataFlow = getExistingDataFlow(controller);
        boolean existingFlowEmpty = isFlowEmpty(existingDataFlow.getFlowDocument());

        logger.trace("Parsing proposed flow bytes as DOM document");
        final Document configuration = proposedFlow.getFlowDocument();

        // check that the proposed flow is inheritable by the controller
        boolean backupAndPurge = false;
        if (existingFlowEmpty) {
            logger.debug("Checking bundle compatibility");

            final BundleCompatibilityCheck bundleCompatibilityCheck = new BundleCompatibilityCheck();
            final FlowInheritability bundleInheritability = bundleCompatibilityCheck.checkInheritability(existingDataFlow, proposedFlow, controller);
            if (!bundleInheritability.isInheritable()) {
                throw new UninheritableFlowException("Proposed flow could not be inherited because it references one or more Bundles that are not available in this NiFi instance: "
                    + bundleInheritability.getExplanation());
            }

            logger.debug("Bundle Compatibility check passed");
        } else {
            logger.debug("Checking flow inheritability");
            final FlowInheritabilityCheck fingerprintCheck = new FlowFingerprintCheck();
            final FlowInheritability inheritability = fingerprintCheck.checkInheritability(existingDataFlow, proposedFlow, controller);

            if (inheritability.isInheritable()) {
                logger.debug("Proposed flow is inheritable");
            } else {
                if (controller.isInitialized()) {
                    // Flow has already been initialized so cannot inherit the cluster flow as liberally.
                    // Since the cluster's flow is not immediately inheritable, we must throw an UninheritableFlowException.
                    throw new UninheritableFlowException("Proposed configuration is not inheritable by the flow controller because of flow differences: " + inheritability.getExplanation());
                }

                logger.debug("Proposed flow is not directly inheritable. However, the Controller has not been synchronized yet, " +
                    "so will check if the existing flow can be backed up and replaced by the proposed flow.");

                final FlowInheritabilityCheck connectionMissingCheck = new ConnectionMissingCheck();
                final FlowInheritability connectionMissingInheritability = connectionMissingCheck.checkInheritability(existingDataFlow, proposedFlow, controller);
                if (connectionMissingInheritability.isInheritable()) {
                    backupAndPurge = true;
                    existingFlowEmpty = true; // Consider the existing flow as being empty
                    logger.debug("Proposed flow contains all connections that currently have data queued. Will backup existing flow and replace, provided all other checks pass");
                } else {
                    throw new UninheritableFlowException("Proposed flow is not inheritable by the flow controller and cannot completely replace the current flow due to: "
                        + connectionMissingInheritability.getExplanation());
                }
            }
        }

        logger.debug("Checking missing component inheritability");
        final FlowInheritabilityCheck missingComponentsCheck = new MissingComponentsCheck();
        final FlowInheritability componentInheritability = missingComponentsCheck.checkInheritability(existingDataFlow, proposedFlow, controller);
        if (!componentInheritability.isInheritable()) {
            throw new UninheritableFlowException("Proposed Flow is not inheritable by the flow controller because of differences in missing components: " + componentInheritability.getExplanation());
        }
        logger.debug("Missing Component Inheritability check passed");

        logger.debug("Checking authorizer inheritability");
        final FlowInheritabilityCheck authorizerCheck = new AuthorizerCheck();
        final FlowInheritability authorizerInheritability = authorizerCheck.checkInheritability(existingDataFlow, proposedFlow, controller);
        final Authorizer authorizer = controller.getAuthorizer();

        if (existingFlowEmpty) {
            logger.debug("Existing flow is empty so will not check Authorizer inheritability. Authorizers will be forcibly inherited if necessary.");
        } else {
            if (!controller.isInitialized() && authorizer instanceof ManagedAuthorizer) {
                logger.debug("Authorizations are not inheritable, but Authorizer is a Managed Authorizer and the Controller has not yet been initialized, so it can be forcibly inherited.");
            } else {
                if (!authorizerInheritability.isInheritable() && authorizerInheritability.getExplanation() != null) {
                    throw new UninheritableFlowException("Proposed Authorizer is not inheritable by the Flow Controller because NiFi has already started the dataflow " +
                        "and Authorizer has differences: " + authorizerInheritability.getExplanation());
                }

                logger.debug("Authorizer inheritability check passed");
            }
        }

        // attempt to sync controller with proposed flow
        try {
            if (backupAndPurge) {
                logger.warn("Proposed flow cannot be directly inherited. However, all data that is queued in this instance is queued in a connection that exists in the Proposed flow. As a " +
                    "result, the existing flow will be backed up and replaced with the proposed flow.");
                final File backupFile = getFlowBackupFile();

                try {
                    flowService.copyCurrentFlow(backupFile);
                } catch (final IOException ioe) {
                    throw new UninheritableFlowException("Could not inherit flow because failed to make a backup of existing flow to " + backupFile.getAbsolutePath(), ioe);
                }

                logger.info("Successfully created backup of existing flow to {}. Will now purge local flow and inherit proposed flow", backupFile.getAbsolutePath());
                controller.purge();
            }

            if (!controller.isFlowSynchronized() && !existingFlowEmpty) {
                updateThreadCounts(existingDataFlow.getFlowDocument().getDocumentElement(), controller);
            }

            if (configuration != null) {
                updateFlow(controller, configuration, existingDataFlow, existingFlowEmpty);
            }

            inheritSnippets(controller, proposedFlow);

            // if auths are inheritable and we have a policy based authorizer, then inherit
            if (authorizer instanceof ManagedAuthorizer) {
                final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
                final String proposedAuthFingerprint = proposedFlow.getAuthorizerFingerprint() == null ? "" : new String(proposedFlow.getAuthorizerFingerprint(), StandardCharsets.UTF_8);

                if (authorizerInheritability.isInheritable()) {
                    logger.debug("Authorizations are inheritable. Will inherit from proposed fingerprint {}", proposedAuthFingerprint);
                    managedAuthorizer.inheritFingerprint(proposedAuthFingerprint);
                } else if (!Objects.equals(managedAuthorizer.getFingerprint(), proposedAuthFingerprint)) {
                    // At this point, the flow is not inheritable, but we've made it this far. This can only happen if the existing flow is empty, so we can
                    // just forcibly inherit the authorizations.
                    logger.debug("Authorizations are not inheritable. Will force inheritance of proposed fingerprint {}", proposedAuthFingerprint);
                    managedAuthorizer.forciblyInheritFingerprint(proposedAuthFingerprint);
                }
            }

            logger.debug("Finished syncing flows");
        } catch (final Exception ex) {
            throw new FlowSynchronizationException(ex);
        }
    }

    private File getFlowBackupFile() {
        final File flowConfigurationFile = nifiProperties.getFlowConfigurationFile();
        final String baseFilename = StringUtils.substringBeforeLast(flowConfigurationFile.getName(), ".xml.gz");
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        final String timestamp = dateFormat.format(new Date());
        final String backupFilename = baseFilename + "-" + timestamp + ".xml.gz";
        final File backupFile = new File(flowConfigurationFile.getParentFile(), backupFilename);

        if (!backupFile.getParentFile().exists() && !backupFile.getParentFile().mkdirs()) {
            throw new UninheritableFlowException("Failed to backup existing flow because the configured directory for flow.xml.gz <" + backupFile.getParentFile().getAbsolutePath()
                + "> does not exist and could not be created");
        }

        return backupFile;
    }

    private DataFlow getExistingDataFlow(final FlowController controller) {
        final FlowManager flowManager = controller.getFlowManager();
        final ProcessGroup root = flowManager.getRootGroup();

        // Determine missing components
        final Set<String> missingComponents = new HashSet<>();
        flowManager.getAllControllerServices().stream().filter(ComponentNode::isExtensionMissing).forEach(cs -> missingComponents.add(cs.getIdentifier()));
        flowManager.getAllReportingTasks().stream().filter(ComponentNode::isExtensionMissing).forEach(r -> missingComponents.add(r.getIdentifier()));
        root.findAllProcessors().stream().filter(AbstractComponentNode::isExtensionMissing).forEach(p -> missingComponents.add(p.getIdentifier()));

        logger.trace("Exporting snippets from controller");
        final byte[] existingSnippets = controller.getSnippetManager().export();

        final byte[] existingAuthFingerprint;
        final Authorizer authorizer = controller.getAuthorizer();
        if (AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
            existingAuthFingerprint = managedAuthorizer.getFingerprint().getBytes(StandardCharsets.UTF_8);
        } else {
            existingAuthFingerprint = null;
        }

        // serialize controller state to bytes
        final byte[] existingFlow;
        try {
            if (controller.isFlowSynchronized()) {
                existingFlow = toBytes(controller);
                return new StandardDataFlow(existingFlow, existingSnippets, existingAuthFingerprint, missingComponents);
            } else {
                existingFlow = readFlowFromDisk();
                if (existingFlow == null || existingFlow.length == 0) {
                    return new StandardDataFlow(existingFlow, existingSnippets, existingAuthFingerprint, missingComponents);
                } else {
                    return new StandardDataFlow(existingFlow, existingSnippets, existingAuthFingerprint, missingComponents);
                }
            }
        } catch (final IOException e) {
            throw new FlowSerializationException(e);
        }
    }

    private void inheritSnippets(final FlowController controller, final DataFlow proposedFlow) {
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
    }

    private void updateFlow(final FlowController controller, final Document configuration, final DataFlow existingFlow, final boolean existingFlowEmpty) throws ReportingTaskInstantiationException {
        final boolean flowAlreadySynchronized = controller.isFlowSynchronized();
        final FlowManager flowManager = controller.getFlowManager();

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

        // if this controller isn't initialized or its empty, add the root group, otherwise update
        final ProcessGroup rootGroup;
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

            final Element parameterContextsElement = DomUtils.getChild(rootElement, "parameterContexts");
            if (parameterContextsElement != null) {
                final List<Element> contextElements = DomUtils.getChildElementsByTagName(parameterContextsElement, "parameterContext");
                for (final Element contextElement : contextElements) {
                    final ParameterContextDTO parameterContextDto = FlowFromDOMFactory.getParameterContext(contextElement, encryptor);
                    createParameterContext(parameterContextDto, controller.getFlowManager());
                }
            }

            logger.trace("Adding root process group");
            rootGroup = addProcessGroup(controller, /* parent group */ null, rootGroupElement, encryptor, encodingVersion);
        } else {
            logger.trace("Updating root process group");
            rootGroup = updateProcessGroup(controller, /* parent group */ null, rootGroupElement, encryptor, encodingVersion);
        }

        rootGroup.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);

        // If there are any Templates that do not exist in the Proposed Flow that do exist in the 'existing flow', we need
        // to ensure that we also add those to the appropriate Process Groups, so that we don't lose them.
        if (!existingFlowEmpty) {
            final Document existingFlowConfiguration = existingFlow.getFlowDocument();
            if (existingFlowConfiguration != null) {
                final Element existingRootElement = (Element) existingFlowConfiguration.getElementsByTagName("flowController").item(0);
                if (existingRootElement != null) {
                    final Element existingRootGroupElement = (Element) existingRootElement.getElementsByTagName("rootGroup").item(0);
                    if (existingRootElement != null) {
                        addLocalTemplates(existingRootGroupElement, rootGroup);
                    }
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
            final ReportingTaskDTO dto = FlowFromDOMFactory.getReportingTask(taskElement, encryptor, encodingVersion);
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
                final Map<ControllerServiceNode, Element> controllerServices = ControllerServiceLoader.loadControllerServices(
                    serviceElements, controller, group, encryptor, encodingVersion);

                // If we are moving controller services to the root group we also need to see if any reporting tasks
                // reference them, and if so we need to clone the CS and update the reporting task reference
                if (group != null) {
                    // find all the controller service ids referenced by reporting tasks
                    final Set<String> controllerServicesInReportingTasks = reportingTaskNodesToDTOs.keySet().stream()
                        .flatMap(r -> r.getEffectivePropertyValues().entrySet().stream())
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
                ControllerServiceLoader.enableControllerServices(controllerServices, controller, encryptor, autoResumeState, encodingVersion);
            }
        }

        scaleRootGroup(rootGroup, encodingVersion);

        // now that controller services are loaded and enabled we can apply the scheduled state to each reporting task
        for (Map.Entry<ReportingTaskNode, ReportingTaskDTO> entry : reportingTaskNodesToDTOs.entrySet()) {
            applyReportingTaskScheduleState(controller, entry.getValue(), entry.getKey(), flowAlreadySynchronized, existingFlowEmpty);
        }
    }

    private ParameterContext createParameterContext(final ParameterContextDTO dto, final FlowManager flowManager) {
        final Map<String, Parameter> parameters = dto.getParameters().stream()
            .map(ParameterEntity::getParameter)
            .map(this::createParameter)
            .collect(Collectors.toMap(param -> param.getDescriptor().getName(), Function.identity()));

        final ParameterContext context = flowManager.createParameterContext(dto.getId(), dto.getName(), parameters);
        context.setDescription(dto.getDescription());
        return context;
    }

    private Parameter createParameter(final ParameterDTO dto) {
        final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder()
            .name(dto.getName())
            .description(dto.getDescription())
            .sensitive(Boolean.TRUE.equals(dto.getSensitive()))
            .build();

        return new Parameter(parameterDescriptor, dto.getValue());
    }

    private void updateReportingTaskControllerServices(final Set<ReportingTaskNode> reportingTasks, final Map<String, ControllerServiceNode> controllerServiceMapping) {
        for (ReportingTaskNode reportingTask : reportingTasks) {
            if (reportingTask.getProperties() != null) {
                reportingTask.pauseValidationTrigger();
                try {
                    final Set<Map.Entry<PropertyDescriptor, String>> propertyDescriptors = reportingTask.getEffectivePropertyValues().entrySet().stream()
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

    private void addLocalTemplates(final Element processGroupElement, final ProcessGroup processGroup) {
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
            addLocalTemplates(childGroupElement, childGroup);
        }
    }

    void scaleRootGroup(final ProcessGroup rootGroup, final FlowEncodingVersion encodingVersion) {
        if (encodingVersion == null || encodingVersion.getMajorVersion() < 1) {
            // Calculate new Positions if the encoding version of the flow is older than 1.0.
            PositionScaler.scale(rootGroup, 1.5, 1.34);
        }
    }

    private void updateThreadCounts(final Element rootElement, final FlowController controller) {
        logger.trace("Setting controller thread counts");
        final Integer maxThreadCount = getInteger(rootElement, "maxThreadCount");
        if (maxThreadCount == null) {
            controller.setMaxTimerDrivenThreadCount(getInt(rootElement, "maxTimerDrivenThreadCount"));
            controller.setMaxEventDrivenThreadCount(getInt(rootElement, "maxEventDrivenThreadCount"));
        } else {
            controller.setMaxTimerDrivenThreadCount(maxThreadCount * 2 / 3);
            controller.setMaxEventDrivenThreadCount(maxThreadCount / 3);
        }
    }


    private static boolean isFlowEmpty(final Document flowDocument) {
        if (flowDocument == null) {
            return true;
        }

        final Element rootElement = flowDocument.getDocumentElement();
        if (rootElement == null) {
            return true;
        }

        final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootElement);

        final Element reportingTasksElement = DomUtils.getChild(rootElement, "reportingTasks");
        if (reportingTasksElement != null) {
            final List<Element> taskElements = DomUtils.getChildElementsByTagName(reportingTasksElement, "reportingTask");
            if (!taskElements.isEmpty()) {
                return false;
            }
        }

        final Element controllerServicesElement = DomUtils.getChild(rootElement, "controllerServices");
        if (controllerServicesElement != null) {
            final List<Element> unrootedControllerServiceElements = DomUtils.getChildElementsByTagName(controllerServicesElement, "controllerService");
            if (!unrootedControllerServiceElements.isEmpty()) {
                return false;
            }
        }

        final Element registriesElement = DomUtils.getChild(rootElement, "registries");
        if (registriesElement != null) {
            final List<Element> flowRegistryElems = DomUtils.getChildElementsByTagName(registriesElement, "flowRegistry");
            if (!flowRegistryElems.isEmpty()) {
                return false;
            }
        }

        final Element parameterContextsElement = DomUtils.getChild(rootElement, "parameterContexts");
        if (parameterContextsElement != null) {
            final List<Element> contextList = DomUtils.getChildElementsByTagName(parameterContextsElement, "parameterContext");
            if (!contextList.isEmpty()) {
                return false;
            }
        }

        logger.trace("Parsing process group from DOM");
        final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
        final ProcessGroupDTO rootGroupDto = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, null, encodingVersion);
        return isEmpty(rootGroupDto);
    }

    private static boolean isEmpty(final ProcessGroupDTO dto) {
        if (dto == null) {
            return true;
        }

        final FlowSnippetDTO contents = dto.getContents();
        if (contents == null) {
            return true;
        }

        final String parameterContextId = dto.getParameterContext() == null ? null : dto.getParameterContext().getId();

        return CollectionUtils.isEmpty(contents.getProcessors())
                && CollectionUtils.isEmpty(contents.getConnections())
                && CollectionUtils.isEmpty(contents.getFunnels())
                && CollectionUtils.isEmpty(contents.getLabels())
                && CollectionUtils.isEmpty(contents.getInputPorts())
                && CollectionUtils.isEmpty(contents.getOutputPorts())
                && CollectionUtils.isEmpty(contents.getProcessGroups())
                && CollectionUtils.isEmpty(contents.getRemoteProcessGroups())
                && CollectionUtils.isEmpty(contents.getControllerServices())
                && parameterContextId == null;
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
            final PropertyEncryptor encryptor, final FlowEncodingVersion encodingVersion) {

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
        final ProcessGroup processGroup = flowManager.getGroup(processGroupDto.getId());
        if (processGroup == null) {
            throw new IllegalStateException("No Group with ID " + processGroupDto.getId() + " exists");
        }

        updateProcessGroup(processGroup, processGroupDto, controller.getFlowManager().getParameterContextManager());

        // determine the scheduled state of all of the Controller Service
        final List<Element> controllerServiceNodeList = getChildrenByTagName(processGroupElement, "controllerService");
        final Set<ControllerServiceNode> toDisable = new HashSet<>();
        final Set<ControllerServiceNode> toEnable = new HashSet<>();

        for (final Element serviceElement : controllerServiceNodeList) {
            final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(serviceElement, encryptor, encodingVersion);
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
            final ProcessorDTO dto = FlowFromDOMFactory.getProcessor(processorElement, encryptor, encodingVersion);
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
                        case RUN_ONCE:
                            if (procState == ScheduledState.DISABLED) {
                                procNode.getProcessGroup().enableProcessor(procNode);
                            } else if (procState == ScheduledState.RUNNING || procState == ScheduledState.RUN_ONCE) {
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
     * will be ignored, or in the case of back pressure settings, will obtain
     * value from the parent of this process group
     *
     * @throws IllegalStateException if no process group can be found with the
     * ID of DTO or with the ID of the DTO's parentGroupId, if the template ID
     * specified is invalid, or if the DTO's Parent Group ID changes but the
     * parent group has incoming or outgoing connections
     *
     * @throws NullPointerException if the DTO or its ID is null
     */
    private void updateProcessGroup(final ProcessGroup group, final ProcessGroupDTO dto, final ParameterContextManager parameterContextManager) {
        final String name = dto.getName();
        final PositionDTO position = dto.getPosition();
        final String comments = dto.getComments();
        final String flowfileConcurrencyName = dto.getFlowfileConcurrency();
        final String flowfileOutboundPolicyName = dto.getFlowfileOutboundPolicy();
        final String defaultFlowFileExpiration = dto.getDefaultFlowFileExpiration();
        final Long defaultBackPressureObjectThreshold = dto.getDefaultBackPressureObjectThreshold();
        final String defaultBackPressureDataSizeThreshold = dto.getDefaultBackPressureDataSizeThreshold();

        if (name != null) {
            group.setName(name);
        }
        if (position != null) {
            group.setPosition(toPosition(position));
        }
        if (comments != null) {
            group.setComments(comments);
        }

        if (flowfileConcurrencyName == null) {
            group.setFlowFileConcurrency(FlowFileConcurrency.UNBOUNDED);
        } else {
            group.setFlowFileConcurrency(FlowFileConcurrency.valueOf(flowfileConcurrencyName));
        }

        if (flowfileOutboundPolicyName == null) {
            group.setFlowFileOutboundPolicy(FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE);
        } else {
            group.setFlowFileOutboundPolicy(FlowFileOutboundPolicy.valueOf(flowfileOutboundPolicyName));
        }

        final ParameterContextReferenceEntity parameterContextReference = dto.getParameterContext();
        if (parameterContextReference != null && parameterContextReference.getId() != null) {
            final String parameterContextId = parameterContextReference.getId();
            final ParameterContext parameterContext = parameterContextManager.getParameterContext(parameterContextId);
            if (!Objects.equals(parameterContext, group.getParameterContext())) {
                group.setParameterContext(parameterContext);
            }
        }

        if (defaultFlowFileExpiration != null) {
            group.setDefaultFlowFileExpiration(defaultFlowFileExpiration);
        }
        if (defaultBackPressureObjectThreshold != null) {
            group.setDefaultBackPressureObjectThreshold(defaultBackPressureObjectThreshold);
        }
        if (defaultBackPressureDataSizeThreshold != null) {
            group.setDefaultBackPressureDataSizeThreshold(defaultBackPressureDataSizeThreshold);
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
            final PropertyEncryptor encryptor, final FlowEncodingVersion encodingVersion) {

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

        final String flowfileConcurrencyName = processGroupDTO.getFlowfileConcurrency();
        final String flowfileOutboundPolicyName = processGroupDTO.getFlowfileOutboundPolicy();
        if (flowfileConcurrencyName == null) {
            processGroup.setFlowFileConcurrency(FlowFileConcurrency.UNBOUNDED);
        } else {
            processGroup.setFlowFileConcurrency(FlowFileConcurrency.valueOf(flowfileConcurrencyName));
        }

        if (flowfileOutboundPolicyName == null) {
            processGroup.setFlowFileOutboundPolicy(FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE);
        } else {
            processGroup.setFlowFileOutboundPolicy(FlowFileOutboundPolicy.valueOf(flowfileOutboundPolicyName));
        }

        processGroup.setDefaultFlowFileExpiration(processGroupDTO.getDefaultFlowFileExpiration());
        processGroup.setDefaultBackPressureObjectThreshold(processGroupDTO.getDefaultBackPressureObjectThreshold());
        processGroup.setDefaultBackPressureDataSizeThreshold(processGroupDTO.getDefaultBackPressureDataSizeThreshold());

        final String parameterContextId = getString(processGroupElement, "parameterContextId");
        if (parameterContextId != null) {
            final ParameterContext parameterContext = controller.getFlowManager().getParameterContextManager().getParameterContext(parameterContextId);
            processGroup.setParameterContext(parameterContext);
        }

        addVariables(processGroupElement, processGroup);
        addVersionControlInfo(processGroup, processGroupDTO, controller);
        addControllerServices(processGroupElement, processGroup, controller, encodingVersion);
        addProcessors(processGroupElement, processGroup, controller, encodingVersion);
        addInputPorts(processGroupElement, processGroup, controller);
        addOutputPorts(processGroupElement, processGroup, controller);
        addFunnels(processGroupElement, processGroup, controller);
        addLabels(processGroupElement, processGroup, controller);
        addNestedProcessGroups(processGroupElement, processGroup, controller, encodingVersion);
        addRemoteProcessGroups(processGroupElement, processGroup, controller);
        addConnections(processGroupElement, processGroup, controller);
        addTemplates(processGroupElement, processGroup);

        return processGroup;
    }

    private void addNestedProcessGroups(final Element processGroupElement, final ProcessGroup processGroup, final FlowController flowController, final FlowEncodingVersion encodingVersion) {
        final List<Element> nestedProcessGroupNodeList = getChildrenByTagName(processGroupElement, "processGroup");
        for (final Element nestedProcessGroupElement : nestedProcessGroupNodeList) {
            addProcessGroup(flowController, processGroup, nestedProcessGroupElement, encryptor, encodingVersion);
        }
    }

    private void addVersionControlInfo(final ProcessGroup processGroup, final ProcessGroupDTO processGroupDTO, final FlowController flowController) {
        final VersionControlInformationDTO versionControlInfoDto = processGroupDTO.getVersionControlInformation();
        if (versionControlInfoDto != null) {
            final FlowRegistry flowRegistry = flowController.getFlowRegistryClient().getFlowRegistry(versionControlInfoDto.getRegistryId());
            final String registryName = flowRegistry == null ? versionControlInfoDto.getRegistryId() : flowRegistry.getName();

            versionControlInfoDto.setState(VersionedFlowState.SYNC_FAILURE.name());
            versionControlInfoDto.setStateExplanation("Process Group has not yet been synchronized with the Flow Registry");
            final StandardVersionControlInformation versionControlInformation = StandardVersionControlInformation.Builder.fromDto(versionControlInfoDto)
                .registryName(registryName)
                .build();

            // pass empty map for the version control mapping because the VersionedComponentId has already been set on the components
            processGroup.setVersionControlInformation(versionControlInformation, Collections.emptyMap());
        }
    }

    private void addVariables(final Element processGroupElement, final ProcessGroup processGroup) {
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
    }

    private void addControllerServices(final Element processGroupElement, final ProcessGroup processGroup, final FlowController flowController, final FlowEncodingVersion encodingVersion) {
        final List<Element> serviceNodeList = getChildrenByTagName(processGroupElement, "controllerService");
        if (!serviceNodeList.isEmpty()) {
            final Map<ControllerServiceNode, Element> controllerServices = ControllerServiceLoader.loadControllerServices(serviceNodeList, flowController, processGroup, encryptor, encodingVersion);
            ControllerServiceLoader.enableControllerServices(controllerServices, flowController, encryptor, autoResumeState, encodingVersion);
        }
    }

    private void addProcessors(final Element processGroupElement, final ProcessGroup processGroup, final FlowController flowController, final FlowEncodingVersion encodingVersion) {
        final List<Element> processorNodeList = getChildrenByTagName(processGroupElement, "processor");
        for (final Element processorElement : processorNodeList) {
            final ProcessorDTO processorDTO = FlowFromDOMFactory.getProcessor(processorElement, encryptor, encodingVersion);

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

            final ProcessorNode procNode = flowController.getFlowManager().createProcessor(processorDTO.getType(), processorDTO.getId(), coordinate, false);
            procNode.setVersionedComponentId(processorDTO.getVersionedComponentId());
            processGroup.addProcessor(procNode);
            updateProcessor(procNode, processorDTO, processGroup, flowController);
        }
    }

    private void addInputPorts(final Element processGroupElement, final ProcessGroup processGroup, final FlowController flowController) {
        final FlowManager flowManager = flowController.getFlowManager();
        final List<Element> inputPortNodeList = getChildrenByTagName(processGroupElement, "inputPort");
        for (final Element inputPortElement : inputPortNodeList) {
            final PortDTO portDTO = FlowFromDOMFactory.getPort(inputPortElement);

            final Port port;
            if (processGroup.isRootGroup() || Boolean.TRUE.equals(portDTO.getAllowRemoteAccess())) {
                port = flowManager.createPublicInputPort(portDTO.getId(), portDTO.getName());
            } else {
                port = flowManager.createLocalInputPort(portDTO.getId(), portDTO.getName());
            }

            port.setVersionedComponentId(portDTO.getVersionedComponentId());
            port.setPosition(toPosition(portDTO.getPosition()));
            port.setComments(portDTO.getComments());
            port.setProcessGroup(processGroup);

            final Set<String> userControls = portDTO.getUserAccessControl();
            if (userControls != null && !userControls.isEmpty()) {
                if (!(port instanceof PublicPort)) {
                    throw new IllegalStateException("Attempting to add User Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((PublicPort) port).setUserAccessControl(userControls);
            }
            final Set<String> groupControls = portDTO.getGroupAccessControl();
            if (groupControls != null && !groupControls.isEmpty()) {
                if (!(port instanceof PublicPort)) {
                    throw new IllegalStateException("Attempting to add Group Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((PublicPort) port).setGroupAccessControl(groupControls);
            }

            processGroup.addInputPort(port);
            if (portDTO.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
            }

            final ScheduledState scheduledState = ScheduledState.valueOf(portDTO.getState());
            if (ScheduledState.RUNNING.equals(scheduledState)) {
                flowController.startConnectable(port);
            } else if (ScheduledState.DISABLED.equals(scheduledState)) {
                processGroup.disableInputPort(port);
            }
        }
    }

    private void addOutputPorts(final Element processGroupElement, final ProcessGroup processGroup, final FlowController flowController) {
        final FlowManager flowManager = flowController.getFlowManager();
        final List<Element> outputPortNodeList = getChildrenByTagName(processGroupElement, "outputPort");
        for (final Element outputPortElement : outputPortNodeList) {
            final PortDTO portDTO = FlowFromDOMFactory.getPort(outputPortElement);

            final Port port;
            if (processGroup.isRootGroup() || Boolean.TRUE.equals(portDTO.getAllowRemoteAccess())) {
                port = flowManager.createPublicOutputPort(portDTO.getId(), portDTO.getName());
            } else {
                port = flowManager.createLocalOutputPort(portDTO.getId(), portDTO.getName());
            }

            port.setVersionedComponentId(portDTO.getVersionedComponentId());
            port.setPosition(toPosition(portDTO.getPosition()));
            port.setComments(portDTO.getComments());
            port.setProcessGroup(processGroup);

            final Set<String> userControls = portDTO.getUserAccessControl();
            if (userControls != null && !userControls.isEmpty()) {
                if (!(port instanceof PublicPort)) {
                    throw new IllegalStateException("Attempting to add User Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((PublicPort) port).setUserAccessControl(userControls);
            }
            final Set<String> groupControls = portDTO.getGroupAccessControl();
            if (groupControls != null && !groupControls.isEmpty()) {
                if (!(port instanceof PublicPort)) {
                    throw new IllegalStateException("Attempting to add Group Access Controls to " + port.getIdentifier() + ", but it is not a RootGroupPort");
                }
                ((PublicPort) port).setGroupAccessControl(groupControls);
            }

            processGroup.addOutputPort(port);
            if (portDTO.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
            }

            final ScheduledState scheduledState = ScheduledState.valueOf(portDTO.getState());
            if (ScheduledState.RUNNING.equals(scheduledState)) {
                flowController.startConnectable(port);
            } else if (ScheduledState.DISABLED.equals(scheduledState)) {
                processGroup.disableOutputPort(port);
            }
        }
    }

    private void addFunnels(final Element processGroupElement, final ProcessGroup processGroup, final FlowController controller) {
        final List<Element> funnelNodeList = getChildrenByTagName(processGroupElement, "funnel");
        for (final Element funnelElement : funnelNodeList) {
            final FunnelDTO funnelDTO = FlowFromDOMFactory.getFunnel(funnelElement);
            final Funnel funnel = controller.getFlowManager().createFunnel(funnelDTO.getId());
            funnel.setVersionedComponentId(funnelDTO.getVersionedComponentId());
            funnel.setPosition(toPosition(funnelDTO.getPosition()));

            // Since this is called during startup, we want to add the funnel without enabling it
            // and then tell the controller to enable it. This way, if the controller is not fully
            // initialized, the starting of the funnel is delayed until the controller is ready.
            processGroup.addFunnel(funnel, false);
            controller.startConnectable(funnel);
        }
    }

    private void addLabels(final Element processGroupElement, final ProcessGroup processGroup, final FlowController controller) {
        final List<Element> labelNodeList = getChildrenByTagName(processGroupElement, "label");
        for (final Element labelElement : labelNodeList) {
            final LabelDTO labelDTO = FlowFromDOMFactory.getLabel(labelElement);
            final Label label = controller.getFlowManager().createLabel(labelDTO.getId(), labelDTO.getLabel());
            label.setVersionedComponentId(labelDTO.getVersionedComponentId());
            label.setStyle(labelDTO.getStyle());

            label.setPosition(toPosition(labelDTO.getPosition()));
            label.setSize(new Size(labelDTO.getWidth(), labelDTO.getHeight()));
            processGroup.addLabel(label);
        }
    }

    private void addRemoteProcessGroups(final Element processGroupElement, final ProcessGroup processGroup, final FlowController controller) {
        final List<Element> remoteProcessGroupNodeList = getChildrenByTagName(processGroupElement, "remoteProcessGroup");
        for (final Element remoteProcessGroupElement : remoteProcessGroupNodeList) {
            final RemoteProcessGroupDTO remoteGroupDto = FlowFromDOMFactory.getRemoteProcessGroup(remoteProcessGroupElement, encryptor);
            final RemoteProcessGroup remoteGroup = controller.getFlowManager().createRemoteProcessGroup(remoteGroupDto.getId(), remoteGroupDto.getTargetUris());
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
    }

    private void addConnections(final Element processGroupElement, final ProcessGroup processGroup, final FlowController controller) {
        final FlowManager flowManager = controller.getFlowManager();

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
    }

    private void addTemplates(final Element processGroupElement, final ProcessGroup processGroup) {
        final List<Element> templateNodeList = getChildrenByTagName(processGroupElement, "template");
        for (final Element templateNode : templateNodeList) {
            final TemplateDTO templateDTO = TemplateUtils.parseDto(templateNode);
            final Template template = new Template(templateDTO);
            processGroup.addTemplate(template);
        }
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
}
