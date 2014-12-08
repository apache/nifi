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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.file.FileUtils;
import org.apache.nifi.fingerprint.FingerprintException;
import org.apache.nifi.fingerprint.FingerprintFactory;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.nifi.encrypt.StringEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author unattributed
 */
public class StandardFlowSynchronizer implements FlowSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(StandardFlowSynchronizer.class);
    public static final URL FLOW_XSD_RESOURCE = StandardFlowSynchronizer.class.getResource("/FlowConfiguration.xsd");
    private final StringEncryptor encryptor;

    public StandardFlowSynchronizer(final StringEncryptor encryptor) {
        this.encryptor = encryptor;
    }

    public static boolean isEmpty(final DataFlow dataFlow, final StringEncryptor encryptor) {
        if (dataFlow == null || dataFlow.getFlow() == null || dataFlow.getFlow().length == 0) {
            return true;
        }

        final Document document = parseFlowBytes(dataFlow.getFlow());
        final Element rootElement = document.getDocumentElement();

        final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
        final ProcessGroupDTO rootGroupDto = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, encryptor);
        return isEmpty(rootGroupDto);
    }

    @Override
    public void sync(final FlowController controller, final DataFlow proposedFlow, final StringEncryptor encryptor) throws FlowSerializationException, UninheritableFlowException, FlowSynchronizationException {
        // get the controller's root group
        final ProcessGroup rootGroup = controller.getGroup(controller.getRootGroupId());

        // handle corner cases involving no proposed flow
        if (proposedFlow == null) {
            if (rootGroup.isEmpty()) {
                return;  // no sync to perform
            } else {
                throw new UninheritableFlowException("Proposed configuration is empty, but the controller contains a data flow.");
            }
        }

        // determine if the controller has been initialized
        final boolean initialized = controller.isInitialized();
        logger.debug("Synching FlowController with proposed flow: Controller is Initialized = {}", initialized);

        // serialize controller state to bytes
        final byte[] existingFlow;
        final boolean existingFlowEmpty;
        try {
            if (initialized) {
                existingFlow = toBytes(controller);
                existingFlowEmpty = controller.getGroup(controller.getRootGroupId()).isEmpty();
            } else {
                existingFlow = readFlowFromDisk();
                if (existingFlow == null || existingFlow.length == 0) {
                    existingFlowEmpty = true;
                } else {
                    final Document document = parseFlowBytes(existingFlow);
                    final Element rootElement = document.getDocumentElement();

                    logger.trace("Setting controller thread counts");
                    final Integer maxThreadCount = getInteger(rootElement, "maxThreadCount");
                    if (maxThreadCount == null) {
                        controller.setMaxTimerDrivenThreadCount(getInt(rootElement, "maxTimerDrivenThreadCount"));
                        controller.setMaxEventDrivenThreadCount(getInt(rootElement, "maxEventDrivenThreadCount"));
                    } else {
                        controller.setMaxTimerDrivenThreadCount(maxThreadCount * 2 / 3);
                        controller.setMaxEventDrivenThreadCount(maxThreadCount / 3);
                    }

                    logger.trace("Parsing process group from DOM");
                    final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
                    final ProcessGroupDTO rootGroupDto = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, encryptor);
                    existingFlowEmpty = isEmpty(rootGroupDto);
                    logger.debug("Existing Flow Empty = {}", existingFlowEmpty);
                }
            }
        } catch (final IOException e) {
            throw new FlowSerializationException(e);
        }

        logger.trace("Exporting templates from controller");
        final byte[] existingTemplates = controller.getTemplateManager().export();
        logger.trace("Exporting snippets from controller");
        final byte[] existingSnippets = controller.getSnippetManager().export();

        final DataFlow existingDataFlow = new StandardDataFlow(existingFlow, existingTemplates, existingSnippets);

        final boolean existingTemplatesEmpty = existingTemplates == null || existingTemplates.length == 0;

        // check that the proposed flow is inheritable by the controller
        try {
            if (!existingFlowEmpty) {
                logger.trace("Checking flow inheritability");
                final String problemInheriting = checkFlowInheritability(existingDataFlow, proposedFlow, controller);
                if (problemInheriting != null) {
                    throw new UninheritableFlowException("Proposed configuration is not inheritable by the flow controller because of flow differences: " + problemInheriting);
                }
            }
            if (!existingTemplatesEmpty) {
                logger.trace("Checking template inheritability");
                final String problemInheriting = checkTemplateInheritability(existingDataFlow, proposedFlow);
                if (problemInheriting != null) {
                    throw new UninheritableFlowException("Proposed configuration is not inheritable by the flow controller because of flow differences: " + problemInheriting);
                }
            }
        } catch (final FingerprintException fe) {
            throw new FlowSerializationException("Failed to generate flow fingerprints", fe);
        }

        // create document by parsing proposed flow bytes
        logger.trace("Parsing proposed flow bytes as DOM document");
        final Document configuration = parseFlowBytes(proposedFlow.getFlow());

        // attempt to sync controller with proposed flow
        try {
            if (configuration != null) {
                // get the root element
                final Element rootElement = (Element) configuration.getElementsByTagName("flowController").item(0);

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

                // if this controller isn't initialized or its emtpy, add the root group, otherwise update
                if (!initialized || existingFlowEmpty) {
                    logger.trace("Adding root process group");
                    addProcessGroup(controller, /* parent group */ null, rootGroupElement, encryptor);
                } else {
                    logger.trace("Updating root process group");
                    updateProcessGroup(controller, /* parent group */ null, rootGroupElement, encryptor);
                }
            }

            logger.trace("Synching templates");
            if ((existingTemplates == null || existingTemplates.length == 0) && proposedFlow.getTemplates() != null && proposedFlow.getTemplates().length > 0) {
                // need to load templates
                final TemplateManager templateManager = controller.getTemplateManager();
                final List<Template> proposedTemplateList = TemplateManager.parseBytes(proposedFlow.getTemplates());
                for (final Template template : proposedTemplateList) {
                    templateManager.addTemplate(template.getDetails());
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

            logger.debug("Finished synching flows");
        } catch (final Exception ex) {
            throw new FlowSynchronizationException(ex);
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

    private static Document parseFlowBytes(final byte[] flow) throws FlowSerializationException {
        // create document by parsing proposed flow bytes
        try {
            // create validating document builder
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            final Schema schema = schemaFactory.newSchema(FLOW_XSD_RESOURCE);
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setSchema(schema);
            final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

            // parse flow
            return (flow == null || flow.length == 0) ? null : docBuilder.parse(new ByteArrayInputStream(flow));
        } catch (final SAXException | ParserConfigurationException | IOException ex) {
            throw new FlowSerializationException(ex);
        }
    }

    private byte[] readFlowFromDisk() throws IOException {
        final Path flowPath = NiFiProperties.getInstance().getFlowConfigurationFile().toPath();
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

    private ProcessGroup updateProcessGroup(final FlowController controller, final ProcessGroup parentGroup, final Element processGroupElement, final StringEncryptor encryptor) throws ProcessorInstantiationException {

        // get the parent group ID
        final String parentId = (parentGroup == null) ? null : parentGroup.getIdentifier();

        // get the process group
        final ProcessGroupDTO processGroupDto = FlowFromDOMFactory.getProcessGroup(parentId, processGroupElement, encryptor);

        // update the process group
        if (parentId == null) {

            /*
             * Labels are not included in the "inherit flow" algorithm, so we cannot
             * blindly update them because they may not exist in the current flow.
             * Therefore, we first remove all labels, and then let the updating
             * process add labels defined in the new flow.
             */
            final ProcessGroup root = controller.getGroup(controller.getRootGroupId());
            for (final Label label : root.findAllLabels()) {
                label.getProcessGroup().removeLabel(label);
            }
        }

        // update the process group
        controller.updateProcessGroup(processGroupDto);

        // get the real process group and ID
        final ProcessGroup processGroup = controller.getGroup(processGroupDto.getId());

        // processors & ports cannot be updated - they must be the same. Except for the scheduled state.
        final List<Element> processorNodeList = getChildrenByTagName(processGroupElement, "processor");
        for (final Element processorElement : processorNodeList) {
            final ProcessorDTO dto = FlowFromDOMFactory.getProcessor(processorElement, encryptor);
            final ProcessorNode procNode = processGroup.getProcessor(dto.getId());

            if (!procNode.getScheduledState().name().equals(dto.getState())) {
                try {
                    switch (ScheduledState.valueOf(dto.getState())) {
                        case DISABLED:
                            // switch processor do disabled. This means we have to stop it (if it's already stopped, this method does nothing),
                            // and then we have to disable it.
                            procNode.getProcessGroup().stopProcessor(procNode);
                            procNode.getProcessGroup().disableProcessor(procNode);
                            break;
                        case RUNNING:
                            // we want to run now. Make sure processor is not disabled and then start it.
                            procNode.getProcessGroup().enableProcessor(procNode);
                            procNode.getProcessGroup().startProcessor(procNode);
                            break;
                        case STOPPED:
                            if (procNode.getScheduledState() == ScheduledState.DISABLED) {
                                procNode.getProcessGroup().enableProcessor(procNode);
                            } else if (procNode.getScheduledState() == ScheduledState.RUNNING) {
                                procNode.getProcessGroup().stopProcessor(procNode);
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

            if (!port.getScheduledState().name().equals(dto.getState())) {
                switch (ScheduledState.valueOf(dto.getState())) {
                    case DISABLED:
                        // switch processor do disabled. This means we have to stop it (if it's already stopped, this method does nothing),
                        // and then we have to disable it.
                        port.getProcessGroup().stopInputPort(port);
                        port.getProcessGroup().disableInputPort(port);
                        break;
                    case RUNNING:
                        // we want to run now. Make sure processor is not disabled and then start it.
                        port.getProcessGroup().enableInputPort(port);
                        port.getProcessGroup().startInputPort(port);
                        break;
                    case STOPPED:
                        if (port.getScheduledState() == ScheduledState.DISABLED) {
                            port.getProcessGroup().enableInputPort(port);
                        } else if (port.getScheduledState() == ScheduledState.RUNNING) {
                            port.getProcessGroup().stopInputPort(port);
                        }
                        break;
                }
            }
        }

        final List<Element> outputPortList = getChildrenByTagName(processGroupElement, "outputPort");
        for (final Element portElement : outputPortList) {
            final PortDTO dto = FlowFromDOMFactory.getPort(portElement);
            final Port port = processGroup.getOutputPort(dto.getId());

            if (!port.getScheduledState().name().equals(dto.getState())) {
                switch (ScheduledState.valueOf(dto.getState())) {
                    case DISABLED:
                        // switch processor do disabled. This means we have to stop it (if it's already stopped, this method does nothing),
                        // and then we have to disable it.
                        port.getProcessGroup().stopOutputPort(port);
                        port.getProcessGroup().disableOutputPort(port);
                        break;
                    case RUNNING:
                        // we want to run now. Make sure processor is not disabled and then start it.
                        port.getProcessGroup().enableOutputPort(port);
                        port.getProcessGroup().startOutputPort(port);
                        break;
                    case STOPPED:
                        if (port.getScheduledState() == ScheduledState.DISABLED) {
                            port.getProcessGroup().enableOutputPort(port);
                        } else if (port.getScheduledState() == ScheduledState.RUNNING) {
                            port.getProcessGroup().stopOutputPort(port);
                        }
                        break;
                }
            }
        }

        // add labels
        final List<Element> labelNodeList = getChildrenByTagName(processGroupElement, "label");
        for (final Element labelElement : labelNodeList) {
            final LabelDTO labelDTO = FlowFromDOMFactory.getLabel(labelElement);
            final Label label = controller.createLabel(labelDTO.getId(), labelDTO.getLabel());
            label.setStyle(labelDTO.getStyle());
            label.setPosition(new Position(labelDTO.getPosition().getX(), labelDTO.getPosition().getY()));
            if (labelDTO.getWidth() != null && labelDTO.getHeight() != null) {
                label.setSize(new Size(labelDTO.getWidth(), labelDTO.getHeight()));
            }

            processGroup.addLabel(label);
        }

        // update nested process groups (recursively)
        final List<Element> nestedProcessGroupNodeList = getChildrenByTagName(processGroupElement, "processGroup");
        for (final Element nestedProcessGroupElement : nestedProcessGroupNodeList) {
            updateProcessGroup(controller, processGroup, nestedProcessGroupElement, encryptor);
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
                        newPrioritizers.add(controller.createPrioritizer(className));
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

        return processGroup;
    }

    private Position toPosition(final PositionDTO dto) {
        return new Position(dto.getX(), dto.getY());
    }

    private void updateProcessor(final ProcessorNode procNode, final ProcessorDTO processorDTO, final ProcessGroup processGroup, final FlowController controller) throws ProcessorInstantiationException {
        final ProcessorConfigDTO config = processorDTO.getConfig();
        procNode.setPosition(toPosition(processorDTO.getPosition()));
        procNode.setName(processorDTO.getName());
        procNode.setStyle(processorDTO.getStyle());
        procNode.setProcessGroup(processGroup);
        procNode.setComments(config.getComments());
        procNode.setLossTolerant(config.isLossTolerant());
        procNode.setPenalizationPeriod(config.getPenaltyDuration());
        procNode.setYieldPeriod(config.getYieldDuration());
        procNode.setBulletinLevel(LogLevel.valueOf(config.getBulletinLevel()));

        if (config.getSchedulingStrategy() != null) {
            procNode.setSchedulingStrategy(SchedulingStrategy.valueOf(config.getSchedulingStrategy()));
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

        for (final Map.Entry<String, String> entry : config.getProperties().entrySet()) {
            if (entry.getValue() == null) {
                procNode.removeProperty(entry.getKey());
            } else {
                procNode.setProperty(entry.getKey(), entry.getValue());
            }
        }

        final ScheduledState scheduledState = ScheduledState.valueOf(processorDTO.getState());
        if (ScheduledState.RUNNING.equals(scheduledState)) {
            controller.startProcessor(processGroup.getIdentifier(), procNode.getIdentifier());
        } else if (ScheduledState.DISABLED.equals(scheduledState)) {
            processGroup.disableProcessor(procNode);
        }
    }

    private ProcessGroup addProcessGroup(final FlowController controller, final ProcessGroup parentGroup, final Element processGroupElement, final StringEncryptor encryptor) throws ProcessorInstantiationException {
        // get the parent group ID
        final String parentId = (parentGroup == null) ? null : parentGroup.getIdentifier();

        // add the process group
        final ProcessGroupDTO processGroupDTO = FlowFromDOMFactory.getProcessGroup(parentId, processGroupElement, encryptor);
        final ProcessGroup processGroup = controller.createProcessGroup(processGroupDTO.getId());
        processGroup.setComments(processGroupDTO.getComments());
        processGroup.setPosition(toPosition(processGroupDTO.getPosition()));
        processGroup.setName(processGroupDTO.getName());
        processGroup.setParent(parentGroup);
        if (parentGroup == null) {
            controller.setRootGroup(processGroup);
        } else {
            parentGroup.addProcessGroup(processGroup);
        }

        // add processors
        final List<Element> processorNodeList = getChildrenByTagName(processGroupElement, "processor");
        for (final Element processorElement : processorNodeList) {
            final ProcessorDTO processorDTO = FlowFromDOMFactory.getProcessor(processorElement, encryptor);
            final ProcessorNode procNode = controller.createProcessor(processorDTO.getType(), processorDTO.getId());
            processGroup.addProcessor(procNode);
            updateProcessor(procNode, processorDTO, processGroup, controller);
        }

        // add input ports
        final List<Element> inputPortNodeList = getChildrenByTagName(processGroupElement, "inputPort");
        for (final Element inputPortElement : inputPortNodeList) {
            final PortDTO portDTO = FlowFromDOMFactory.getPort(inputPortElement);

            final Port port;
            if (processGroup.isRootGroup()) {
                port = controller.createRemoteInputPort(portDTO.getId(), portDTO.getName());
            } else {
                port = controller.createLocalInputPort(portDTO.getId(), portDTO.getName());
            }

            port.setPosition(toPosition(portDTO.getPosition()));
            port.setComments(portDTO.getComments());
            port.setProcessGroup(processGroup);

            final Set<String> userControls = portDTO.getUserAccessControl();
            if (userControls != null && !userControls.isEmpty()) {
                if (!(port instanceof RootGroupPort)) {
                    throw new IllegalStateException("Attempting to add User Access Controls to " + port + ", but it is not a RootGroupPort");
                }
                ((RootGroupPort) port).setUserAccessControl(userControls);
            }
            final Set<String> groupControls = portDTO.getGroupAccessControl();
            if (groupControls != null && !groupControls.isEmpty()) {
                if (!(port instanceof RootGroupPort)) {
                    throw new IllegalStateException("Attempting to add Group Access Controls to " + port + ", but it is not a RootGroupPort");
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
                port = controller.createRemoteOutputPort(portDTO.getId(), portDTO.getName());
            } else {
                port = controller.createLocalOutputPort(portDTO.getId(), portDTO.getName());
            }
            port.setPosition(toPosition(portDTO.getPosition()));
            port.setComments(portDTO.getComments());
            port.setProcessGroup(processGroup);

            final Set<String> userControls = portDTO.getUserAccessControl();
            if (userControls != null && !userControls.isEmpty()) {
                if (!(port instanceof RootGroupPort)) {
                    throw new IllegalStateException("Attempting to add User Access Controls to " + port + ", but it is not a RootGroupPort");
                }
                ((RootGroupPort) port).setUserAccessControl(userControls);
            }
            final Set<String> groupControls = portDTO.getGroupAccessControl();
            if (groupControls != null && !groupControls.isEmpty()) {
                if (!(port instanceof RootGroupPort)) {
                    throw new IllegalStateException("Attempting to add Group Access Controls to " + port + ", but it is not a RootGroupPort");
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
            final Funnel funnel = controller.createFunnel(funnelDTO.getId());
            funnel.setPosition(toPosition(funnelDTO.getPosition()));
            processGroup.addFunnel(funnel);
            controller.startConnectable(funnel);
        }

        // add labels
        final List<Element> labelNodeList = getChildrenByTagName(processGroupElement, "label");
        for (final Element labelElement : labelNodeList) {
            final LabelDTO labelDTO = FlowFromDOMFactory.getLabel(labelElement);
            final Label label = controller.createLabel(labelDTO.getId(), labelDTO.getLabel());
            label.setStyle(labelDTO.getStyle());

            label.setPosition(toPosition(labelDTO.getPosition()));
            label.setSize(new Size(labelDTO.getWidth(), labelDTO.getHeight()));
            processGroup.addLabel(label);
        }

        // add nested process groups (recursively)
        final List<Element> nestedProcessGroupNodeList = getChildrenByTagName(processGroupElement, "processGroup");
        for (final Element nestedProcessGroupElement : nestedProcessGroupNodeList) {
            addProcessGroup(controller, processGroup, nestedProcessGroupElement, encryptor);
        }

        // add remote process group
        final List<Element> remoteProcessGroupNodeList = getChildrenByTagName(processGroupElement, "remoteProcessGroup");
        for (final Element remoteProcessGroupElement : remoteProcessGroupNodeList) {
            final RemoteProcessGroupDTO remoteGroupDto = FlowFromDOMFactory.getRemoteProcessGroup(remoteProcessGroupElement);
            final RemoteProcessGroup remoteGroup = controller.createRemoteProcessGroup(remoteGroupDto.getId(), remoteGroupDto.getTargetUri());
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

            final Set<RemoteProcessGroupPortDescriptor> inputPorts = new HashSet<>();
            for (final Element portElement : getChildrenByTagName(remoteProcessGroupElement, "inputPort")) {
                inputPorts.add(FlowFromDOMFactory.getRemoteProcessGroupPort(portElement));
            }
            remoteGroup.setInputPorts(inputPorts);

            final Set<RemoteProcessGroupPortDescriptor> outputPorts = new HashSet<>();
            for (final Element portElement : getChildrenByTagName(remoteProcessGroupElement, "outputPort")) {
                outputPorts.add(FlowFromDOMFactory.getRemoteProcessGroupPort(portElement));
            }
            remoteGroup.setOutputPorts(outputPorts);
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
                final ProcessGroup sourceGroup = controller.getGroup(sourceDto.getGroupId());
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
                final ProcessGroup destinationGroup = controller.getGroup(destinationDto.getGroupId());
                if (destinationGroup == null) {
                    throw new RuntimeException("Found Invalid ProcessGroup ID for Destination: " + dto.getDestination().getGroupId());
                }

                destination = destinationGroup.getConnectable(destinationDto.getId());
            }
            if (destination == null) {
                throw new RuntimeException("Found Invalid Connectable ID for Destination: " + dto.getDestination().getId());
            }

            final Connection connection = controller.createConnection(dto.getId(), dto.getName(), source, destination, dto.getSelectedRelationships());
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
                        newPrioritizers.add(controller.createPrioritizer(className));
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

            processGroup.addConnection(connection);
        }

        return processGroup;
    }

    /**
     * Returns true if the given controller can inherit the proposed flow
     * without orphaning flow files.
     *
     * @param existingFlow
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
        final FingerprintFactory fingerprintFactory = new FingerprintFactory(encryptor);
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

        final boolean inheritable = existingFlowFingerprintBeforeHash.equals(proposedFlowFingerprintBeforeHash);
        if (!inheritable) {
            return findFirstDiscrepancy(existingFlowFingerprintBeforeHash, proposedFlowFingerprintBeforeHash, "Flows");
        }

        return null;
    }

    /**
     * Returns true if the given controller can inherit the proposed flow
     * without orphaning flow files.
     *
     * @param existingFlow
     * @param proposedFlow the flow to inherit
     *
     * @return null if the controller can inherit the specified flow, an
     * explanation of why it cannot be inherited otherwise
     *
     * @throws FingerprintException if flow fingerprints could not be generated
     */
    public String checkTemplateInheritability(final DataFlow existingFlow, final DataFlow proposedFlow) throws FingerprintException {
        if (existingFlow == null) {
            return null;  // no existing flow, so equivalent to proposed flow
        }

        // check if the Flow is inheritable
        final FingerprintFactory fingerprintFactory = new FingerprintFactory(encryptor);
        // check if the Templates are inheritable
        final byte[] existingTemplateBytes = existingFlow.getTemplates();
        if (existingTemplateBytes == null || existingTemplateBytes.length == 0) {
            return null;
        }

        final List<Template> existingTemplates = TemplateManager.parseBytes(existingTemplateBytes);
        final String existingTemplateFingerprint = fingerprintFactory.createFingerprint(existingTemplates);
        if (existingTemplateFingerprint.trim().isEmpty()) {
            return null;
        }

        final byte[] proposedTemplateBytes = proposedFlow.getTemplates();
        if (proposedTemplateBytes == null || proposedTemplateBytes.length == 0) {
            return "Proposed Flow does not contain any Templates but Current Flow does";
        }

        final List<Template> proposedTemplates = TemplateManager.parseBytes(proposedTemplateBytes);
        final String proposedTemplateFingerprint = fingerprintFactory.createFingerprint(proposedTemplates);
        if (proposedTemplateFingerprint.trim().isEmpty()) {
            return "Proposed Flow does not contain any Templates but Current Flow does";
        }

        try {
            final String existingTemplateMd5 = fingerprintFactory.md5Hash(existingTemplateFingerprint);
            final String proposedTemplateMd5 = fingerprintFactory.md5Hash(proposedTemplateFingerprint);

            if (!existingTemplateMd5.equals(proposedTemplateMd5)) {
                return findFirstDiscrepancy(existingTemplateFingerprint, proposedTemplateFingerprint, "Templates");
            }
        } catch (final NoSuchAlgorithmException e) {
            throw new FingerprintException(e);
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
}
