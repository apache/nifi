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
package org.apache.nifi.controller.serialization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.encrypt.EncryptionException;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.remote.StandardRemoteProcessGroupPortDescriptor;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.DomUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class FlowFromDOMFactory {
    private static final Logger logger = LoggerFactory.getLogger(FlowFromDOMFactory.class);

    public static BundleDTO getBundle(final Element bundleElement) {
        if (bundleElement == null) {
            return null;
        }

        final Element groupElement = DomUtils.getChild(bundleElement, "group");
        final Element artifactElement = DomUtils.getChild(bundleElement, "artifact");
        final Element versionElement = DomUtils.getChild(bundleElement, "version");

        return new BundleDTO(groupElement.getTextContent(), artifactElement.getTextContent(), versionElement.getTextContent());
    }

    public static PositionDTO getPosition(final Element positionElement) {
        if (positionElement == null) {
            throw new IllegalArgumentException("Invalid Flow: Found no 'position' element");
        }
        return new PositionDTO(Double.parseDouble(positionElement.getAttribute("x")), Double.parseDouble(positionElement.getAttribute("y")));
    }

    public static Size getSize(final Element sizeElement) {
        if (sizeElement == null) {
            throw new IllegalArgumentException("Invalid Flow: Found no 'size' element");
        }

        return new Size(Double.parseDouble(sizeElement.getAttribute("width")), Double.parseDouble(sizeElement.getAttribute("height")));
    }

    public static Map<String, String> getStyle(final Element stylesElement) {
        final Map<String, String> styles = new HashMap<>();
        if (stylesElement == null) {
            return styles;
        }

        for (final Element styleElement : getChildrenByTagName(stylesElement, "style")) {
            final String styleName = styleElement.getAttribute("name");
            final String styleValue = styleElement.getTextContent();
            styles.put(styleName, styleValue);
        }

        return styles;
    }

    public static ControllerServiceDTO getControllerService(final Element element, final StringEncryptor encryptor) {
        final ControllerServiceDTO dto = new ControllerServiceDTO();

        dto.setId(getString(element, "id"));
        dto.setName(getString(element, "name"));
        dto.setComments(getString(element, "comment"));
        dto.setType(getString(element, "class"));
        dto.setBundle(getBundle(DomUtils.getChild(element, "bundle")));

        final boolean enabled = getBoolean(element, "enabled");
        dto.setState(enabled ? ControllerServiceState.ENABLED.name() : ControllerServiceState.DISABLED.name());

        dto.setProperties(getProperties(element, encryptor));
        dto.setAnnotationData(getString(element, "annotationData"));

        return dto;
    }

    public static ReportingTaskDTO getReportingTask(final Element element, final StringEncryptor encryptor) {
        final ReportingTaskDTO dto = new ReportingTaskDTO();

        dto.setId(getString(element, "id"));
        dto.setName(getString(element, "name"));
        dto.setComments(getString(element, "comment"));
        dto.setType(getString(element, "class"));
        dto.setBundle(getBundle(DomUtils.getChild(element, "bundle")));
        dto.setSchedulingPeriod(getString(element, "schedulingPeriod"));
        dto.setState(getString(element, "scheduledState"));
        dto.setSchedulingStrategy(getString(element, "schedulingStrategy"));

        dto.setProperties(getProperties(element, encryptor));
        dto.setAnnotationData(getString(element, "annotationData"));

        return dto;
    }

    public static ProcessGroupDTO getProcessGroup(final String parentId, final Element element, final StringEncryptor encryptor, final FlowEncodingVersion encodingVersion) {
        final ProcessGroupDTO dto = new ProcessGroupDTO();
        final String groupId = getString(element, "id");
        dto.setId(groupId);
        dto.setParentGroupId(parentId);
        dto.setName(getString(element, "name"));
        dto.setPosition(getPosition(DomUtils.getChild(element, "position")));
        dto.setComments(getString(element, "comment"));

        final Map<String, String> variables = new HashMap<>();
        final NodeList variableList = DomUtils.getChildNodesByTagName(element, "variable");
        for (int i = 0; i < variableList.getLength(); i++) {
            final Element variableElement = (Element) variableList.item(i);
            final String name = variableElement.getAttribute("name");
            final String value = variableElement.getAttribute("value");
            variables.put(name, value);
        }
        dto.setVariables(variables);

        final Set<ProcessorDTO> processors = new HashSet<>();
        final Set<ConnectionDTO> connections = new HashSet<>();
        final Set<FunnelDTO> funnels = new HashSet<>();
        final Set<PortDTO> inputPorts = new HashSet<>();
        final Set<PortDTO> outputPorts = new HashSet<>();
        final Set<LabelDTO> labels = new HashSet<>();
        final Set<ProcessGroupDTO> processGroups = new HashSet<>();
        final Set<RemoteProcessGroupDTO> remoteProcessGroups = new HashSet<>();

        NodeList nodeList = DomUtils.getChildNodesByTagName(element, "processor");
        for (int i = 0; i < nodeList.getLength(); i++) {
            processors.add(getProcessor((Element) nodeList.item(i), encryptor));
        }

        nodeList = DomUtils.getChildNodesByTagName(element, "funnel");
        for (int i = 0; i < nodeList.getLength(); i++) {
            funnels.add(getFunnel((Element) nodeList.item(i)));
        }

        nodeList = DomUtils.getChildNodesByTagName(element, "inputPort");
        for (int i = 0; i < nodeList.getLength(); i++) {
            inputPorts.add(getPort((Element) nodeList.item(i)));
        }

        nodeList = DomUtils.getChildNodesByTagName(element, "outputPort");
        for (int i = 0; i < nodeList.getLength(); i++) {
            outputPorts.add(getPort((Element) nodeList.item(i)));
        }

        nodeList = DomUtils.getChildNodesByTagName(element, "label");
        for (int i = 0; i < nodeList.getLength(); i++) {
            labels.add(getLabel((Element) nodeList.item(i)));
        }

        nodeList = DomUtils.getChildNodesByTagName(element, "processGroup");
        for (int i = 0; i < nodeList.getLength(); i++) {
            processGroups.add(getProcessGroup(groupId, (Element) nodeList.item(i), encryptor, encodingVersion));
        }

        nodeList = DomUtils.getChildNodesByTagName(element, "remoteProcessGroup");
        for (int i = 0; i < nodeList.getLength(); i++) {
            remoteProcessGroups.add(getRemoteProcessGroup((Element) nodeList.item(i), encryptor));
        }

        nodeList = DomUtils.getChildNodesByTagName(element, "connection");
        for (int i = 0; i < nodeList.getLength(); i++) {
            connections.add(getConnection((Element) nodeList.item(i)));
        }

        final FlowSnippetDTO groupContents = new FlowSnippetDTO();
        groupContents.setConnections(connections);
        groupContents.setFunnels(funnels);
        groupContents.setInputPorts(inputPorts);
        groupContents.setLabels(labels);
        groupContents.setOutputPorts(outputPorts);
        groupContents.setProcessGroups(processGroups);
        groupContents.setProcessors(processors);
        groupContents.setRemoteProcessGroups(remoteProcessGroups);

        dto.setContents(groupContents);
        return dto;
    }

    public static ConnectionDTO getConnection(final Element element) {
        final ConnectionDTO dto = new ConnectionDTO();
        dto.setId(getString(element, "id"));
        dto.setName(getString(element, "name"));
        dto.setLabelIndex(getOptionalInt(element, "labelIndex"));
        dto.setzIndex(getOptionalLong(element, "zIndex"));

        final List<PositionDTO> bends = new ArrayList<>();
        final Element bendPointsElement = DomUtils.getChild(element, "bendPoints");
        if (bendPointsElement != null) {
            for (final Element bendPointElement : getChildrenByTagName(bendPointsElement, "bendPoint")) {
                final PositionDTO bend = getPosition(bendPointElement);
                bends.add(bend);
            }
        }
        dto.setBends(bends);

        final ConnectableDTO sourceConnectable = new ConnectableDTO();
        dto.setSource(sourceConnectable);
        sourceConnectable.setId(getString(element, "sourceId"));
        sourceConnectable.setGroupId(getString(element, "sourceGroupId"));
        sourceConnectable.setType(getString(element, "sourceType"));

        final ConnectableDTO destConnectable = new ConnectableDTO();
        dto.setDestination(destConnectable);
        destConnectable.setId(getString(element, "destinationId"));
        destConnectable.setGroupId(getString(element, "destinationGroupId"));
        destConnectable.setType(getString(element, "destinationType"));

        final Set<String> relationships = new HashSet<>();
        final List<Element> relationshipNodeList = getChildrenByTagName(element, "relationship");
        for (final Element relationshipElem : relationshipNodeList) {
            relationships.add(relationshipElem.getTextContent());
        }
        dto.setSelectedRelationships(relationships);

        dto.setBackPressureObjectThreshold(getLong(element, "maxWorkQueueSize"));

        final String maxDataSize = getString(element, "maxWorkQueueDataSize");
        if (maxDataSize != null && !maxDataSize.trim().isEmpty()) {
            dto.setBackPressureDataSizeThreshold(maxDataSize);
        }

        String expiration = getString(element, "flowFileExpiration");
        if (expiration == null) {
            expiration = "0 sec";
        }
        dto.setFlowFileExpiration(expiration);

        final List<String> prioritizerClasses = new ArrayList<>();
        final List<Element> prioritizerNodeList = getChildrenByTagName(element, "queuePrioritizerClass");
        for (final Element prioritizerElement : prioritizerNodeList) {
            prioritizerClasses.add(prioritizerElement.getTextContent().trim());
        }
        dto.setPrioritizers(prioritizerClasses);

        return dto;
    }

    public static RemoteProcessGroupDTO getRemoteProcessGroup(final Element element, final StringEncryptor encryptor) {
        final RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();
        dto.setId(getString(element, "id"));
        dto.setName(getString(element, "name"));
        dto.setTargetUri(getString(element, "url"));
        dto.setTargetUris(getString(element, "urls"));
        dto.setTransmitting(getBoolean(element, "transmitting"));
        dto.setPosition(getPosition(DomUtils.getChild(element, "position")));
        dto.setCommunicationsTimeout(getString(element, "timeout"));
        dto.setComments(getString(element, "comment"));
        dto.setYieldDuration(getString(element, "yieldPeriod"));
        dto.setTransportProtocol(getString(element, "transportProtocol"));
        dto.setProxyHost(getString(element, "proxyHost"));
        dto.setProxyPort(getOptionalInt(element, "proxyPort"));
        dto.setProxyUser(getString(element, "proxyUser"));
        dto.setLocalNetworkInterface(getString(element, "networkInterface"));

        final String rawPassword = getString(element, "proxyPassword");
        final String proxyPassword = encryptor == null ? rawPassword : decrypt(rawPassword, encryptor);
        dto.setProxyPassword(proxyPassword);

        return dto;
    }

    public static LabelDTO getLabel(final Element element) {
        final LabelDTO dto = new LabelDTO();
        dto.setId(getString(element, "id"));
        dto.setLabel(getString(element, "value"));
        dto.setPosition(getPosition(DomUtils.getChild(element, "position")));
        final Size size = getSize(DomUtils.getChild(element, "size"));
        dto.setWidth(size.getWidth());
        dto.setHeight(size.getHeight());
        dto.setStyle(getStyle(DomUtils.getChild(element, "styles")));

        return dto;
    }

    public static FunnelDTO getFunnel(final Element element) {
        final FunnelDTO dto = new FunnelDTO();
        dto.setId(getString(element, "id"));
        dto.setPosition(getPosition(DomUtils.getChild(element, "position")));

        return dto;
    }

    public static PortDTO getPort(final Element element) {
        final PortDTO portDTO = new PortDTO();
        portDTO.setId(getString(element, "id"));
        portDTO.setPosition(getPosition(DomUtils.getChild(element, "position")));
        portDTO.setName(getString(element, "name"));
        portDTO.setComments(getString(element, "comments"));
        final ScheduledState scheduledState = getScheduledState(element);
        portDTO.setState(scheduledState.toString());

        final List<Element> maxTasksElements = getChildrenByTagName(element, "maxConcurrentTasks");
        if (!maxTasksElements.isEmpty()) {
            portDTO.setConcurrentlySchedulableTaskCount(Integer.parseInt(maxTasksElements.get(0).getTextContent()));
        }

        final List<Element> userAccessControls = getChildrenByTagName(element, "userAccessControl");
        if (userAccessControls != null && !userAccessControls.isEmpty()) {
            final Set<String> users = new HashSet<>();
            portDTO.setUserAccessControl(users);
            for (final Element userElement : userAccessControls) {
                users.add(userElement.getTextContent());
            }
        }

        final List<Element> groupAccessControls = getChildrenByTagName(element, "groupAccessControl");
        if (groupAccessControls != null && !groupAccessControls.isEmpty()) {
            final Set<String> groups = new HashSet<>();
            portDTO.setGroupAccessControl(groups);
            for (final Element groupElement : groupAccessControls) {
                groups.add(groupElement.getTextContent());
            }
        }

        return portDTO;
    }

    public static RemoteProcessGroupPortDescriptor getRemoteProcessGroupPort(final Element element) {
        final StandardRemoteProcessGroupPortDescriptor descriptor = new StandardRemoteProcessGroupPortDescriptor();

        // What we have serialized is the ID of the Remote Process Group, followed by a dash ('-'), followed by
        // the actual ID of the port; we want to get rid of the remote process group id.
        String id = getString(element, "id");
        if (id.length() > 37) {
            id = id.substring(37);
        }

        descriptor.setId(id);
        descriptor.setName(getString(element, "name"));
        descriptor.setComments(getString(element, "comments"));
        descriptor.setConcurrentlySchedulableTaskCount(getInt(element, "maxConcurrentTasks"));
        descriptor.setUseCompression(getBoolean(element, "useCompression"));
        descriptor.setBatchCount(getOptionalInt(element, "batchCount"));
        descriptor.setBatchSize(getString(element, "batchSize"));
        descriptor.setBatchDuration(getString(element, "batchDuration"));
        descriptor.setTransmitting("RUNNING".equalsIgnoreCase(getString(element, "scheduledState")));

        return descriptor;
    }

    public static ProcessorDTO getProcessor(final Element element, final StringEncryptor encryptor) {
        final ProcessorDTO dto = new ProcessorDTO();

        dto.setId(getString(element, "id"));
        dto.setName(getString(element, "name"));
        dto.setType(getString(element, "class"));
        dto.setBundle(getBundle(DomUtils.getChild(element, "bundle")));
        dto.setPosition(getPosition(DomUtils.getChild(element, "position")));
        dto.setStyle(getStyle(DomUtils.getChild(element, "styles")));

        final ProcessorConfigDTO configDto = new ProcessorConfigDTO();
        dto.setConfig(configDto);
        configDto.setComments(getString(element, "comment"));
        configDto.setConcurrentlySchedulableTaskCount(getInt(element, "maxConcurrentTasks"));
        final String schedulingPeriod = getString(element, "schedulingPeriod");
        configDto.setSchedulingPeriod(schedulingPeriod);
        configDto.setPenaltyDuration(getString(element, "penalizationPeriod"));
        configDto.setYieldDuration(getString(element, "yieldPeriod"));
        configDto.setBulletinLevel(getString(element, "bulletinLevel"));
        configDto.setLossTolerant(getBoolean(element, "lossTolerant"));
        final ScheduledState scheduledState = getScheduledState(element);
        dto.setState(scheduledState.toString());

        // handle scheduling strategy
        final String schedulingStrategyName = getString(element, "schedulingStrategy");
        if (schedulingStrategyName == null || schedulingStrategyName.trim().isEmpty()) {
            configDto.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN.name());
        } else {
            configDto.setSchedulingStrategy(schedulingStrategyName.trim());
        }

        // handle execution node
        final String executionNode = getString(element, "executionNode");
        if (executionNode == null || executionNode.trim().isEmpty()) {
            configDto.setExecutionNode(ExecutionNode.ALL.name());
        } else {
            configDto.setExecutionNode(executionNode.trim());
        }

        final Long runDurationNanos = getOptionalLong(element, "runDurationNanos");
        if (runDurationNanos != null) {
            configDto.setRunDurationMillis(TimeUnit.NANOSECONDS.toMillis(runDurationNanos));
        }

        configDto.setProperties(getProperties(element, encryptor));
        configDto.setAnnotationData(getString(element, "annotationData"));

        final Set<String> autoTerminatedRelationships = new HashSet<>();
        final List<Element> autoTerminateList = getChildrenByTagName(element, "autoTerminatedRelationship");
        for (final Element autoTerminateElement : autoTerminateList) {
            autoTerminatedRelationships.add(autoTerminateElement.getTextContent());
        }
        configDto.setAutoTerminatedRelationships(autoTerminatedRelationships);

        return dto;
    }

    private static LinkedHashMap<String, String> getProperties(final Element element, final StringEncryptor encryptor) {
        final LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        final List<Element> propertyNodeList = getChildrenByTagName(element, "property");
        for (final Element propertyElement : propertyNodeList) {
            final String name = getString(propertyElement, "name");

            final String rawPropertyValue = getString(propertyElement, "value");
            final String value = encryptor == null ? rawPropertyValue : decrypt(rawPropertyValue, encryptor);
            properties.put(name, value);
        }
        return properties;
    }

    private static String getString(final Element element, final String childElementName) {
        final List<Element> nodeList = getChildrenByTagName(element, childElementName);
        if (nodeList == null || nodeList.isEmpty()) {
            return null;
        }
        final Element childElement = nodeList.get(0);
        return childElement.getTextContent();
    }

    private static Integer getOptionalInt(final Element element, final String childElementName) {
        final List<Element> nodeList = getChildrenByTagName(element, childElementName);
        if (nodeList == null || nodeList.isEmpty()) {
            return null;
        }
        final Element childElement = nodeList.get(0);
        final String val = childElement.getTextContent();
        if (val == null) {
            return null;
        }
        return Integer.parseInt(val);
    }

    private static Long getOptionalLong(final Element element, final String childElementName) {
        final List<Element> nodeList = getChildrenByTagName(element, childElementName);
        if (nodeList == null || nodeList.isEmpty()) {
            return null;
        }
        final Element childElement = nodeList.get(0);
        final String val = childElement.getTextContent();
        if (val == null) {
            return null;
        }
        return Long.parseLong(val);
    }

    private static int getInt(final Element element, final String childElementName) {
        return Integer.parseInt(getString(element, childElementName));
    }

    private static long getLong(final Element element, final String childElementName) {
        return Long.parseLong(getString(element, childElementName));
    }

    private static boolean getBoolean(final Element element, final String childElementName) {
        return Boolean.parseBoolean(getString(element, childElementName));
    }

    private static ScheduledState getScheduledState(final Element element) {
        return ScheduledState.valueOf(getString(element, "scheduledState"));
    }

    private static List<Element> getChildrenByTagName(final Element element, final String childElementName) {
        return DomUtils.getChildElementsByTagName(element, childElementName);
    }

    private static String decrypt(final String value, final StringEncryptor encryptor) {
        if (value != null && value.startsWith(FlowSerializer.ENC_PREFIX) && value.endsWith(FlowSerializer.ENC_SUFFIX)) {
            try {
                return encryptor.decrypt(value.substring(FlowSerializer.ENC_PREFIX.length(), value.length() - FlowSerializer.ENC_SUFFIX.length()));
            } catch (EncryptionException e) {
                final String moreDescriptiveMessage = "There was a problem decrypting a sensitive flow configuration value. " +
                        "Check that the nifi.sensitive.props.key value in nifi.properties matches the value used to encrypt the flow.xml.gz file";
                logger.error(moreDescriptiveMessage, e);
                throw new EncryptionException(moreDescriptiveMessage, e);
            }
        } else {
            return value;
        }
    }
}
