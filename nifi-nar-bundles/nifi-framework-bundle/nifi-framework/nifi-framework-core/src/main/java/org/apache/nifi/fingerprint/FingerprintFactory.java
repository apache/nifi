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
package org.apache.nifi.fingerprint;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
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
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Creates a fingerprint of a flow.xml. The order of elements or attributes in the flow.xml does not influence the fingerprint generation.
 *
 * Only items in the flow.xml that influence the processing of data are incorporated into the fingerprint. Examples of items involved in the fingerprint are: processor IDs, processor relationships,
 * and processor properties. Examples of items not involved in the fingerprint are: items in the processor "settings" or "comments" tabs, position information, flow controller settings, and counters.
 *
 */
public class FingerprintFactory {

    /*
     * Developer Note: This class should be changed with care and coordinated
     * with all classes that use fingerprinting.  Improper coordination may
     * lead to orphaning flow files, especially when flows are reloaded in a
     * clustered environment.
     */
    // no fingerprint value
    public static final String NO_VALUE = "NO_VALUE";

    private static final String FLOW_CONFIG_XSD = "/FlowConfiguration.xsd";
    private static final String ENCRYPTED_VALUE_PREFIX = "enc{";
    private static final String ENCRYPTED_VALUE_SUFFIX = "}";
    private final StringEncryptor encryptor;
    private final DocumentBuilder flowConfigDocBuilder;

    private static final Logger logger = LoggerFactory.getLogger(FingerprintFactory.class);

    public FingerprintFactory(final StringEncryptor encryptor) {
        this.encryptor = encryptor;

        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema;
        try {
            schema = schemaFactory.newSchema(FingerprintFactory.class.getResource(FLOW_CONFIG_XSD));
        } catch (final Exception e) {
            throw new RuntimeException("Failed to parse schema for file flow configuration.", e);
        }
        try {
            documentBuilderFactory.setSchema(schema);
            flowConfigDocBuilder = documentBuilderFactory.newDocumentBuilder();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create document builder for flow configuration.", e);
        }
    }

    /**
     * Creates a fingerprint of a flow. The order of elements or attributes in the flow does not influence the fingerprint generation.
     * This method does not accept a FlowController, which means that Processors cannot be created in order to verify default property
     * values, etc. As a result, if Flow A and Flow B are fingerprinted and Flow B, for instance, contains a property with a default value
     * that is not present in Flow A, then the two will have different fingerprints.
     *
     * @param flowBytes the flow represented as bytes
     *
     * @return a generated fingerprint
     *
     * @throws FingerprintException if the fingerprint failed to be generated
     */
    public synchronized String createFingerprint(final byte[] flowBytes) throws FingerprintException {
        return createFingerprint(flowBytes, null);
    }

    /**
     * Creates a fingerprint of a flow. The order of elements or attributes in the flow does not influence the fingerprint generation.
     *
     * @param flowBytes the flow represented as bytes
     * @param controller the controller
     *
     * @return a generated fingerprint
     *
     * @throws FingerprintException if the fingerprint failed to be generated
     */
    public synchronized String createFingerprint(final byte[] flowBytes, final FlowController controller) throws FingerprintException {
        try {
            return createFingerprint(parseFlow(flowBytes), controller);
        } catch (final NoSuchAlgorithmException e) {
            throw new FingerprintException(e);
        }
    }

    /**
     * Creates a fingerprint from an XML document representing the flow.xml.
     *
     * @param flowDoc the DOM
     *
     * @return the fingerprint
     *
     * @throws NoSuchAlgorithmException ex
     * @throws UnsupportedEncodingException ex
     */
    private String createFingerprint(final Document flowDoc, final FlowController controller) throws NoSuchAlgorithmException {
        if (flowDoc == null) {
            return "";
        }

        // builder to hold fingerprint state
        final StringBuilder fingerprintBuilder = new StringBuilder();

        // add flow controller fingerprint
        final Element flowControllerElem = flowDoc.getDocumentElement();
        if (flowControllerElem == null) {
            logger.warn("Unable to create fingerprint because no 'flowController' element found in XML.");
            return "";
        }
        addFlowControllerFingerprint(fingerprintBuilder, flowControllerElem, controller);

        return fingerprintBuilder.toString();
    }

    /**
     * Parse the given flow.xml bytes into a Document instance.
     *
     * @param flow a flow
     *
     * @return the DOM
     *
     * @throws FingerprintException if the flow could not be parsed
     */
    private Document parseFlow(final byte[] flow) throws FingerprintException {
        if (flow == null || flow.length == 0) {
            return null;
        }

        try {
            return flowConfigDocBuilder.parse(new ByteArrayInputStream(flow));
        } catch (final SAXException | IOException ex) {
            throw new FingerprintException(ex);
        }
    }

    private StringBuilder addFlowControllerFingerprint(final StringBuilder builder, final Element flowControllerElem, final FlowController controller) {
        // root group
        final Element rootGroupElem = (Element) DomUtils.getChildNodesByTagName(flowControllerElem, "rootGroup").item(0);
        addProcessGroupFingerprint(builder, rootGroupElem, controller);

        final Element controllerServicesElem = DomUtils.getChild(flowControllerElem, "controllerServices");
        if (controllerServicesElem != null) {
            final List<ControllerServiceDTO> serviceDtos = new ArrayList<>();
            for (final Element serviceElem : DomUtils.getChildElementsByTagName(controllerServicesElem, "controllerService")) {
                final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(serviceElem, encryptor);
                serviceDtos.add(dto);
            }

            Collections.sort(serviceDtos, new Comparator<ControllerServiceDTO>() {
                @Override
                public int compare(final ControllerServiceDTO o1, final ControllerServiceDTO o2) {
                    if (o1 == null && o2 == null) {
                        return 0;
                    }
                    if (o1 == null && o2 != null) {
                        return 1;
                    }
                    if (o1 != null && o2 == null) {
                        return -1;
                    }

                    return o1.getId().compareTo(o2.getId());
                }
            });

            for (final ControllerServiceDTO dto : serviceDtos) {
                addControllerServiceFingerprint(builder, dto, controller);
            }
        }

        final Element reportingTasksElem = DomUtils.getChild(flowControllerElem, "reportingTasks");
        if (reportingTasksElem != null) {
            final List<ReportingTaskDTO> reportingTaskDtos = new ArrayList<>();
            for (final Element taskElem : DomUtils.getChildElementsByTagName(reportingTasksElem, "reportingTask")) {
                final ReportingTaskDTO dto = FlowFromDOMFactory.getReportingTask(taskElem, encryptor);
                reportingTaskDtos.add(dto);
            }

            Collections.sort(reportingTaskDtos, new Comparator<ReportingTaskDTO>() {
                @Override
                public int compare(final ReportingTaskDTO o1, final ReportingTaskDTO o2) {
                    if (o1 == null && o2 == null) {
                        return 0;
                    }
                    if (o1 == null && o2 != null) {
                        return 1;
                    }
                    if (o1 != null && o2 == null) {
                        return -1;
                    }

                    return o1.getId().compareTo(o2.getId());
                }
            });

            for (final ReportingTaskDTO dto : reportingTaskDtos) {
                addReportingTaskFingerprint(builder, dto, controller);
            }
        }

        return builder;
    }

    private StringBuilder addSnippetFingerprint(final StringBuilder builder, final FlowSnippetDTO snippet, final FlowController controller) {
        final Comparator<ComponentDTO> componentComparator = new Comparator<ComponentDTO>() {
            @Override
            public int compare(final ComponentDTO o1, final ComponentDTO o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 == null) {
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }

                return o1.getId().compareTo(o2.getId());
            }
        };

        final Set<ConnectionDTO> connections = snippet.getConnections();
        if (connections == null || connections.isEmpty()) {
            builder.append("NO_CONNECTIONS");
        } else {
            final List<ConnectionDTO> sortedConnections = new ArrayList<>(connections);
            Collections.sort(sortedConnections, componentComparator);

            for (final ConnectionDTO connection : sortedConnections) {
                addConnectionFingerprint(builder, connection);
            }
        }

        final Set<FunnelDTO> funnels = snippet.getFunnels();
        if (funnels == null || funnels.isEmpty()) {
            builder.append("NO_FUNNELS");
        } else {
            final List<FunnelDTO> sortedFunnels = new ArrayList<>(funnels);
            Collections.sort(sortedFunnels, componentComparator);

            for (final FunnelDTO funnel : sortedFunnels) {
                addFunnelFingerprint(builder, funnel);
            }
        }

        final Set<PortDTO> inputPorts = snippet.getInputPorts();
        if (inputPorts == null || inputPorts.isEmpty()) {
            builder.append("NO_INPUT_PORTS");
        } else {
            final List<PortDTO> sortedInputPorts = new ArrayList<>(inputPorts);
            Collections.sort(sortedInputPorts, componentComparator);

            for (final PortDTO port : sortedInputPorts) {
                addPortFingerprint(builder, port);
            }
        }

        final Set<PortDTO> outputPorts = snippet.getOutputPorts();
        if (outputPorts == null || outputPorts.isEmpty()) {
            builder.append("NO_OUTPUT_PORTS");
        } else {
            final List<PortDTO> sortedOutputPorts = new ArrayList<>(outputPorts);
            Collections.sort(sortedOutputPorts, componentComparator);

            for (final PortDTO port : sortedOutputPorts) {
                addPortFingerprint(builder, port);
            }
        }

        final Set<LabelDTO> labels = snippet.getLabels();
        if (labels == null || labels.isEmpty()) {
            builder.append("NO_LABELS");
        } else {
            final List<LabelDTO> sortedLabels = new ArrayList<>(labels);
            Collections.sort(sortedLabels, componentComparator);

            for (final LabelDTO label : sortedLabels) {
                addLabelFingerprint(builder, label);
            }
        }

        final Set<ProcessGroupDTO> procGroups = snippet.getProcessGroups();
        if (procGroups == null || procGroups.isEmpty()) {
            builder.append("NO_PROCESS_GROUPS");
        } else {
            final List<ProcessGroupDTO> sortedProcGroups = new ArrayList<>(procGroups);
            Collections.sort(sortedProcGroups, componentComparator);

            for (final ProcessGroupDTO procGroup : sortedProcGroups) {
                addProcessGroupFingerprint(builder, procGroup, controller);
            }
        }

        final Set<ProcessorDTO> processors = snippet.getProcessors();
        if (processors == null || processors.isEmpty()) {
            builder.append("NO_PROCESSORS");
        } else {
            final List<ProcessorDTO> sortedProcessors = new ArrayList<>(processors);
            Collections.sort(sortedProcessors, componentComparator);

            for (final ProcessorDTO proc : sortedProcessors) {
                addProcessorFingerprint(builder, proc, controller);
            }
        }

        final Set<RemoteProcessGroupDTO> remoteGroups = snippet.getRemoteProcessGroups();
        if (remoteGroups == null || remoteGroups.isEmpty()) {
            builder.append("NO_REMOTE_PROCESS_GROUPS");
        } else {
            final List<RemoteProcessGroupDTO> sortedRemoteGroups = new ArrayList<>(remoteGroups);
            Collections.sort(sortedRemoteGroups, componentComparator);

            for (final RemoteProcessGroupDTO remoteGroup : sortedRemoteGroups) {
                addRemoteProcessGroupFingerprint(builder, remoteGroup);
            }
        }

        final Set<ControllerServiceDTO> services = snippet.getControllerServices();
        if (services == null || services.isEmpty()) {
            builder.append("NO_CONTROLLER_SERVICES");
        } else {
            final List<ControllerServiceDTO> sortedServices = new ArrayList<>(services);
            Collections.sort(sortedServices, componentComparator);

            for (final ControllerServiceDTO service : sortedServices) {
                addControllerServiceFingerprint(builder, service, controller);
            }
        }

        return builder;
    }

    private StringBuilder addProcessGroupFingerprint(final StringBuilder builder, final Element processGroupElem, final FlowController controller) throws FingerprintException {
        // id
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(processGroupElem, "id"));

        // processors
        final List<Element> processorElems = DomUtils.getChildElementsByTagName(processGroupElem, "processor");
        Collections.sort(processorElems, getIdsComparator());
        for (final Element processorElem : processorElems) {
            addFlowFileProcessorFingerprint(builder, processorElem, controller);
        }

        // input ports
        final NodeList inputPortElems = DomUtils.getChildNodesByTagName(processGroupElem, "inputPort");
        final List<Element> sortedInputPortElems = sortElements(inputPortElems, getIdsComparator());
        for (final Element inputPortElem : sortedInputPortElems) {
            addPortFingerprint(builder, inputPortElem);
        }

        // labels
        final NodeList labelElems = DomUtils.getChildNodesByTagName(processGroupElem, "label");
        final List<Element> sortedLabels = sortElements(labelElems, getIdsComparator());
        for (final Element labelElem : sortedLabels) {
            addLabelFingerprint(builder, labelElem);
        }

        // output ports
        final NodeList outputPortElems = DomUtils.getChildNodesByTagName(processGroupElem, "outputPort");
        final List<Element> sortedOutputPortElems = sortElements(outputPortElems, getIdsComparator());
        for (final Element outputPortElem : sortedOutputPortElems) {
            addPortFingerprint(builder, outputPortElem);
        }

        // process groups
        final NodeList nestedProcessGroupElems = DomUtils.getChildNodesByTagName(processGroupElem, "processGroup");
        final List<Element> sortedNestedProcessGroupElems = sortElements(nestedProcessGroupElems, getIdsComparator());
        for (final Element nestedProcessGroupElem : sortedNestedProcessGroupElems) {
            addProcessGroupFingerprint(builder, nestedProcessGroupElem, controller);
        }

        // remote process groups
        final NodeList remoteProcessGroupElems = DomUtils.getChildNodesByTagName(processGroupElem, "remoteProcessGroup");
        final List<Element> sortedRemoteProcessGroupElems = sortElements(remoteProcessGroupElems, getIdsComparator());
        for (final Element remoteProcessGroupElem : sortedRemoteProcessGroupElems) {
            addRemoteProcessGroupFingerprint(builder, remoteProcessGroupElem);
        }

        // connections
        final NodeList connectionElems = DomUtils.getChildNodesByTagName(processGroupElem, "connection");
        final List<Element> sortedConnectionElems = sortElements(connectionElems, getIdsComparator());
        for (final Element connectionElem : sortedConnectionElems) {
            addConnectionFingerprint(builder, connectionElem);
        }

        // funnel
        final NodeList funnelElems = DomUtils.getChildNodesByTagName(processGroupElem, "funnel");
        final List<Element> sortedFunnelElems = sortElements(funnelElems, getIdsComparator());
        for (final Element funnelElem : sortedFunnelElems) {
            addFunnelFingerprint(builder, funnelElem);
        }

        return builder;
    }

    private StringBuilder addProcessGroupFingerprint(final StringBuilder builder, final ProcessGroupDTO group, final FlowController controller) {
        builder.append(group.getId());
        builder.append(group.getName());
        builder.append(group.getParentGroupId());

        final FlowSnippetDTO contents = group.getContents();
        addSnippetFingerprint(builder, contents, controller);
        return builder;
    }

    private StringBuilder addFlowFileProcessorFingerprint(final StringBuilder builder, final Element processorElem, final FlowController controller) throws FingerprintException {
        // id
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(processorElem, "id"));
        // class
        final NodeList childNodes = DomUtils.getChildNodesByTagName(processorElem, "class");
        final String className = childNodes.item(0).getTextContent();
        appendFirstValue(builder, childNodes);
        // annotation data
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(processorElem, "annotationData"));

        // create an instance of the Processor so that we know the default property values
        Processor processor = null;
        try {
            if (controller != null) {
                processor = controller.createProcessor(className, UUID.randomUUID().toString(), false).getProcessor();
            }
        } catch (ProcessorInstantiationException e) {
            logger.warn("Unable to create Processor of type {} due to {}; its default properties will be fingerprinted instead of being ignored.", className, e.toString());
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }

        // properties
        final NodeList propertyElems = DomUtils.getChildNodesByTagName(processorElem, "property");
        final List<Element> sortedPropertyElems = sortElements(propertyElems, getProcessorPropertiesComparator());
        for (final Element propertyElem : sortedPropertyElems) {
            final String propName = DomUtils.getChildElementsByTagName(propertyElem, "name").get(0).getTextContent();
            String propValue = getFirstValue(DomUtils.getChildNodesByTagName(propertyElem, "value"), null);
            addPropertyFingerprint(builder, processor, propName, propValue);
        }

        final NodeList autoTerminateElems = DomUtils.getChildNodesByTagName(processorElem, "autoTerminatedRelationship");
        final List<Element> sortedAutoTerminateElems = sortElements(autoTerminateElems, getElementTextComparator());
        for (final Element autoTerminateElem : sortedAutoTerminateElems) {
            builder.append(autoTerminateElem.getTextContent());
        }

        return builder;
    }

    private StringBuilder addProcessorFingerprint(final StringBuilder builder, final ProcessorDTO processor, final FlowController controller) {
        final ProcessorConfigDTO config = processor.getConfig();

        builder.append(processor.getId());
        builder.append(processor.getClass().getName());
        builder.append(processor.getName());
        builder.append(config.getBulletinLevel());
        builder.append(config.getComments());
        builder.append(config.getSchedulingPeriod());
        builder.append(config.getSchedulingStrategy());
        builder.append(config.getYieldDuration());
        builder.append(config.getConcurrentlySchedulableTaskCount());
        builder.append(config.getPenaltyDuration());
        builder.append(config.getAnnotationData());

        // create an instance of the Processor so that we know the default property values
        Processor processorInstance = null;
        try {
            if (controller != null) {
                processorInstance = controller.createProcessor(processor.getType(), UUID.randomUUID().toString(), false).getProcessor();
            }
        } catch (ProcessorInstantiationException e) {
            logger.warn("Unable to create Processor of type {} due to {}; its default properties will be fingerprinted instead of being ignored.", processor.getType(), e.toString());
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }

        addPropertiesFingerprint(builder, processorInstance, config.getProperties());

        final Set<String> autoTerm = config.getAutoTerminatedRelationships();
        if (autoTerm == null || autoTerm.isEmpty()) {
            builder.append("NO_AUTO_TERMINATED_RELATIONSHIPS");
        } else {
            final List<String> sortedAutoTerm = new ArrayList<>(autoTerm);
            Collections.sort(sortedAutoTerm);
            for (final String rel : sortedAutoTerm) {
                builder.append(rel);
            }
        }

        return builder;
    }

    private StringBuilder addPropertyFingerprint(final StringBuilder builder, final ConfigurableComponent component, final String propName, final String propValue) throws FingerprintException {
        // If we have a component to use, first determine if the value given is the default value for the specified property.
        // If so, we do not add the property to the fingerprint.
        // We do this because if a component is updated to add a new property, whenever we connect to the cluster, we have issues because
        // the Cluster Coordinator's flow comes from disk, where the flow.xml doesn't have the new property but our FlowController does have the new property.
        // This causes the fingerprints not to match. As a result, we just ignore default values, and this resolves the issue.

        if (component != null) {
            final PropertyDescriptor descriptor = component.getPropertyDescriptor(propName);
            if (descriptor != null && propValue != null && propValue.equals(descriptor.getDefaultValue())) {
                return builder;
            }
        }

        // check if there is a value
        if (propValue == null) {
            return builder;
        }

        // append name
        builder.append(propName).append("=");

        // append value
        if (isEncrypted(propValue)) {
            // propValue is non null, no need to use getValue
            builder.append(decrypt(propValue));
        } else {
            builder.append(getValue(propValue, NO_VALUE));
        }

        return builder;
    }

    private StringBuilder addPortFingerprint(final StringBuilder builder, final Element portElem) throws FingerprintException {
        // id
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(portElem, "id"));
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(portElem, "name"));

        final NodeList userAccessControlNodeList = DomUtils.getChildNodesByTagName(portElem, "userAccessControl");
        if (userAccessControlNodeList == null || userAccessControlNodeList.getLength() == 0) {
            builder.append("NO_USER_ACCESS_CONTROL");
        } else {
            final List<String> sortedAccessControl = new ArrayList<>();
            for (int i = 0; i < userAccessControlNodeList.getLength(); i++) {
                sortedAccessControl.add(userAccessControlNodeList.item(i).getTextContent());
            }
            Collections.sort(sortedAccessControl);
            for (final String user : sortedAccessControl) {
                builder.append(user);
            }
        }

        final NodeList groupAccessControlNodeList = DomUtils.getChildNodesByTagName(portElem, "userAccessControl");
        if (groupAccessControlNodeList == null || groupAccessControlNodeList.getLength() == 0) {
            builder.append("NO_GROUP_ACCESS_CONTROL");
        } else {
            final List<String> sortedAccessControl = new ArrayList<>();
            for (int i = 0; i < groupAccessControlNodeList.getLength(); i++) {
                sortedAccessControl.add(groupAccessControlNodeList.item(i).getTextContent());
            }

            Collections.sort(sortedAccessControl);
            for (final String user : sortedAccessControl) {
                builder.append(user);
            }
        }

        return builder;
    }

    private StringBuilder addPortFingerprint(final StringBuilder builder, final PortDTO port) {
        builder.append(port.getId());
        builder.append(port.getName());
        final Set<String> userAccessControl = port.getUserAccessControl();
        if (userAccessControl == null || userAccessControl.isEmpty()) {
            builder.append("NO_USER_ACCESS_CONTROL");
        } else {
            final List<String> sortedAccessControl = new ArrayList<>(userAccessControl);
            Collections.sort(sortedAccessControl);
            for (final String user : sortedAccessControl) {
                builder.append(user);
            }
        }

        final Set<String> groupAccessControl = port.getGroupAccessControl();
        if (groupAccessControl == null || groupAccessControl.isEmpty()) {
            builder.append("NO_GROUP_ACCESS_CONTROL");
        } else {
            final List<String> sortedAccessControl = new ArrayList<>(groupAccessControl);
            Collections.sort(sortedAccessControl);
            for (final String user : sortedAccessControl) {
                builder.append(user);
            }
        }

        return builder;
    }

    private StringBuilder addLabelFingerprint(final StringBuilder builder, final Element labelElem) {
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(labelElem, "id"));
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(labelElem, "value"));
        return builder;
    }

    private StringBuilder addLabelFingerprint(final StringBuilder builder, final LabelDTO label) {
        builder.append(label.getId());
        builder.append(label.getLabel());
        return builder;
    }

    private StringBuilder addRemoteProcessGroupFingerprint(final StringBuilder builder, final Element remoteProcessGroupElem) throws FingerprintException {
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(remoteProcessGroupElem, "id"));
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(remoteProcessGroupElem, "url"));

        final NodeList inputPortList = DomUtils.getChildNodesByTagName(remoteProcessGroupElem, "inputPort");
        final NodeList outputPortList = DomUtils.getChildNodesByTagName(remoteProcessGroupElem, "outputPort");

        final Comparator<Element> portComparator = new Comparator<Element>() {
            @Override
            public int compare(final Element o1, final Element o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 == null) {
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }

                NodeList nameList1 = DomUtils.getChildNodesByTagName(o1, "name");
                NodeList nameList2 = DomUtils.getChildNodesByTagName(o2, "name");

                if (nameList1.getLength() == 0 && nameList2.getLength() == 0) {
                    return 0;
                }
                if (nameList1.getLength() == 0) {
                    return 1;
                }
                if (nameList2.getLength() == 0) {
                    return -1;
                }

                return nameList1.item(0).getTextContent().compareTo(nameList2.item(0).getTextContent());
            }
        };

        final List<Element> sortedInputPorts = new ArrayList<>(inputPortList.getLength());
        for (int i = 0; i < inputPortList.getLength(); i++) {
            sortedInputPorts.add((Element) inputPortList.item(i));
        }
        Collections.sort(sortedInputPorts, portComparator);

        final List<Element> sortedOutputPorts = new ArrayList<>(outputPortList.getLength());
        for (int i = 0; i < outputPortList.getLength(); i++) {
            sortedOutputPorts.add((Element) outputPortList.item(i));
        }
        Collections.sort(sortedOutputPorts, portComparator);

        for (final Element inputPortElement : sortedInputPorts) {
            addRemoteGroupPortFingerprint(builder, inputPortElement);
        }

        for (final Element outputPortElement : sortedOutputPorts) {
            addRemoteGroupPortFingerprint(builder, outputPortElement);
        }

        return builder;
    }

    private StringBuilder addRemoteGroupPortFingerprint(final StringBuilder builder, final Element remoteGroupPortElement) {
        for (final String childName : new String[] {"id", "maxConcurrentTasks", "useCompression"}) {
            appendFirstValue(builder, DomUtils.getChildNodesByTagName(remoteGroupPortElement, childName));
        }

        return builder;
    }

    private StringBuilder addRemoteGroupPortFingerprint(final StringBuilder builder, final RemoteProcessGroupPortDTO port) {
        builder.append(port.getId());
        builder.append(port.getConcurrentlySchedulableTaskCount());
        builder.append(port.getUseCompression());
        return builder;
    }

    private StringBuilder addRemoteProcessGroupFingerprint(final StringBuilder builder, final RemoteProcessGroupDTO remoteGroup) {
        builder.append(remoteGroup.getId());
        builder.append(remoteGroup.getTargetUri());

        final Comparator<RemoteProcessGroupPortDTO> comparator = new Comparator<RemoteProcessGroupPortDTO>() {
            @Override
            public int compare(RemoteProcessGroupPortDTO o1, RemoteProcessGroupPortDTO o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 == null) {
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }

                return o1.getName().compareTo(o2.getName());
            }
        };

        final RemoteProcessGroupContentsDTO contents = remoteGroup.getContents();
        if (contents != null) {
            if (contents.getInputPorts() != null) {
                final List<RemoteProcessGroupPortDTO> sortedInputPorts = new ArrayList<>(contents.getInputPorts());
                Collections.sort(sortedInputPorts, comparator);
                for (final RemoteProcessGroupPortDTO port : sortedInputPorts) {
                    if (port.isConnected()) {
                        addRemoteGroupPortFingerprint(builder, port);
                    }
                }
            }

            if (contents.getOutputPorts() != null) {
                final List<RemoteProcessGroupPortDTO> sortedOutputPorts = new ArrayList<>(contents.getOutputPorts());
                Collections.sort(sortedOutputPorts, comparator);
                for (final RemoteProcessGroupPortDTO port : sortedOutputPorts) {
                    if (port.isConnected()) {
                        addRemoteGroupPortFingerprint(builder, port);
                    }
                }
            }
        }

        return builder;
    }

    private StringBuilder addConnectionFingerprint(final StringBuilder builder, final Element connectionElem) throws FingerprintException {
        // id
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(connectionElem, "id"));
        // source id
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(connectionElem, "sourceId"));
        // source group id
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(connectionElem, "sourceGroupId"));
        // source type
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(connectionElem, "sourceType"));
        // destination id
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(connectionElem, "destinationId"));
        // destination group id
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(connectionElem, "destinationGroupId"));
        // destination type
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(connectionElem, "destinationType"));

        appendFirstValue(builder, DomUtils.getChildNodesByTagName(connectionElem, "name"));

        // relationships
        final NodeList relationshipElems = DomUtils.getChildNodesByTagName(connectionElem, "relationship");
        final List<Element> sortedRelationshipElems = sortElements(relationshipElems, getConnectionRelationshipsComparator());
        for (final Element relationshipElem : sortedRelationshipElems) {
            builder.append(getValue(relationshipElem, "NO_VALUE"));
        }

        return builder;
    }

    private StringBuilder addConnectionFingerprint(final StringBuilder builder, final ConnectionDTO connection) throws FingerprintException {
        builder.append(connection.getId());
        builder.append(connection.getSource().getId());
        builder.append(connection.getSource().getGroupId());
        builder.append(connection.getSource().getType());
        builder.append(connection.getDestination().getId());
        builder.append(connection.getDestination().getGroupId());
        builder.append(connection.getDestination().getType());
        if (connection.getSelectedRelationships() != null) {
            final List<String> sortedSelectedRelationships = new ArrayList<>(connection.getSelectedRelationships());
            Collections.sort(sortedSelectedRelationships);
            for (final String rel : sortedSelectedRelationships) {
                builder.append(rel);
            }
        }

        return builder;
    }

    private StringBuilder addFunnelFingerprint(final StringBuilder builder, final Element funnelElem) throws FingerprintException {
        // id
        appendFirstValue(builder, DomUtils.getChildNodesByTagName(funnelElem, "id"));
        return builder;
    }

    private StringBuilder addFunnelFingerprint(final StringBuilder builder, final FunnelDTO funnel) {
        builder.append(funnel.getId());
        return builder;
    }

    private void addControllerServiceFingerprint(final StringBuilder builder, final ControllerServiceDTO dto, final FlowController controller) {
        builder.append(dto.getId());
        builder.append(dto.getType());
        builder.append(dto.getName());
        builder.append(dto.getComments());
        builder.append(dto.getAnnotationData());
        builder.append(dto.getState());

        // create an instance of the ControllerService so that we know the default property values
        ControllerService controllerService = null;
        try {
            if (controller != null) {
                controllerService = controller.createControllerService(dto.getType(), UUID.randomUUID().toString(), false).getControllerServiceImplementation();
            }
        } catch (Exception e) {
            logger.warn("Unable to create ControllerService of type {} due to {}; its default properties will be fingerprinted instead of being ignored.", dto.getType(), e.toString());
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }

        addPropertiesFingerprint(builder, controllerService, dto.getProperties());
    }

    private void addPropertiesFingerprint(final StringBuilder builder, final ConfigurableComponent component, final Map<String, String> properties) {
        if (properties == null) {
            builder.append("NO_PROPERTIES");
        } else {
            final SortedMap<String, String> sortedProps = new TreeMap<>(properties);
            for (final Map.Entry<String, String> entry : sortedProps.entrySet()) {
                addPropertyFingerprint(builder, component, entry.getKey(), entry.getValue());
            }
        }
    }

    private void addReportingTaskFingerprint(final StringBuilder builder, final ReportingTaskDTO dto, final FlowController controller) {
        builder.append(dto.getId());
        builder.append(dto.getType());
        builder.append(dto.getName());
        builder.append(dto.getComments());
        builder.append(dto.getSchedulingPeriod());
        builder.append(dto.getSchedulingStrategy());
        builder.append(dto.getAnnotationData());

        // create an instance of the ReportingTask so that we know the default property values
        ReportingTask reportingTask = null;
        try {
            if (controller != null) {
                reportingTask = controller.createReportingTask(dto.getType(), UUID.randomUUID().toString(), false, false).getReportingTask();
            }
        } catch (Exception e) {
            logger.warn("Unable to create ReportingTask of type {} due to {}; its default properties will be fingerprinted instead of being ignored.", dto.getType(), e.toString());
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }

        addPropertiesFingerprint(builder, reportingTask, dto.getProperties());
    }

    private Comparator<Element> getIdsComparator() {
        return new Comparator<Element>() {
            @Override
            public int compare(final Element e1, final Element e2) {
                // compare using processor ids
                final String e1Id = getFirstValue(DomUtils.getChildNodesByTagName(e1, "id"));
                final String e2Id = getFirstValue(DomUtils.getChildNodesByTagName(e2, "id"));
                return e1Id.compareTo(e2Id);
            }
        };
    }

    private Comparator<Element> getProcessorPropertiesComparator() {
        return new Comparator<Element>() {
            @Override
            public int compare(final Element e1, final Element e2) {
                // combine the property name and value for the first required property
                final String e1PropName = getFirstValue(DomUtils.getChildNodesByTagName(e1, "name"));
                String e1PropValue = getFirstValue(DomUtils.getChildNodesByTagName(e1, "value"));
                if (isEncrypted(e1PropValue)) {
                    e1PropValue = decrypt(e1PropValue);
                }
                final String e1CombinedValue = e1PropName + e1PropValue;

                // combine the property name and value for the second required property
                final String e2PropName = getFirstValue(DomUtils.getChildNodesByTagName(e2, "name"));
                String e2PropValue = getFirstValue(DomUtils.getChildNodesByTagName(e2, "value"));
                if (isEncrypted(e2PropValue)) {
                    e2PropValue = decrypt(e2PropValue);
                }
                final String e2CombinedValue = e2PropName + e2PropValue;

                // compare the combined values
                return e1CombinedValue.compareTo(e2CombinedValue);
            }
        };
    }

    private Comparator<Element> getConnectionRelationshipsComparator() {
        return getSingleChildComparator("relationship");
    }

    private Comparator<Element> getSingleChildComparator(final String childElementName) {
        return new Comparator<Element>() {
            @Override
            public int compare(final Element e1, final Element e2) {
                if (e2 == null) {
                    return -1;
                } else if (e1 == null) {
                    return 1;
                }

                // compare using processor ids
                final String e1Id = getFirstValue(DomUtils.getChildNodesByTagName(e1, childElementName));
                if (e1Id == null) {
                    return 1;
                }
                final String e2Id = getFirstValue(DomUtils.getChildNodesByTagName(e2, childElementName));
                if (e2Id == null) {
                    return -1;
                }

                return e1Id.compareTo(e2Id);
            }
        };
    }

    private Comparator<Element> getElementTextComparator() {
        return new Comparator<Element>() {
            @Override
            public int compare(final Element e1, final Element e2) {
                if (e2 == null) {
                    return -1;
                } else if (e1 == null) {
                    return 1;
                }

                return e1.getTextContent().compareTo(e2.getTextContent());
            }
        };
    }

    private List<Element> sortElements(final NodeList nodeList, final Comparator<Element> comparator) {

        final List<Element> result = new ArrayList<>();

        // add node list to sorted list
        for (int i = 0; i < nodeList.getLength(); i++) {
            result.add((Element) nodeList.item(i));
        }

        Collections.sort(result, comparator);

        return result;
    }

    private String getValue(final Node node) {
        return getValue(node, NO_VALUE);
    }

    private String getValue(final Node node, final String defaultValue) {
        final String value;
        if (node.getTextContent() == null || StringUtils.isBlank(node.getTextContent())) {
            value = defaultValue;
        } else {
            value = node.getTextContent().trim();
        }
        return value;
    }

    private String getValue(final String value, final String defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        } else {
            return value;
        }
    }

    private String getFirstValue(final NodeList nodeList) {
        return getFirstValue(nodeList, NO_VALUE);
    }

    private String getFirstValue(final NodeList nodeList, final String defaultValue) {
        final String value;
        if (nodeList == null || nodeList.getLength() == 0) {
            value = defaultValue;
        } else {
            value = getValue(nodeList.item(0));
        }
        return value;
    }

    private StringBuilder appendFirstValue(final StringBuilder builder, final NodeList nodeList) {
        return appendFirstValue(builder, nodeList, NO_VALUE);
    }

    private StringBuilder appendFirstValue(final StringBuilder builder, final NodeList nodeList, final String defaultValue) {
        return builder.append(getFirstValue(nodeList, defaultValue));
    }

    private boolean isEncrypted(final String value) {
        return (value.startsWith(ENCRYPTED_VALUE_PREFIX) && value.endsWith(ENCRYPTED_VALUE_SUFFIX));
    }

    private String decrypt(final String value) throws FingerprintException {
        final int decryptStartIdx = ENCRYPTED_VALUE_PREFIX.length();
        final int decryptEndIdx = value.length() - ENCRYPTED_VALUE_SUFFIX.length();
        return encryptor.decrypt(value.substring(decryptStartIdx, decryptEndIdx));
    }
}
