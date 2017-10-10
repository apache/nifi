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

import org.apache.nifi.persistence.TemplateDeserializer;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RelationshipDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.w3c.dom.Element;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class TemplateUtils {

    public static TemplateDTO parseDto(final Element templateElement) {
        try {
            final DOMSource domSource = new DOMSource(templateElement);

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final StreamResult streamResult = new StreamResult(baos);

            // need to stream the template element as the TemplateDeserializer.deserialize operation needs to re-parse
            // in order to apply explicit properties on the XMLInputFactory
            final TransformerFactory transformerFactory = TransformerFactory.newInstance();
            final Transformer transformer = transformerFactory.newTransformer();
            transformer.transform(domSource, streamResult);

            return parseDto(baos.toByteArray());
        } catch (final Exception e) {
            throw new RuntimeException("Could not parse XML as a valid template", e);
        }
    }

    public static TemplateDTO parseDto(final byte[] bytes) {
        try (final InputStream in = new ByteArrayInputStream(bytes)) {
            return TemplateDeserializer.deserialize(in);
        } catch (final IOException ioe) {
            throw new RuntimeException("Could not parse bytes as template", ioe);
        }
    }

    /**
     * Scrubs the template prior to persisting in order to remove fields that shouldn't be included or are unnecessary.
     *
     * @param templateDto template
     */
    public static void scrubTemplate(final TemplateDTO templateDto) {
        scrubSnippet(templateDto.getSnippet());
    }

    private static void scrubSnippet(final FlowSnippetDTO snippet) {
        // ensure that contents have been specified
        if (snippet != null) {
            // go through each processor if specified
            if (snippet.getProcessors() != null) {
                scrubProcessors(snippet.getProcessors());
            }

            // go through each connection if specified
            if (snippet.getConnections() != null) {
                scrubConnections(snippet.getConnections());
            }

            // go through each remote process group if specified
            if (snippet.getRemoteProcessGroups() != null) {
                scrubRemoteProcessGroups(snippet.getRemoteProcessGroups());
            }

            // go through each process group if specified
            if (snippet.getProcessGroups() != null) {
                scrubProcessGroups(snippet.getProcessGroups());
            }

            // go through each controller service if specified
            if (snippet.getControllerServices() != null) {
                scrubControllerServices(snippet.getControllerServices());
            }
        }
    }

    /**
     * Scrubs process groups prior to saving.
     *
     * @param processGroups groups
     */
    private static void scrubProcessGroups(final Set<ProcessGroupDTO> processGroups) {
        // go through each process group
        for (final ProcessGroupDTO processGroupDTO : processGroups) {
            processGroupDTO.setActiveRemotePortCount(null);
            processGroupDTO.setDisabledCount(null);
            processGroupDTO.setInactiveRemotePortCount(null);
            processGroupDTO.setInputPortCount(null);
            processGroupDTO.setInvalidCount(null);
            processGroupDTO.setOutputPortCount(null);
            processGroupDTO.setRunningCount(null);
            processGroupDTO.setStoppedCount(null);

            scrubSnippet(processGroupDTO.getContents());
        }
    }

    /**
     * Scrubs processors prior to saving. This includes removing sensitive properties, validation errors, property descriptors, etc.
     *
     * @param processors procs
     */
    private static void scrubProcessors(final Set<ProcessorDTO> processors) {
        // go through each processor
        for (final ProcessorDTO processorDTO : processors) {
            final ProcessorConfigDTO processorConfig = processorDTO.getConfig();

            // ensure that some property configuration have been specified
            if (processorConfig != null) {
                // if properties have been specified, remove sensitive ones
                if (processorConfig.getProperties() != null) {
                    Map<String, String> processorProperties = processorConfig.getProperties();

                    // look for sensitive properties and remove them
                    if (processorConfig.getDescriptors() != null) {
                        final Collection<PropertyDescriptorDTO> descriptors = processorConfig.getDescriptors().values();
                        for (PropertyDescriptorDTO descriptor : descriptors) {
                            if (Boolean.TRUE.equals(descriptor.isSensitive())) {
                                processorProperties.put(descriptor.getName(), null);
                            }

                            scrubPropertyDescriptor(descriptor);
                        }
                    }
                }

                processorConfig.setCustomUiUrl(null);
                processorConfig.setDefaultConcurrentTasks(null);
                processorConfig.setDefaultSchedulingPeriod(null);
                processorConfig.setAutoTerminatedRelationships(null);
            }

            if (processorDTO.getRelationships() != null) {
                for (final RelationshipDTO relationship : processorDTO.getRelationships()) {
                    relationship.setDescription(null);
                }
            }

            processorDTO.setExtensionMissing(null);
            processorDTO.setMultipleVersionsAvailable(null);
            processorDTO.setValidationErrors(null);
            processorDTO.setInputRequirement(null);
            processorDTO.setDescription(null);
            processorDTO.setInputRequirement(null);
            processorDTO.setPersistsState(null);
            processorDTO.setSupportsBatching(null);
            processorDTO.setSupportsEventDriven(null);
            processorDTO.setSupportsParallelProcessing(null);
        }
    }

    /**
     * The only thing that we really need from the Property Descriptors in the templates is the
     * flag that indicates whether or not the property identifies a controller service.
     * Everything else is unneeded and makes templates very verbose and more importantly makes it
     * so that if one of these things changes, the template itself changes, which makes it hard to
     * use a CM tool for versioning. So we remove all that we don't need.
     *
     * @param descriptor the PropertyDescriptor to scrub
     */
    private static void scrubPropertyDescriptor(final PropertyDescriptorDTO descriptor) {
        descriptor.setAllowableValues(null);
        descriptor.setDefaultValue(null);
        descriptor.setDescription(null);
        descriptor.setDisplayName(null);
        descriptor.setDynamic(null);
        descriptor.setRequired(null);
        descriptor.setSensitive(null);
        descriptor.setSupportsEl(null);
        descriptor.setIdentifiesControllerServiceBundle(null);
    }

    private static void scrubControllerServices(final Set<ControllerServiceDTO> controllerServices) {
        for (final ControllerServiceDTO serviceDTO : controllerServices) {
            final Map<String, String> properties = serviceDTO.getProperties();
            final Map<String, PropertyDescriptorDTO> descriptors = serviceDTO.getDescriptors();

            if (properties != null && descriptors != null) {
                for (final PropertyDescriptorDTO descriptor : descriptors.values()) {
                    if (Boolean.TRUE.equals(descriptor.isSensitive())) {
                        properties.put(descriptor.getName(), null);
                    }

                    scrubPropertyDescriptor(descriptor);
                }
            }

            serviceDTO.setControllerServiceApis(null);

            serviceDTO.setExtensionMissing(null);
            serviceDTO.setMultipleVersionsAvailable(null);

            serviceDTO.setCustomUiUrl(null);
            serviceDTO.setValidationErrors(null);
        }
    }

    /**
     * Scrubs connections prior to saving. This includes removing available relationships.
     *
     * @param connections conns
     */
    private static void scrubConnections(final Set<ConnectionDTO> connections) {
        // go through each connection
        for (final ConnectionDTO connectionDTO : connections) {
            connectionDTO.setAvailableRelationships(null);

            scrubConnectable(connectionDTO.getSource());
            scrubConnectable(connectionDTO.getDestination());
        }
    }

    /**
     * Remove unnecessary fields in connectables prior to saving.
     *
     * @param connectable connectable
     */
    private static void scrubConnectable(final ConnectableDTO connectable) {
        if (connectable != null) {
            connectable.setComments(null);
            connectable.setExists(null);
            connectable.setRunning(null);
            connectable.setTransmitting(null);
            connectable.setName(null);
        }
    }

    /**
     * Remove unnecessary fields in remote groups prior to saving.
     *
     * @param remoteGroups groups
     */
    private static void scrubRemoteProcessGroups(final Set<RemoteProcessGroupDTO> remoteGroups) {
        // go through each remote process group
        for (final RemoteProcessGroupDTO remoteProcessGroupDTO : remoteGroups) {
            remoteProcessGroupDTO.setFlowRefreshed(null);
            remoteProcessGroupDTO.setInputPortCount(null);
            remoteProcessGroupDTO.setOutputPortCount(null);
            remoteProcessGroupDTO.setTransmitting(null);
            remoteProcessGroupDTO.setProxyPassword(null);
            remoteProcessGroupDTO.setActiveRemoteInputPortCount(null);
            remoteProcessGroupDTO.setInactiveRemoteInputPortCount(null);
            remoteProcessGroupDTO.setActiveRemoteOutputPortCount(null);
            remoteProcessGroupDTO.setInactiveRemoteOutputPortCount(null);
            remoteProcessGroupDTO.setAuthorizationIssues(null);
            remoteProcessGroupDTO.setFlowRefreshed(null);
            remoteProcessGroupDTO.setName(null);
            remoteProcessGroupDTO.setTargetSecure(null);
            remoteProcessGroupDTO.setTransmitting(null);
        }
    }
}
