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
package org.apache.nifi.web.dao.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.StandardSnippet;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.dao.SnippetDAO;
import org.apache.nifi.web.util.SnippetUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class StandardSnippetDAO implements SnippetDAO {

    private FlowController flowController;
    private SnippetUtils snippetUtils;

    private StandardSnippet locateSnippet(String snippetId) {
        final StandardSnippet snippet = flowController.getSnippetManager().getSnippet(snippetId);

        if (snippet == null) {
            throw new ResourceNotFoundException(String.format("Unable to find snippet with id '%s'.", snippetId));
        }

        return snippet;
    }

    @Override
    public FlowSnippetDTO copySnippet(final String groupId, final String snippetId, final Double originX, final Double originY, final String idGenerationSeed) {
        try {
            // ensure the parent group exist
            final ProcessGroup processGroup = flowController.getGroup(groupId);
            if (processGroup == null) {
                throw new IllegalArgumentException("The specified parent process group could not be found");
            }

            // get the existing snippet
            Snippet existingSnippet = getSnippet(snippetId);

            // get the process group
            ProcessGroup existingSnippetProcessGroup = flowController.getGroup(existingSnippet.getParentGroupId());

            // ensure the group could be found
            if (existingSnippetProcessGroup == null) {
                throw new IllegalStateException("The parent process group for the existing snippet could not be found.");
            }

            // generate the snippet contents
            FlowSnippetDTO snippetContents = snippetUtils.populateFlowSnippet(existingSnippet, true, false, false);

            // resolve sensitive properties
            lookupSensitiveProperties(snippetContents);

            // copy snippet
            snippetContents = snippetUtils.copy(snippetContents, processGroup, idGenerationSeed, true);

            // move the snippet if necessary
            if (originX != null && originY != null) {
                org.apache.nifi.util.SnippetUtils.moveSnippet(snippetContents, originX, originY);
            }

            try {
                // instantiate the snippet and return the contents
                flowController.instantiateSnippet(processGroup, snippetContents);
                return snippetContents;
            } catch (IllegalStateException ise) {
                // illegal state will be thrown from instantiateSnippet when there is an issue with the snippet _before_ any of the
                // components are actually created. if we've received this exception we want to attempt to roll back any of the
                // policies that we've already cloned for this request
                snippetUtils.rollbackClonedPolicies(snippetContents);

                // rethrow the same exception
                throw ise;
            }
        } catch (ProcessorInstantiationException pie) {
            throw new NiFiCoreException(String.format("Unable to copy snippet because processor type '%s' is unknown to this NiFi.",
                    StringUtils.substringAfterLast(pie.getMessage(), ".")));
        }
    }

    private Map<String, Revision> mapDtoToRevision(final Map<String, RevisionDTO> revisionMap) {
        final Map<String, Revision> revisions = new HashMap<>(revisionMap.size());
        for (final Map.Entry<String, RevisionDTO> entry : revisionMap.entrySet()) {
            final RevisionDTO revisionDto = entry.getValue();
            final Revision revision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), entry.getKey());

            revisions.put(entry.getKey(), revision);
        }
        return revisions;
    }

    @Override
    public Snippet createSnippet(final SnippetDTO snippetDTO) {
        // create the snippet request
        final StandardSnippet snippet = new StandardSnippet();
        snippet.setId(snippetDTO.getId());
        snippet.setParentGroupId(snippetDTO.getParentGroupId());
        snippet.addProcessors(mapDtoToRevision(snippetDTO.getProcessors()));
        snippet.addProcessGroups(mapDtoToRevision(snippetDTO.getProcessGroups()));
        snippet.addRemoteProcessGroups(mapDtoToRevision(snippetDTO.getRemoteProcessGroups()));
        snippet.addInputPorts(mapDtoToRevision(snippetDTO.getInputPorts()));
        snippet.addOutputPorts(mapDtoToRevision(snippetDTO.getOutputPorts()));
        snippet.addConnections(mapDtoToRevision(snippetDTO.getConnections()));
        snippet.addLabels(mapDtoToRevision(snippetDTO.getLabels()));
        snippet.addFunnels(mapDtoToRevision(snippetDTO.getFunnels()));

        // ensure this snippet isn't empty
        if (snippet.isEmpty()) {
            throw new IllegalArgumentException("Cannot create an empty snippet.");
        }

        // ensure the parent group exist
        final ProcessGroup processGroup = flowController.getGroup(snippet.getParentGroupId());
        if (processGroup == null) {
            throw new IllegalArgumentException("The specified parent process group could not be found.");
        }

        // store the snippet
        flowController.getSnippetManager().addSnippet(snippet);
        return snippet;
    }

    @Override
    public void verifyDeleteSnippetComponents(String snippetId) {
        final Snippet snippet = locateSnippet(snippetId);

        // ensure the parent group exist
        final ProcessGroup processGroup = flowController.getGroup(snippet.getParentGroupId());
        if (processGroup == null) {
            throw new IllegalArgumentException("The specified parent process group could not be found.");
        }

        // verify the processGroup can remove the snippet
        processGroup.verifyCanDelete(snippet);
    }

    @Override
    public void deleteSnippetComponents(String snippetId) {
        // verify the action
        verifyDeleteSnippetComponents(snippetId);

        // locate the snippet in question
        final Snippet snippet = locateSnippet(snippetId);

        // remove the contents
        final ProcessGroup processGroup = flowController.getGroup(snippet.getParentGroupId());
        if (processGroup == null) {
            throw new IllegalArgumentException("The specified parent process group could not be found.");
        }

        // remove the underlying components
        processGroup.remove(snippet);
    }

    @Override
    public Snippet getSnippet(String snippetId) {
        return locateSnippet(snippetId);
    }

    @Override
    public boolean hasSnippet(String snippetId) {
        return flowController.getSnippetManager().getSnippet(snippetId) != null;
    }

    @Override
    public void dropSnippet(String snippetId) {
        // drop the snippet itself
        final StandardSnippet snippet = locateSnippet(snippetId);
        flowController.getSnippetManager().removeSnippet(snippet);
    }

    @Override
    public void verifyUpdateSnippetComponent(SnippetDTO snippetDTO) {
        final Snippet snippet = locateSnippet(snippetDTO.getId());

        // if the group is changing move it
        if (snippetDTO.getParentGroupId() != null && !snippet.getParentGroupId().equals(snippetDTO.getParentGroupId())) {
            // get the current process group
            final ProcessGroup processGroup = flowController.getGroup(snippet.getParentGroupId());
            if (processGroup == null) {
                throw new IllegalArgumentException("The specified parent process group could not be found.");
            }

            // get the new process group
            final ProcessGroup newProcessGroup = flowController.getGroup(snippetDTO.getParentGroupId());
            if (newProcessGroup == null) {
                throw new IllegalArgumentException("The new process group could not be found.");
            }

            // perform the verification
            processGroup.verifyCanMove(snippet, newProcessGroup);
        }
    }

    @Override
    public Snippet updateSnippetComponents(final SnippetDTO snippetDTO) {
        // verify the action
        verifyUpdateSnippetComponent(snippetDTO);

        // find the snippet in question
        final StandardSnippet snippet = locateSnippet(snippetDTO.getId());

        // if the group is changing move it
        if (snippetDTO.getParentGroupId() != null && !snippet.getParentGroupId().equals(snippetDTO.getParentGroupId())) {
            final ProcessGroup currentProcessGroup = flowController.getGroup(snippet.getParentGroupId());
            if (currentProcessGroup == null) {
                throw new IllegalArgumentException("The current process group could not be found.");
            }

            final ProcessGroup newProcessGroup = flowController.getGroup(snippetDTO.getParentGroupId());
            if (newProcessGroup == null) {
                throw new IllegalArgumentException("The new process group could not be found.");
            }

            // move the snippet
            currentProcessGroup.move(snippet, newProcessGroup);

            // update its parent group id
            snippet.setParentGroupId(snippetDTO.getParentGroupId());
        }

        return snippet;
    }

    private void lookupSensitiveProperties(final FlowSnippetDTO snippet) {
        // ensure that contents have been specified
        if (snippet != null) {
            // go through each processor if specified
            if (snippet.getProcessors() != null) {
                lookupSensitiveProcessorProperties(snippet.getProcessors());
            }

            if (snippet.getControllerServices() != null) {
                lookupSensitiveControllerServiceProperties(snippet.getControllerServices());
            }

            // go through each process group if specified
            if (snippet.getProcessGroups() != null) {
                for (final ProcessGroupDTO group : snippet.getProcessGroups()) {
                    lookupSensitiveProperties(group.getContents());
                }
            }
        }
    }

    private void lookupSensitiveProcessorProperties(final Set<ProcessorDTO> processors) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());

        // go through each processor
        for (final ProcessorDTO processorDTO : processors) {
            final ProcessorConfigDTO processorConfig = processorDTO.getConfig();

            // ensure that some property configuration have been specified
            if (processorConfig != null && processorConfig.getProperties() != null) {
                final Map<String, String> processorProperties = processorConfig.getProperties();

                // find the corresponding processor
                final ProcessorNode processorNode = rootGroup.findProcessor(processorDTO.getId());
                if (processorNode == null) {
                    throw new IllegalArgumentException(String.format("Unable to create snippet because Processor '%s' could not be found", processorDTO.getId()));
                }

                // look for sensitive properties get the actual value
                for (Entry<PropertyDescriptor, String> entry : processorNode.getProperties().entrySet()) {
                    final PropertyDescriptor descriptor = entry.getKey();

                    if (descriptor.isSensitive()) {
                        processorProperties.put(descriptor.getName(), entry.getValue());
                    }
                }
            }
        }
    }

    private void lookupSensitiveControllerServiceProperties(final Set<ControllerServiceDTO> controllerServices) {
        // go through each service
        for (final ControllerServiceDTO serviceDTO : controllerServices) {

            // ensure that some property configuration have been specified
            final Map<String, String> serviceProperties = serviceDTO.getProperties();
            if (serviceProperties != null) {
                // find the corresponding controller service
                final ControllerServiceNode serviceNode = flowController.getControllerServiceNode(serviceDTO.getId());
                if (serviceNode == null) {
                    throw new IllegalArgumentException(String.format("Unable to create snippet because Controller Service '%s' could not be found", serviceDTO.getId()));
                }

                // look for sensitive properties get the actual value
                for (Entry<PropertyDescriptor, String> entry : serviceNode.getProperties().entrySet()) {
                    final PropertyDescriptor descriptor = entry.getKey();

                    if (descriptor.isSensitive()) {
                        serviceProperties.put(descriptor.getName(), entry.getValue());
                    }
                }
            }
        }
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }

    public void setSnippetUtils(SnippetUtils snippetUtils) {
        this.snippetUtils = snippetUtils;
    }

}
