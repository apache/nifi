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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.TemplateUtils;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.dao.TemplateDAO;
import org.apache.nifi.web.util.SnippetUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class StandardTemplateDAO extends ComponentDAO implements TemplateDAO {

    private FlowController flowController;
    private SnippetUtils snippetUtils;

    private Template locateTemplate(String templateId) {
        // get the template
        Template template = flowController.getGroup(flowController.getRootGroupId()).findTemplate(templateId);

        // ensure the template exists
        if (template == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate template with id '%s'.", templateId));
        }

        return template;
    }

    @Override
    public void verifyCanAddTemplate(String name, String groupId) {
        final ProcessGroup processGroup = flowController.getGroup(groupId);
        if (processGroup == null) {
            throw new ResourceNotFoundException("Could not find Process Group with ID " + groupId);
        }

        verifyAdd(name, processGroup);
    }

    private void verifyAdd(final String name, final ProcessGroup processGroup) {
        processGroup.verifyCanAddTemplate(name);
    }

    @Override
    public void verifyComponentTypes(FlowSnippetDTO snippet) {
        flowController.verifyComponentTypesInSnippet(snippet);
    }

    @Override
    public Template createTemplate(TemplateDTO templateDTO, String groupId) {
        final ProcessGroup processGroup = flowController.getGroup(groupId);
        if (processGroup == null) {
            throw new ResourceNotFoundException("Could not find Process Group with ID " + groupId);
        }

        verifyAdd(templateDTO.getName(), processGroup);

        TemplateUtils.scrubTemplate(templateDTO);
        final Template template = new Template(templateDTO);
        processGroup.addTemplate(template);

        return template;
    }

    @Override
    public Template importTemplate(TemplateDTO templateDTO, String groupId) {
        return createTemplate(templateDTO, groupId);
    }

    @Override
    public FlowSnippetDTO instantiateTemplate(String groupId, Double originX, Double originY, String encodingVersion,
                                              FlowSnippetDTO requestSnippet, String idGenerationSeed) {

        ProcessGroup group = locateProcessGroup(flowController, groupId);

        try {
            // copy the template which pre-processes all ids
            FlowSnippetDTO snippet = snippetUtils.copy(requestSnippet, group, idGenerationSeed, false);

            // calculate scaling factors based on the template encoding version attempt to parse the encoding version.
            // get the major version, or 0 if no version could be parsed
            final FlowEncodingVersion templateEncodingVersion = FlowEncodingVersion.parse(encodingVersion);
            int templateEncodingMajorVersion = templateEncodingVersion != null ? templateEncodingVersion.getMajorVersion() : 0;

            // based on the major version < 1, use the default scaling factors.  Otherwise, don't scale (use factor of 1.0)
            double factorX = templateEncodingMajorVersion < 1 ? FlowController.DEFAULT_POSITION_SCALE_FACTOR_X : 1.0;
            double factorY = templateEncodingMajorVersion < 1 ? FlowController.DEFAULT_POSITION_SCALE_FACTOR_Y : 1.0;

            // reposition and scale the template contents
            org.apache.nifi.util.SnippetUtils.moveAndScaleSnippet(snippet, originX, originY, factorX, factorY);

            // find all the child process groups in each process group in the top level of this snippet
            final List<ProcessGroupDTO> childProcessGroups  = org.apache.nifi.util.SnippetUtils.findAllProcessGroups(snippet);
            // scale (but don't reposition) child process groups
            childProcessGroups.stream().forEach(processGroup -> org.apache.nifi.util.SnippetUtils.scaleSnippet(processGroup.getContents(), factorX, factorY));

            // instantiate the template into this group
            flowController.instantiateSnippet(group, snippet);

            return snippet;
        } catch (ProcessorInstantiationException pie) {
            throw new NiFiCoreException(String.format("Unable to instantiate template because processor type '%s' is unknown to this NiFi.",
                    StringUtils.substringAfterLast(pie.getMessage(), ".")));
        }
    }

    @Override
    public void deleteTemplate(String templateId) {
        // ensure the template exists
        final Template template = locateTemplate(templateId);

        // remove the specified template
        template.getProcessGroup().removeTemplate(template);
    }

    @Override
    public Template getTemplate(String templateId) {
        return locateTemplate(templateId);
    }

    @Override
    public Set<Template> getTemplates() {
        final Set<Template> templates = new HashSet<>();
        for (final Template template : flowController.getGroup(flowController.getRootGroupId()).findAllTemplates()) {
            templates.add(template);
        }
        return templates;
    }

    /*
     * setters
     */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }

    public void setSnippetUtils(SnippetUtils snippetUtils) {
        this.snippetUtils = snippetUtils;
    }

}
