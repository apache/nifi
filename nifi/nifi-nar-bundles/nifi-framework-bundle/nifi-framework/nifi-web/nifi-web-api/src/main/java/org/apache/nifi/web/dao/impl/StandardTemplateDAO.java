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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.WebApplicationException;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.dao.TemplateDAO;
import org.apache.nifi.web.util.SnippetUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 */
public class StandardTemplateDAO extends ComponentDAO implements TemplateDAO {

    private FlowController flowController;
    private SnippetUtils snippetUtils;

    private Template locateTemplate(String templateId) {
        // get the template
        Template template = flowController.getTemplate(templateId);

        // ensure the template exists
        if (template == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate template with id '%s'.", templateId));
        }

        return template;
    }

    @Override
    public Template createTemplate(TemplateDTO templateDTO) {
        try {
            return flowController.addTemplate(templateDTO);
        } catch (IOException ioe) {
            throw new WebApplicationException(new IOException("Unable to save specified template: " + ioe.getMessage()));
        }
    }

    @Override
    public Template importTemplate(TemplateDTO templateDTO) {
        try {
            return flowController.importTemplate(templateDTO);
        } catch (IOException ioe) {
            throw new WebApplicationException(new IOException("Unable to import specified template: " + ioe.getMessage()));
        }
    }

    @Override
    public FlowSnippetDTO instantiateTemplate(String groupId, Double originX, Double originY, String templateId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        // get the template id and find the template
        Template template = flowController.getTemplate(templateId);

        // ensure the template could be found
        if (template == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate template with id '%s'.", templateId));
        }

        try {
            // copy the template which pre-processes all ids
            TemplateDTO templateDetails = template.getDetails();
            FlowSnippetDTO snippet = snippetUtils.copy(templateDetails.getSnippet(), group);

            // reposition the template contents
            org.apache.nifi.util.SnippetUtils.moveSnippet(snippet, originX, originY);

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
        locateTemplate(templateId);

        try {
            // remove the specified template
            flowController.removeTemplate(templateId);
        } catch (final IOException ioe) {
            throw new WebApplicationException(new IOException("Unable to remove specified template: " + ioe.getMessage()));
        }
    }

    @Override
    public Template getTemplate(String templateId) {
        return locateTemplate(templateId);
    }

    @Override
    public Set<Template> getTemplates() {
        final Set<Template> templates = new HashSet<>();
        for (final Template template : flowController.getTemplates()) {
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
