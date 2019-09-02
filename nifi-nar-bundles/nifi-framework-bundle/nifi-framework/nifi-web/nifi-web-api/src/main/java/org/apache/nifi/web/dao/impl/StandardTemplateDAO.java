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
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.StandardFlowSnippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.TemplateUtils;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.parameter.ExpressionLanguageAwareParameterParser;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.dao.TemplateDAO;
import org.apache.nifi.web.util.SnippetUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 *
 */
public class StandardTemplateDAO extends ComponentDAO implements TemplateDAO {

    private FlowController flowController;
    private SnippetUtils snippetUtils;

    private Template locateTemplate(String templateId) {
        // get the template
        Template template = flowController.getFlowManager().getRootGroup().findTemplate(templateId);

        // ensure the template exists
        if (template == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate template with id '%s'.", templateId));
        }

        return template;
    }

    @Override
    public void verifyCanAddTemplate(String name, String groupId) {
        final ProcessGroup processGroup = flowController.getFlowManager().getGroup(groupId);
        if (processGroup == null) {
            throw new ResourceNotFoundException("Could not find Process Group with ID " + groupId);
        }

        processGroup.verifyCanAddTemplate(name);
    }


    @Override
    public void verifyCanInstantiate(final String groupId, final FlowSnippetDTO snippetDTO) {
        final ProcessGroup processGroup = flowController.getFlowManager().getGroup(groupId);
        if (processGroup == null) {
            throw new ResourceNotFoundException("Could not find Process Group with ID " + groupId);
        }

        verifyComponentTypes(snippetDTO);
        verifyParameterReferences(snippetDTO, processGroup.getParameterContext());
    }

    /**
     * Verifies that the Processors and Controller Services within the template do not make any illegal references to Parameters. I.e., if a Parameter is referenced from a non-sensitive property,
     * the Parameter itself must not be sensitive. Likewise, if a Parameter is referenced from a sensitive property, the Parameter itself must be sensitive.
     *
     * @param snippet the contents of the snippet
     * @param parameterContext the Parameter Context
     */
    private void verifyParameterReferences(final FlowSnippetDTO snippet, final ParameterContext parameterContext) {
        if (parameterContext == null) {
            return; // no parameters are referenced because there is no parameter context.
        }

        final ParameterParser parameterParser = new ExpressionLanguageAwareParameterParser();

        for (final ProcessorDTO processor : snippet.getProcessors()) {
            final BundleDTO bundleDto = processor.getBundle();
            final BundleCoordinate bundleCoordinate = new BundleCoordinate(bundleDto.getGroup(), bundleDto.getArtifact(), bundleDto.getVersion());
            final ConfigurableComponent component = flowController.getExtensionManager().getTempComponent(processor.getType(), bundleCoordinate);

            final ProcessorConfigDTO config = processor.getConfig();
            for (final Map.Entry<String, String> entry : config.getProperties().entrySet()) {
                final String propertyName = entry.getKey();
                final ParameterTokenList references = parameterParser.parseTokens(entry.getValue());

                final PropertyDescriptor descriptor = component.getPropertyDescriptor(propertyName);
                verifyParameterReference(descriptor, references, parameterContext);
            }
        }

        for (final ControllerServiceDTO service : snippet.getControllerServices()) {
            final BundleDTO bundleDto = service.getBundle();
            final BundleCoordinate bundleCoordinate = new BundleCoordinate(bundleDto.getGroup(), bundleDto.getArtifact(), bundleDto.getVersion());
            final ConfigurableComponent component = flowController.getExtensionManager().getTempComponent(service.getType(), bundleCoordinate);

            for (final Map.Entry<String, String> entry : service.getProperties().entrySet()) {
                final String propertyName = entry.getKey();
                final ParameterTokenList references = parameterParser.parseTokens(entry.getValue());

                final PropertyDescriptor descriptor = component.getPropertyDescriptor(propertyName);
                verifyParameterReference(descriptor, references, parameterContext);
            }
        }
    }

    private void verifyParameterReference(final PropertyDescriptor descriptor, final ParameterTokenList parameterTokenList, final ParameterContext parameterContext) {
        if (descriptor == null || parameterTokenList == null) {
            return;
        }

        final List<ParameterReference> references = parameterTokenList.toReferenceList();
        for (final ParameterReference reference : references) {
            final String parameterName = reference.getParameterName();
            final Optional<Parameter> parameter = parameterContext.getParameter(parameterName);
            if (!parameter.isPresent()) {
                continue;
            }

            final boolean parameterSensitive = parameter.get().getDescriptor().isSensitive();
            if (descriptor.isSensitive() && !parameterSensitive) {
                throw new IllegalStateException("Cannot instantiate template within this Process Group because template references the '" + parameterName
                    + "' Parameter in a sensitive property, but the Parameter is not sensitive");
            }
            if (!descriptor.isSensitive() && parameterSensitive) {
                throw new IllegalStateException("Cannot instantiate template within this Process Group because template references the '" + parameterName
                    + "' Parameter in a non-sensitive property, but the Parameter is sensitive");
            }
        }
    }

    private void verifyComponentTypes(FlowSnippetDTO snippetDto) {
        final StandardFlowSnippet flowSnippet = new StandardFlowSnippet(snippetDto, flowController.getExtensionManager());
        flowSnippet.verifyComponentTypesInSnippet();
    }

    @Override
    public Template importTemplate(TemplateDTO templateDTO, String groupId) {
        return createTemplate(templateDTO, groupId);
    }

    @Override
    public Template createTemplate(final TemplateDTO templateDTO, final String groupId) {
        final ProcessGroup processGroup = flowController.getFlowManager().getGroup(groupId);
        if (processGroup == null) {
            throw new ResourceNotFoundException("Could not find Process Group with ID " + groupId);
        }

        verifyCanAddTemplate(templateDTO.getName(), groupId);

        TemplateUtils.scrubTemplate(templateDTO);
        TemplateUtils.escapeParameterReferences(templateDTO);

        final Template template = new Template(templateDTO);
        processGroup.addTemplate(template);

        return template;
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
            childProcessGroups.forEach(processGroup -> org.apache.nifi.util.SnippetUtils.scaleSnippet(processGroup.getContents(), factorX, factorY));

            // instantiate the template into this group
            flowController.getFlowManager().instantiateSnippet(group, snippet);

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
        templates.addAll(flowController.getFlowManager().getRootGroup().findAllTemplates());
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
