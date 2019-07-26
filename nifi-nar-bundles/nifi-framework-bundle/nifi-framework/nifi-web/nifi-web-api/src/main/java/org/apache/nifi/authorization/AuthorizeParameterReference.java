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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.registry.flow.VersionedConfigurableComponent;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class AuthorizeParameterReference {

    public static void authorizeParameterReferences(final Map<String, String> proposedProperties, final Map<String, String> currentProperties, final Authorizer authorizer,
                                                    final Authorizable parameterContextAuthorizable, final NiFiUser user) {
        if (proposedProperties == null) {
            return;
        }

        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();

        boolean referencesParameter = false;
        for (final Map.Entry<String, String> entry : proposedProperties.entrySet()) {
            final String propertyName = entry.getKey();
            final String proposedPropertyValue = entry.getValue();

            // Check if any Parameter is referenced. If so, user must have READ policy on the Parameter Context
            ParameterTokenList tokenList = parameterParser.parseTokens(proposedPropertyValue);
            if (!tokenList.toReferenceList().isEmpty()) {
                referencesParameter = true;
                break;
            }

            // If the proposed value does not reference a Parameter but the old value does (the Parameter is being de-referenced) then we must also ensure that
            // the user has the READ policy on the Parameter Context. This is consistent with our policies on referencing Controller Services. This is done largely
            // to ensure that if a user does not have the READ policy they are not able to change a value so that it doesn't reference the Parameter and then be left
            // in a state where they cannot undo that change because they cannot change the value back to the previous value (as doing so would require READ policy
            // on the Parameter Context as per above)
            final String currentValue = currentProperties.get(propertyName);
            tokenList = parameterParser.parseTokens(currentValue);
            if (!tokenList.toReferenceList().isEmpty()) {
                referencesParameter = true;
                break;
            }
        }

        if (referencesParameter) {
            parameterContextAuthorizable.authorize(authorizer, RequestAction.READ, user);
        }
    }

    public static void authorizeParameterReferences(final FlowSnippetDTO flowSnippet, final Authorizer authorizer, final Authorizable parameterContextAuthorizable, final NiFiUser user) {
        for (final ProcessorDTO processorDto : flowSnippet.getProcessors()) {
            final ProcessorConfigDTO configDto = processorDto.getConfig();
            if (configDto == null) {
                continue;
            }

            authorizeParameterReferences(configDto.getProperties(), Collections.emptyMap(), authorizer, parameterContextAuthorizable, user);
        }

        for (final ControllerServiceDTO serviceDto : flowSnippet.getControllerServices()) {
            authorizeParameterReferences(serviceDto.getProperties(), Collections.emptyMap(), authorizer, parameterContextAuthorizable, user);
        }

        // Note: there is no need to recurse here because when a template/snippet is instantiated, if there are any components in child Process Groups, a new Process Group will be created
        // without any Parameter Context, so there is no need to perform any authorization beyond the top-level group where the instantiation is occurring.
    }

    /**
     * If any parameter is referenced by the given component node, will authorize user against both the source group and destination group's Parameter contexts
     * @param sourceGroup the group that the component is being moved from
     * @param destinationGroup the group that the component is being moved to
     * @param component the component being moved
     * @param authorizer the authorizer
     * @param user the nifi user
     */
    public static void authorizeParameterReferences(final ProcessGroup sourceGroup, final ProcessGroup destinationGroup, final ComponentAuthorizable component, final Authorizer authorizer,
                                                    final NiFiUser user) {
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();

        boolean referencesParameter = false;
        for (final PropertyDescriptor propertyDescriptor : component.getPropertyDescriptors()) {
            final String rawValue = component.getRawValue(propertyDescriptor);

            final ParameterTokenList tokenList = parameterParser.parseTokens(rawValue);
            if (!tokenList.toReferenceList().isEmpty()) {
                referencesParameter = true;
                break;
            }
        }

        if (referencesParameter) {
            final ParameterContext sourceContext = sourceGroup.getParameterContext();
            if (sourceContext != null) {
                sourceContext.authorize(authorizer, RequestAction.READ, user);
            }

            final ParameterContext destinationContext = destinationGroup.getParameterContext();
            if (destinationContext != null) {
                destinationContext.authorize(authorizer, RequestAction.READ, user);
            }
        }
    }


    /**
     * Checks the changes between the VersionedProcessGroup and the currently instantiated Process Group and if any change results in a Parameter being referenced or de-referenced, will ensure
     * that user is authorized to read the Parameter Context
     * @param versionedGroup the versioned process group
     * @param instantiatedGroup the instantiated process group that matches the given versioned group
     * @param authorizer the authorizer
     * @param user the user
     */
    public static void authorizeParameterReferences(final VersionedProcessGroup versionedGroup, final ProcessGroup instantiatedGroup, final Authorizer authorizer, final NiFiUser user) {

        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();

        final Map<String, ProcessorNode> processorsByVersionedComponentId = mapByVersionedComponentId(instantiatedGroup.getProcessors(), ProcessorNode::getVersionedComponentId);
        final Map<String, ControllerServiceNode> servicesByVersionedComponentId = mapByVersionedComponentId(instantiatedGroup.getControllerServices(false),
            ControllerServiceNode::getVersionedComponentId);

        // If any Processor or Controller Service in the Versioned flow references a Parameter, then we must authorize against the Parameter Context because we want to update the flow to now
        // reference it.
        // If any Processor or Controller Service in the instantiated flow references a Parameter, then we must authorize against the Parameter Context because we want to update the flow to no
        // longer reference it.
        boolean changeReferencesParameter = false;
        for (final VersionedProcessor processor : versionedGroup.getProcessors()) {
            final ProcessorNode instantiatedProcessor = processorsByVersionedComponentId.get(processor.getIdentifier());
            if (isChangeReferencingParameter(processor, instantiatedProcessor, parameterParser)) {
                changeReferencesParameter = true;
                break;
            }
        }

        if (!changeReferencesParameter) {
            for (final VersionedControllerService service : versionedGroup.getControllerServices()) {
                final ControllerServiceNode instantiatedService = servicesByVersionedComponentId.get(service.getIdentifier());

                if (isChangeReferencingParameter(service, instantiatedService, parameterParser)) {
                    changeReferencesParameter = true;
                    break;
                }
            }
        }

        if (changeReferencesParameter) {
            instantiatedGroup.getParameterContext().authorize(authorizer, RequestAction.READ, user);
        }


        // We must now consider any child groups that are included.
        final Map<String, ProcessGroup> childGroupsByVersionedComponentId = mapByVersionedComponentId(instantiatedGroup.getProcessGroups(), ProcessGroup::getVersionedComponentId);
        for (final VersionedProcessGroup childGroup : versionedGroup.getProcessGroups()) {
            final ProcessGroup instantiatedChild = childGroupsByVersionedComponentId.get(childGroup.getIdentifier());
            if (instantiatedChild == null) {
                // No child group has been instantiated, so there is no Parameter Context to check permissions of. We can just ignore this group.
                continue;
            }

            authorizeParameterReferences(childGroup, instantiatedChild, authorizer, user);
        }
    }

    private static <T> Map<String, T> mapByVersionedComponentId(final Collection<T> components, final Function<T, Optional<String>> versionedIdFunction) {
        final Map<String, T> componentsByVersionedComponentId = new HashMap<>();
        for (final T component : components) {
            final Optional<String> versionedComponentIdOption = versionedIdFunction.apply(component);
            if (versionedComponentIdOption.isPresent()) {
                // If the Processor has a Versioned Component ID, properties that reference Parameters may or may not be changed. So, we need to consider properties that are changed below.
                componentsByVersionedComponentId.put(versionedComponentIdOption.get(), component);
            }
        }

        return componentsByVersionedComponentId;
    }

    private static boolean isChangeReferencingParameter(final VersionedConfigurableComponent versionedComponent, final ComponentNode instantiatedComponent, final ParameterParser parameterParser) {
        for (final Map.Entry<String, String> entry : versionedComponent.getProperties().entrySet()) {
            final String propertyName = entry.getKey();
            final String propertyValue = entry.getValue();

            final String rawConfiguredValue = instantiatedComponent.getProperty(instantiatedComponent.getPropertyDescriptor(propertyName)).getRawValue();
            if (instantiatedComponent == null || !Objects.equals(propertyValue, rawConfiguredValue)) {
                // If property value changed or it's a new property value, we need to check if it references any Parameters.
                final ParameterTokenList tokenList = parameterParser.parseTokens(propertyValue);
                final List<ParameterReference> referenceList = tokenList.toReferenceList();
                if (!referenceList.isEmpty()) {
                    return true;
                }

                if (rawConfiguredValue != null) {
                    final ParameterTokenList configuredValueTokenList = parameterParser.parseTokens(rawConfiguredValue);
                    final List<ParameterReference> configuredValueReferenceList = configuredValueTokenList.toReferenceList();
                    if (!configuredValueReferenceList.isEmpty()) {
                        return true;
                    }
                }
            }
        }

        // If any property exists in the currently instantiated processor but not in the Versioned Processor, then that property will be removed, so we will need to ensure that Parameter
        // Context permissions are checked.
        if (instantiatedComponent == null) {
            return false;
        }

        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : instantiatedComponent.getProperties().entrySet()) {
            final String propertyName = entry.getKey().getName();
            final String versionedProcessorValue = versionedComponent.getProperties().get(propertyName);
            if (versionedProcessorValue == null) {
                // Property does not exist in Versioned Processor, so it will be removed. We must ensure that Parameter Context permissions are checked if Parameter is referenced.
                final String rawPropertyValue = entry.getValue() == null ? null : entry.getValue().getRawValue();
                final ParameterTokenList tokenList = parameterParser.parseTokens(rawPropertyValue);
                final List<ParameterReference> referenceList = tokenList.toReferenceList();
                if (!referenceList.isEmpty()) {
                    return true;
                }
            }
        }

        return false;
    }
}
