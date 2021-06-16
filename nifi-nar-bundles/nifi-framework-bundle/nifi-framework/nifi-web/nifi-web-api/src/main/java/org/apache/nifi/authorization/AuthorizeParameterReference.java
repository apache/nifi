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
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.registry.flow.VersionedParameter;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AuthorizeParameterReference {

    public static void authorizeParameterReferences(final Map<String, String> proposedProperties, final Authorizer authorizer, final Authorizable parameterContextAuthorizable, final NiFiUser user) {
        if (proposedProperties == null || parameterContextAuthorizable == null) {
            return;
        }

        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();

        boolean referencesParameter = false;
        for (final String proposedPropertyValue : proposedProperties.values()) {
            // Check if any Parameter is referenced. If so, user must have READ policy on the Parameter Context
            ParameterTokenList tokenList = parameterParser.parseTokens(proposedPropertyValue);
            if (!tokenList.toReferenceList().isEmpty()) {
                referencesParameter = true;
                break;
            }
        }

        if (referencesParameter) {
            parameterContextAuthorizable.authorize(authorizer, RequestAction.READ, user);
        }
    }

    public static void authorizeParameterReferences(final ComponentAuthorizable authorizable, final Authorizer authorizer, final Authorizable parameterContextAuthorizable, final NiFiUser user) {
        if (parameterContextAuthorizable == null) {
            return;
        }

        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();

        boolean referencesParameter = false;
        for (final PropertyDescriptor propertyDescriptor : authorizable.getPropertyDescriptors()) {
            final String rawValue = authorizable.getRawValue(propertyDescriptor);

            final ParameterTokenList tokenList = parameterParser.parseTokens(rawValue);
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

            authorizeParameterReferences(configDto.getProperties(), authorizer, parameterContextAuthorizable, user);
        }

        for (final ControllerServiceDTO serviceDto : flowSnippet.getControllerServices()) {
            authorizeParameterReferences(serviceDto.getProperties(), authorizer, parameterContextAuthorizable, user);
        }

        // Note: there is no need to recurse here because when a template/snippet is instantiated, if there are any components in child Process Groups, a new Process Group will be created
        // without any Parameter Context, so there is no need to perform any authorization beyond the top-level group where the instantiation is occurring.
    }

    /**
     * If any parameter is referenced by the given component node, will authorize user against the given group's Parameter context
     * @param destinationGroup the group that the component is being moved to
     * @param component the component being moved
     * @param authorizer the authorizer
     * @param user the nifi user
     */
    public static void authorizeParameterReferences(final ProcessGroup destinationGroup, final ComponentAuthorizable component, final Authorizer authorizer, final NiFiUser user) {
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
            final ParameterContext destinationContext = destinationGroup.getParameterContext();
            if (destinationContext != null) {
                destinationContext.authorize(authorizer, RequestAction.READ, user);
            }
        }
    }

    /**
     * Ensures that any Parameter Context that is referenced by the given VersionedParameterContext is readable by the given user. If the Versioned Parameter Context references a Parameter Context
     * (by name) that does not exist in the current flow, ensures that the user has persmissions to create a new Parameter Context. If the Versioned Parameter Context contains any Parameters that
     * do not currently exist in the Parameter Context that is referenced, ensures that the usre has permissions to WRITE to the Parameter Context so that the additional Parameter can be added.
     *
     * @param versionedParameterContext the Versioned Parameter Context
     * @param serviceFacade the Service Facade
     * @param authorizer the authorizer
     * @param lookup the authorizable lookup
     * @param user the user
     */
    public static void authorizeParameterContextAddition(final VersionedParameterContext versionedParameterContext, final NiFiServiceFacade serviceFacade, final Authorizer authorizer,
                                                         final AuthorizableLookup lookup, final NiFiUser user) {
        final ParameterContext parameterContext = serviceFacade.getParameterContextByName(versionedParameterContext.getName(), user);

        if (parameterContext == null) {
            // If Parameter Context does not yet exist, authorize that the user is allowed to create it.
            lookup.getParameterContexts().authorize(authorizer, RequestAction.WRITE, user);
            return;
        }

        // User must have READ permissions to the Parameter Context in order to use it
        parameterContext.authorize(authorizer, RequestAction.READ, user);

        // Parameter Context exists. Check if there are any new parameters that must be added.
        final Set<String> existingParameterNames = parameterContext.getParameters().keySet().stream()
            .map(ParameterDescriptor::getName)
            .collect(Collectors.toSet());

        boolean requiresAddition = false;
        for (final VersionedParameter versionedParameter : versionedParameterContext.getParameters()) {
            final String versionedParameterName = versionedParameter.getName();
            if (!existingParameterNames.contains(versionedParameterName)) {
                requiresAddition = true;
                break;
            }
        }

        if (requiresAddition) {
            // User is required to have WRITE permission to the Parameter Context in order to add one or more parameters.
            parameterContext.authorize(authorizer, RequestAction.WRITE, user);
        }
    }
}
