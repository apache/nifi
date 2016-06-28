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

import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;

public class Template implements Authorizable {

    private final TemplateDTO dto;
    private volatile ProcessGroup processGroup;

    public Template(final TemplateDTO dto) {
        this.dto = dto;
    }

    public String getIdentifier() {
        return dto.getId();
    }

    /**
     * Returns a TemplateDTO object that describes the contents of this Template
     *
     * @return template dto
     */
    public TemplateDTO getDetails() {
        return dto;
    }

    public void setProcessGroup(final ProcessGroup group) {
        this.processGroup = group;
    }

    public ProcessGroup getProcessGroup() {
        return processGroup;
    }


    @Override
    public Authorizable getParentAuthorizable() {
        return null;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.Template, dto.getId(), dto.getName());
    }

    private Set<Authorizable> getAuthorizableComponents() {
        return getAuthorizableComponents(processGroup);
    }

    private Set<Authorizable> getAuthorizableComponents(final ProcessGroup processGroup) {
        final Set<Authorizable> authComponents = new HashSet<>();
        final FlowSnippetDTO snippet = dto.getSnippet();

        authComponents.add(processGroup);

        // If there is any component in the DTO that still exists in the flow, check its authorizations
        for (final ConnectionDTO connectionDto : snippet.getConnections()) {
            final Connection connection = processGroup.getConnection(connectionDto.getId());
            if (connection != null) {
                authComponents.add(connection);
            }
        }

        // TODO: Authorize Controller Services
        for (final ControllerServiceDTO service : snippet.getControllerServices()) {
        }

        for (final LabelDTO labelDto : snippet.getLabels()) {
            final Label label = processGroup.getLabel(labelDto.getId());
            if (label != null) {
                authComponents.add(label);
            }
        }

        for (final ProcessorDTO processorDto : snippet.getProcessors()) {
            final ProcessorNode procNode = processGroup.getProcessor(processorDto.getId());
            if (procNode != null) {
                authComponents.add(procNode);
            }
        }

        for (final RemoteProcessGroupDTO groupDto : snippet.getRemoteProcessGroups()) {
            final RemoteProcessGroup rpg = processGroup.getRemoteProcessGroup(groupDto.getId());
            if (rpg != null) {
                authComponents.add(rpg);
            }
        }

        for (final ProcessGroupDTO groupDto : snippet.getProcessGroups()) {
            final ProcessGroup group = processGroup.getProcessGroup(groupDto.getId());
            if (group != null) {
                authComponents.addAll(getAuthorizableComponents(processGroup));
            }
        }

        return authComponents;
    }

    @Override
    public void authorize(final Authorizer authorizer, final RequestAction action, final NiFiUser user) throws AccessDeniedException {
        final AuthorizationResult result = checkAuthorization(authorizer, action, true, user);
        if (Result.Denied.equals(result)) {
            final String explanation = result.getExplanation() == null ? "Access is denied" : result.getExplanation();
            throw new AccessDeniedException(explanation);
        }
    }

    @Override
    public AuthorizationResult checkAuthorization(final Authorizer authorizer, final RequestAction action, final NiFiUser user) {
        return checkAuthorization(authorizer, action, false, user);
    }

    private AuthorizationResult checkAuthorization(final Authorizer authorizer, final RequestAction action, final boolean accessAttempt, final NiFiUser user) {
        // TODO - include user details context

        // build the request
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
            .identity(user.getIdentity())
            .anonymous(user.isAnonymous())
            .accessAttempt(accessAttempt)
            .action(action)
            .resource(getResource())
            .build();

        // perform the authorization
        final AuthorizationResult result = authorizer.authorize(request);

        // verify the results
        if (Result.ResourceNotFound.equals(result.getResult())) {
            for (final Authorizable child : getAuthorizableComponents()) {
                final AuthorizationResult childResult = child.checkAuthorization(authorizer, action, user);
                if (Result.Denied.equals(childResult)) {
                    return childResult;
                }
            }

            return AuthorizationResult.denied();
        } else {
            return result;
        }
    }

    @Override
    public String toString() {
        return "Template[id=" + getIdentifier() + ", Name=" + dto.getName() + "]";
    }
}
