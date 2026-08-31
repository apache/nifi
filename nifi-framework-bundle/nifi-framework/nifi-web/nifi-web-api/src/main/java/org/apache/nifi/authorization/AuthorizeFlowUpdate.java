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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;

import java.util.Map;
import java.util.Set;

/**
 * Authorizes replacing the contents of a Process Group with a proposed flow snapshot along with resolved references
 */
public final class AuthorizeFlowUpdate {
    /**
     * Identifiers of inherited Controller Services and Parameter Providers from a proposed snapshot that could not be
     * resolved against the local flow.
     *
     * @param controllerServices unresolved inherited Controller Service identifiers
     * @param parameterProviders unresolved Parameter Provider identifiers
     */
    public record UnresolvedReferences(Set<String> controllerServices, Set<String> parameterProviders) {
    }

    /**
     * Resolves inherited references on the proposed snapshot and then authorizes the current user requesting flow updates
     *
     * @param groupId the id of the process group being updated
     * @param flowSnapshot the proposed flow contents
     * @param serviceFacade the service facade
     * @param authorizer the authorizer
     * @param lookup the authorizable lookup
     * @param user the user to authorize
     */
    public static void resolveAndAuthorizeFlowUpdate(
            final String groupId,
            final RegisteredFlowSnapshot flowSnapshot,
            final NiFiServiceFacade serviceFacade,
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final NiFiUser user
    ) {
        final FlowSnapshotContainer flowSnapshotContainer = new FlowSnapshotContainer(flowSnapshot);
        final UnresolvedReferences unresolvedReferences = resolveReferences(groupId, flowSnapshotContainer, serviceFacade, user);
        authorizeFlowUpdate(groupId, flowSnapshot, unresolvedReferences, serviceFacade, authorizer, lookup, user);
    }

    /**
     * Discovers compatible bundles for the proposed snapshot and resolves the inherited Controller Services and the
     * Parameter Providers that it references to components in the local flow
     *
     * @param groupId the id of the process group being updated
     * @param flowSnapshotContainer the proposed flow snapshot along with the snapshots of any version controlled child groups
     * @param serviceFacade the service facade
     * @param user the user performing the update
     * @return the references that could not be resolved against the local flow
     */
    public static UnresolvedReferences resolveReferences(
            final String groupId,
            final FlowSnapshotContainer flowSnapshotContainer,
            final NiFiServiceFacade serviceFacade,
            final NiFiUser user
    ) {
        final RegisteredFlowSnapshot flowSnapshot = flowSnapshotContainer.getFlowSnapshot();

        // Discover compatible bundles since the flow snapshot can contain different versions
        serviceFacade.discoverCompatibleBundles(flowSnapshot.getFlowContents());
        serviceFacade.discoverCompatibleBundles(flowSnapshot.getParameterProviders());

        // If there are any Controller Services referenced that are inherited from the parent group, resolve those to point to the appropriate Controller Service
        final Set<String> unresolvedControllerServices = serviceFacade.resolveInheritedControllerServices(flowSnapshotContainer, groupId, user);

        // If there are any Parameter Providers referenced by Parameter Contexts, resolve these to point to the appropriate Parameter Provider
        final Set<String> unresolvedParameterProviders = serviceFacade.resolveParameterProviders(flowSnapshot, user);

        return new UnresolvedReferences(unresolvedControllerServices, unresolvedParameterProviders);
    }

    /**
     * Authorizes READ and WRITE permissions for the given user on the Process Group being updated
     *
     * @param groupId the id of the process group being updated
     * @param flowSnapshot the proposed flow contents
     * @param unresolvedReferences the references from the proposed snapshot that could not be resolved against the local flow
     * @param serviceFacade the service facade
     * @param authorizer the authorizer
     * @param lookup the authorizable lookup
     * @param user the user to authorize
     */
    public static void authorizeFlowUpdate(
            final String groupId,
            final RegisteredFlowSnapshot flowSnapshot,
            final UnresolvedReferences unresolvedReferences,
            final NiFiServiceFacade serviceFacade,
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final NiFiUser user
    ) {
        final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
        AuthorizeProcessGroup.authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.READ, true, false, true, false, true);
        AuthorizeProcessGroup.authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.WRITE, true, false, true, false, false);

        final Map<String, VersionedParameterContext> parameterContexts = flowSnapshot.getParameterContexts();
        if (parameterContexts != null) {
            for (final VersionedParameterContext parameterContext : parameterContexts.values()) {
                AuthorizeParameterReference.authorizeParameterContextAddition(parameterContext, serviceFacade, authorizer, lookup, user);
            }
        }

        AuthorizeParameterProviders.authorizeUnresolvedParameterProviders(unresolvedReferences.parameterProviders(), authorizer, lookup, user);
        AuthorizeControllerServiceReference.authorizeUnresolvedControllerServiceReferences(groupId, unresolvedReferences.controllerServices(), authorizer, lookup, user);
    }
}
