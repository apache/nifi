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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.BundleDTO;

import java.util.List;
import java.util.Optional;

public abstract class ComponentDAO {

    /**
     * Returns whether the specified object is not null.
     *
     * @param <T> type
     * @param object object
     * @return true if the specified object is not null
     */
    protected <T> boolean isNotNull(T object) {
        return object != null;
    }

    /**
     * Returns whether any of the specified objects are not null.
     *
     * @param <T> type
     * @param objects objects
     * @return true if any of the specified objects are not null
     */
    protected <T> boolean isAnyNotNull(T... objects) {
        for (final T object : objects) {
            if (object != null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Locates the specified ProcessGroup.
     *
     * @param flowController controller
     * @param groupId id
     * @return group
     */
    protected ProcessGroup locateProcessGroup(final FlowController flowController, final String groupId) {
        return locateProcessGroup(flowController, groupId, false);
    }

    /**
     * Locates the specified ProcessGroup, optionally including Connector-managed ProcessGroups.
     *
     * @param flowController controller
     * @param groupId id
     * @param includeConnectorManaged whether to include Connector-managed ProcessGroups in the search
     * @return group
     */
    protected ProcessGroup locateProcessGroup(final FlowController flowController, final String groupId, final boolean includeConnectorManaged) {
        // First, try to find the group in the main flow hierarchy (non-Connector groups)
        ProcessGroup group = flowController.getFlowManager().getGroup(groupId, null);

        if (group != null) {
            return group;
        }

        // Search Connector-managed ProcessGroups. The unconditional search is important so that if a component exists
        // in a Connector-managed flow but the Connector is not in Troubleshooting mode, we can produce a clear 409
        // Conflict response rather than a 404 Not Found.
        group = flowController.getFlowManager().getGroup(groupId);
        if (group != null) {
            if (includeConnectorManaged) {
                return group;
            }

            verifyAccessibleForComponentOperation(group, groupId);
            return group;
        }

        throw new ResourceNotFoundException(String.format("Unable to locate group with id '%s'.", groupId));
    }

    /**
     * Verifies that the component represented by the given {@link ProcessGroup} (or a component contained within it) is
     * accessible for a direct user-facing operation such as GET/PUT/POST/DELETE of the component itself. Components that
     * live within a Connector's managed Process Group hierarchy are only accessible when the owning Connector is in
     * {@link ConnectorState#TROUBLESHOOTING} mode. If the owning Connector is not in Troubleshooting mode, an
     * {@link IllegalStateException} is thrown which is translated by the REST layer into a 409 Conflict response.
     *
     * <p>Connector-aware REST endpoints that need to read components within a managed flow regardless of the
     * Connector's state must obtain those components through the
     * {@link org.apache.nifi.web.dao.ConnectorManagedComponentLookup} facade (or, for authorization, through
     * {@link org.apache.nifi.authorization.AuthorizableLookup#forConnectorManagedFlow()}) so that this verification is
     * skipped at the locate call site rather than being bypassed globally for the current thread.
     *
     * @param group the ProcessGroup that owns (or is) the component being accessed
     * @param componentId the identifier of the component being accessed (used in the error message)
     */
    protected void verifyAccessibleForComponentOperation(final ProcessGroup group, final String componentId) {
        if (group == null) {
            return;
        }

        final Optional<ConnectorNode> owningConnector = group.findOwningConnector();
        if (owningConnector.isEmpty()) {
            return;
        }

        final ConnectorNode connector = owningConnector.get();
        if (connector.getCurrentState() != ConnectorState.TROUBLESHOOTING) {
            throw new ConnectorManagedAccessException("Component [" + componentId + "] is managed by a Connector ["
                + connector.getIdentifier() + "]; the Connector must be in Troubleshooting mode for this component to be accessible.",
                connector.getIdentifier());
        }
    }

    protected void verifyCreate(final ExtensionManager extensionManager, final String type, final BundleDTO bundle) {
        final List<Bundle> bundles = extensionManager.getBundles(type);

        if (bundle != null) {
            final BundleCoordinate coordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            if (bundles.stream().filter(b -> b.getBundleDetails().getCoordinate().equals(coordinate)).count() == 0) {
                throw new IllegalStateException(String.format("%s is not known to this NiFi instance.", coordinate));
            }
        } else {
            if (bundles.isEmpty()) {
                throw new IllegalStateException(String.format("%s is not known to this NiFi instance.", type));
            } else if (bundles.size() > 1) {
                throw new IllegalStateException(String.format("Multiple versions of %s exist. Please specify the desired bundle.", type));
            }
        }
    }
}
