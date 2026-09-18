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
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;

import java.util.List;

/**
 * Authorizes updates to components that reference a Controller Service.
 */
public final class AuthorizeControllerServiceReferencingComponents {

    /**
     * Authorizes operation of the Controller Service together with each component that the requested state change
     * affects. Referencing Controller Services are authorized when a Controller Service state is requested. Referencing
     * Processors, Reporting Tasks, and Flow Analysis Rules are authorized when a scheduled state is requested. The
     * referencing components are resolved from the Controller Service reference graph, matching the components that the
     * requested state change updates.
     *
     * @param authorizer authorizer
     * @param lookup lookup
     * @param controllerServiceId controller service id
     * @param controllerServiceState requested Controller Service state or null when not requested
     * @param scheduledState requested scheduled state or null when not requested
     * @param user user
     */
    public static void authorize(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final String controllerServiceId,
            final ControllerServiceState controllerServiceState,
            final ScheduledState scheduledState,
            final NiFiUser user) {

        final Authorizable controllerService = lookup.getControllerService(controllerServiceId).getAuthorizable();
        OperationAuthorizable.authorizeOperation(controllerService, authorizer, user);

        if (controllerServiceState != null) {
            authorizeReferencingComponents(authorizer, lookup, controllerServiceId, user, ControllerServiceNode.class);
            return;
        }

        if (scheduledState == null) {
            return;
        }

        authorizeReferencingComponents(authorizer, lookup, controllerServiceId, user, ProcessorNode.class);
        authorizeReferencingComponents(authorizer, lookup, controllerServiceId, user, ReportingTaskNode.class);
        authorizeReferencingComponents(authorizer, lookup, controllerServiceId, user, FlowAnalysisRuleNode.class);
    }

    private static void authorizeReferencingComponents(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final String controllerServiceId,
            final NiFiUser user,
            final Class<? extends Authorizable> componentType) {

        final List<Authorizable> referencingComponents = lookup.getControllerServiceReferencingComponents(controllerServiceId, componentType);
        for (final Authorizable referencingComponent : referencingComponents) {
            OperationAuthorizable.authorizeOperation(referencingComponent, authorizer, user);
        }
    }

    private AuthorizeControllerServiceReferencingComponents() {
    }
}
