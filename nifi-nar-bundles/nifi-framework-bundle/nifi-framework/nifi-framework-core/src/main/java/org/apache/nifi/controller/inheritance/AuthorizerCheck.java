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
package org.apache.nifi.controller.inheritance;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class AuthorizerCheck implements FlowInheritabilityCheck {
    @Override
    public FlowInheritability checkInheritability(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController flowController) {
        final byte[] existing = existingFlow.getAuthorizerFingerprint();
        final byte[] proposed = proposedFlow.getAuthorizerFingerprint();

        // both are using external authorizers so nothing to inherit, but we don't want to throw an exception
        if (existing == null && proposed == null) {
            return FlowInheritability.notInheritable(null);
        }

        // current is external, but proposed is internal
        if (existing == null && proposed != null) {
            return FlowInheritability.notInheritable(
                "Current Authorizer is an external Authorizer, but proposed Authorizer is an internal Authorizer");
        }

        // current is internal, but proposed is external
        if (existing != null && proposed == null) {
            return FlowInheritability.notInheritable(
                "Current Authorizer is an internal Authorizer, but proposed Authorizer is an external Authorizer");
        }

        // both are internal, but not the same
        if (!Arrays.equals(existing, proposed)) {
            final Authorizer authorizer = flowController.getAuthorizer();

            if (AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
                final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;

                try {
                    // if the configurations are not equal, see if the manager indicates the proposed configuration is inheritable
                    managedAuthorizer.checkInheritability(new String(proposed, StandardCharsets.UTF_8));
                    return FlowInheritability.inheritable();
                } catch (final UninheritableAuthorizationsException e) {
                    return FlowInheritability.notInheritable("Proposed Authorizations do not match current Authorizations: " + e.getMessage());
                }
            } else {
                // should never hit since the existing is only null when authorizer is not managed
                return FlowInheritability.notInheritable("Proposed Authorizations do not match current Authorizations and are not configured with an internal Authorizer");
            }
        }

        // both are internal and equal
        return FlowInheritability.notInheritable(null);
    }
}
