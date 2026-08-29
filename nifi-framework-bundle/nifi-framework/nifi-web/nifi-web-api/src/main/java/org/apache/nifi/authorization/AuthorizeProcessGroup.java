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
import org.apache.nifi.authorization.user.NiFiUserUtils;

import java.util.function.Consumer;

/**
 * Authorizes a Process Group along with encapsulated and referenced Components
 */
public final class AuthorizeProcessGroup {
    /**
     * Authorizes the specified Process Group and referenced Components
     *
     * @param processGroupAuthorizable process group
     * @param authorizer authorizer
     * @param lookup lookup
     * @param action action
     * @param authorizeReferencedServices whether to authorize referenced services
     * @param authorizeControllerServices whether to authorize controller services
     * @param authorizeTransitiveServices whether to authorize transitive services
     * @param authorizeParameterReferences whether to authorize parameter context that contained referenced parameter if applicable
     * @param authorizeParameterContext whether to authorize the bound parameter context if applicable
     */
    public static void authorizeProcessGroup(
            final ProcessGroupAuthorizable processGroupAuthorizable,
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final RequestAction action,
            final boolean authorizeReferencedServices,
            final boolean authorizeControllerServices,
            final boolean authorizeTransitiveServices,
            final boolean authorizeParameterReferences,
            final boolean authorizeParameterContext
    ) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Consumer<Authorizable> authorize = authorizable -> authorizable.authorize(authorizer, action, user);


        authorize.accept(processGroupAuthorizable.getAuthorizable());


        if (authorizeParameterContext) {
            processGroupAuthorizable.getParameterContextAuthorizable().ifPresent(authorize);
        }

        processGroupAuthorizable.getEncapsulatedProcessors().forEach(processorAuthorizable -> {
            authorize.accept(processorAuthorizable.getAuthorizable());
            if (authorizeReferencedServices) {
                AuthorizeControllerServiceReference.authorizeControllerServiceReferences(processorAuthorizable, authorizer, lookup, authorizeTransitiveServices);
            }
            if (authorizeParameterReferences) {
                AuthorizeParameterReference.authorizeParameterReferences(processorAuthorizable, authorizer, processorAuthorizable.getParameterContext(), user);
            }
        });
        processGroupAuthorizable.getEncapsulatedConnections().stream().map(AuthorizableHolder::getAuthorizable).forEach(authorize);
        processGroupAuthorizable.getEncapsulatedInputPorts().forEach(authorize);
        processGroupAuthorizable.getEncapsulatedOutputPorts().forEach(authorize);
        processGroupAuthorizable.getEncapsulatedFunnels().forEach(authorize);
        processGroupAuthorizable.getEncapsulatedLabels().forEach(authorize);
        processGroupAuthorizable.getEncapsulatedProcessGroups().forEach(pga -> {
            final Authorizable authorizable = pga.getAuthorizable();

            authorize.accept(authorizable);

            if (authorizeParameterContext) {
                pga.getParameterContextAuthorizable().ifPresent(authorize);
            }
        });
        processGroupAuthorizable.getEncapsulatedRemoteProcessGroups().forEach(authorize);

        if (authorizeControllerServices) {
            processGroupAuthorizable.getEncapsulatedControllerServices().forEach(controllerServiceAuthorizable -> {
                authorize.accept(controllerServiceAuthorizable.getAuthorizable());
                if (authorizeReferencedServices) {
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(controllerServiceAuthorizable, authorizer, lookup, authorizeTransitiveServices);
                }
                if (authorizeParameterReferences) {
                    AuthorizeParameterReference.authorizeParameterReferences(controllerServiceAuthorizable, authorizer, controllerServiceAuthorizable.getParameterContext(), user);
                }
            });
        }
    }
}
