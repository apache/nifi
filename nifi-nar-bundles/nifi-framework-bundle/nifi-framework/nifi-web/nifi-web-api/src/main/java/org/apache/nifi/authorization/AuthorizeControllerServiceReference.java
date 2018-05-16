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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.web.ResourceNotFoundException;

import java.util.Map;
import java.util.Objects;

/**
 * Authorizes references to Controller Services. Utilizes when Processors, Controller Services, and Reporting Tasks are created and updated.
 */
public final class AuthorizeControllerServiceReference {

    /**
     * Authorizes any referenced controller services from the specified configurable component.
     *
     * @param authorizable authorizable that may reference a controller service
     * @param authorizer authorizer
     * @param lookup lookup
     */
    public static void authorizeControllerServiceReferences(final ComponentAuthorizable authorizable, final Authorizer authorizer,
                                                            final AuthorizableLookup lookup, final boolean authorizeTransitiveServices) {

        // consider each property when looking for service references
        authorizable.getPropertyDescriptors().stream().forEach(descriptor -> {
            // if this descriptor identifies a controller service
            if (descriptor.getControllerServiceDefinition() != null) {
                // get the service id
                final String serviceId = authorizable.getValue(descriptor);

                // authorize the service if configured
                if (serviceId != null) {
                    try {
                        final ComponentAuthorizable currentServiceAuthorizable = lookup.getControllerService(serviceId);
                        currentServiceAuthorizable.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());

                        if (authorizeTransitiveServices) {
                            authorizeControllerServiceReferences(currentServiceAuthorizable, authorizer, lookup, authorizeTransitiveServices);
                        }
                    } catch (ResourceNotFoundException e) {
                        // ignore if the resource is not found, if the referenced service was previously deleted, it should not stop this action
                    }
                }
            }
        });
    }

    /**
     * Authorizes the proposed properties for the specified authorizable.
     *
     * @param proposedProperties proposed properties
     * @param authorizable authorizable that may reference a controller service
     * @param authorizer authorizer
     * @param lookup lookup
     */
    public static void authorizeControllerServiceReferences(final Map<String, String> proposedProperties, final ComponentAuthorizable authorizable,
                                                            final Authorizer authorizer, final AuthorizableLookup lookup) {

        // only attempt to authorize if properties are changing
        if (proposedProperties != null) {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();

            for (final Map.Entry<String, String> entry : proposedProperties.entrySet()) {
                final String propertyName = entry.getKey();
                final PropertyDescriptor propertyDescriptor = authorizable.getPropertyDescriptor(propertyName);

                // if this descriptor identifies a controller service
                if (propertyDescriptor.getControllerServiceDefinition() != null) {
                    final String currentValue = authorizable.getValue(propertyDescriptor);
                    final String proposedValue = entry.getValue();

                    // if the value is changing
                    if (!Objects.equals(currentValue, proposedValue)) {
                        // ensure access to the old service
                        if (currentValue != null) {
                            try {
                                final Authorizable currentServiceAuthorizable = lookup.getControllerService(currentValue).getAuthorizable();
                                currentServiceAuthorizable.authorize(authorizer, RequestAction.READ, user);
                            } catch (ResourceNotFoundException e) {
                                // ignore if the resource is not found, if currentValue was previously deleted, it should not stop assignment of proposedValue
                            }
                        }

                        // ensure access to the new service
                        if (proposedValue != null) {
                            final Authorizable newServiceAuthorizable = lookup.getControllerService(proposedValue).getAuthorizable();
                            newServiceAuthorizable.authorize(authorizer, RequestAction.READ, user);
                        }
                    }
                }
            }
        }
    }
}
