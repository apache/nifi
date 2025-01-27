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
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.PropertyConfigurationMapper;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.web.ResourceNotFoundException;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
        authorizable.getPropertyDescriptors().forEach(descriptor -> {
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
                    } catch (ResourceNotFoundException ignored) {
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
                    final String proposedValue = entry.getValue();

                    String proposedEffectiveValue = new PropertyConfigurationMapper()
                        .mapRawPropertyValuesToPropertyConfiguration(propertyDescriptor, proposedValue)
                        .getEffectiveValue(authorizable.getParameterContext());

                    final String currentValue = authorizable.getValue(propertyDescriptor);

                    // if the value is changing
                    if (!Objects.equals(currentValue, proposedValue)) {
                        // ensure access to the old service
                        if (currentValue != null) {
                            try {
                                final Authorizable currentServiceAuthorizable = lookup.getControllerService(currentValue).getAuthorizable();
                                currentServiceAuthorizable.authorize(authorizer, RequestAction.READ, user);
                            } catch (ResourceNotFoundException ignored) {
                                // ignore if the resource is not found, if currentValue was previously deleted, it should not stop assignment of proposedValue
                            }
                        }

                        // ensure access to the new service
                        if (proposedValue != null) {
                            final ParameterParser parser = new ExpressionLanguageAgnosticParameterParser();
                            final ParameterTokenList tokenList = parser.parseTokens(proposedValue);
                            final boolean referencesParameter = !tokenList.toReferenceList().isEmpty();
                            if (referencesParameter) {
                                authorizeControllerServiceReference(authorizable, authorizer, lookup, user, propertyDescriptor, proposedEffectiveValue);
                            } else {
                                final Authorizable newServiceAuthorizable = lookup.getControllerService(proposedValue).getAuthorizable();
                                newServiceAuthorizable.authorize(authorizer, RequestAction.READ, user);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Authorizes a proposed new controller service reference.
     *
     * @param authorizable authorizable that may reference a controller service
     * @param authorizer authorizer
     * @param lookup lookup
     * @param user user
     * @param propertyDescriptor the propertyDescriptor referencing a controller service
     * @param proposedEffectiveValue the new proposed value (id of the controller service) to use as a reference
     */
    public static void authorizeControllerServiceReference(
        ComponentAuthorizable authorizable,
        Authorizer authorizer,
        AuthorizableLookup lookup,
        NiFiUser user,
        PropertyDescriptor propertyDescriptor,
        String proposedEffectiveValue
    ) {
        final String currentValue = authorizable.getValue(propertyDescriptor);

        if (authorizable.getAuthorizable() instanceof ComponentNode) {
            authorize(authorizer, lookup, user, currentValue);
            authorize(authorizer, lookup, user, proposedEffectiveValue);
        } else {
            throw new IllegalArgumentException(authorizable.getAuthorizable().getResource().getSafeDescription() + " cannot reference Controller Services through Parameters.");
        }
    }

    private static void authorize(Authorizer authorizer, AuthorizableLookup lookup, NiFiUser user, String serviceId) {
        Optional.ofNullable(serviceId).map(lookup::getControllerService).ifPresent(service -> {
            Authorizable serviceAuthorizable = service.getAuthorizable();

            serviceAuthorizable.authorize(authorizer, RequestAction.READ, user);
        });
    }

    /**
     * Unresolved Controller Services may be unresolved because
     * - The identifier matches an existing Controller Service id (unresolved because the proposed id was unchanged from resolved id)
     * - An applicable Controller Service does not exist
     * - The user lacks permission to an applicable Controller Service
     *
     * If any of those unresolved Controller Services do match a local Controller Service we need to authorize access to it.
     *
     * @param groupId the group id
     * @param unresolvedControllerServices the unresolved Controller Services
     * @param authorizer the Authorizer
     * @param lookup the AuthorizableLookup
     * @param user the current user
     */
    public static void authorizeUnresolvedControllerServiceReferences(final String groupId, final Set<String> unresolvedControllerServices,
                                                                      final Authorizer authorizer, final AuthorizableLookup lookup, final NiFiUser user) {

        // if there are no unresolved Controller Services we can return
        if (unresolvedControllerServices.isEmpty()) {
            return;
        }

        lookup.getControllerServices(groupId, cs -> {
            // if the unresolved controller service directly contains the id of a locally available service, include it for authorization
            final boolean containsId = unresolvedControllerServices.contains(cs.getIdentifier());
            if (containsId) {
                return true;
            }

            // if the unresolved controller service contains the versioned id of a locally available service, include it for authorization
            final Optional<String> versionedId = cs.getVersionedComponentId();
            return versionedId.filter(unresolvedControllerServices::contains).isPresent();
        })
        .forEach(ca -> ca.getAuthorizable().authorize(authorizer, RequestAction.READ, user));
    }
}
