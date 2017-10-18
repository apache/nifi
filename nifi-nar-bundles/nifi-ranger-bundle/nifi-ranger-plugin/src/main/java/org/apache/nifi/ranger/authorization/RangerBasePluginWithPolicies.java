/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.ranger.authorization;

import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.util.StringUtils;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Extends the base plugin to convert service policies into NiFi policy domain model.
 */
public class RangerBasePluginWithPolicies extends RangerBasePlugin {

    private static final Logger logger = LoggerFactory.getLogger(RangerBasePluginWithPolicies.class);

    private UserGroupProvider userGroupProvider;
    private AtomicReference<PolicyLookup> policies = new AtomicReference<>(new PolicyLookup());

    public RangerBasePluginWithPolicies(final String serviceType, final String appId) {
        this(serviceType, appId, null);
    }

    public RangerBasePluginWithPolicies(final String serviceType, final String appId, final UserGroupProvider userGroupProvider) {
        super(serviceType, appId);
        this.userGroupProvider = userGroupProvider; // will be null if used outside of the ManagedRangerAuthorizer
    }

    @Override
    public void setPolicies(final ServicePolicies policies) {
        super.setPolicies(policies);

        if (policies == null || policies.getPolicies() == null) {
            this.policies.set(new PolicyLookup());
        } else {
            this.policies.set(createPolicyLookup(policies));
        }
    }

    /**
     * Determines if a policy exists for the given resource.
     *
     * @param resourceIdentifier the id of the resource
     *
     * @return true if a policy exists for the given resource, false otherwise
     */
    public boolean doesPolicyExist(final String resourceIdentifier, final RequestAction requestAction) {
        if (resourceIdentifier == null) {
            return false;
        }

        final PolicyLookup policyLookup = policies.get();
        return policyLookup.getAccessPolicy(resourceIdentifier, requestAction) != null;
    }

    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return policies.get().getAccessPolicies();
    }

    public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
        return policies.get().getAccessPolicy(identifier);
    }

    public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
        return policies.get().getAccessPolicy(resourceIdentifier, action);
    }

    private PolicyLookup createPolicyLookup(final ServicePolicies servicePolicies) {
        final Map<String, AccessPolicy> policiesByIdentifier = new HashMap<>();
        final Map<String, Map<RequestAction, AccessPolicy>> policiesByResource = new HashMap<>();

        logger.info("Converting Ranger ServicePolicies model into NiFi policy model for viewing purposes in NiFi UI.");

        servicePolicies.getPolicies().stream().forEach(policy -> {
            // only consider policies that are enabled
            if (Boolean.TRUE.equals(policy.getIsEnabled())) {
                // get all the resources for this policy - excludes/recursive support disabled
                final Set<String> resources = policy.getResources().values().stream()
                        .filter(resource -> {
                            final boolean isExclude = Boolean.TRUE.equals(resource.getIsExcludes());
                            final boolean isRecursive =  Boolean.TRUE.equals(resource.getIsRecursive());

                            if (isExclude) {
                                logger.warn(String.format("Resources [%s] marked as an exclude policy. Skipping policy for viewing purposes. "
                                        + "Will still be used for access decisions.", StringUtils.join(resource.getValues(), ", ")));
                            }
                            if (isRecursive) {
                                logger.warn(String.format("Resources [%s] marked as a recursive policy. Skipping policy for viewing purposes. "
                                        + "Will still be used for access decisions.", StringUtils.join(resource.getValues(), ", ")));
                            }

                            return !isExclude && !isRecursive;
                        })
                        .flatMap(resource -> resource.getValues().stream())
                        .collect(Collectors.toSet());

                policy.getPolicyItems().forEach(policyItem -> {
                    // get all the users for this policy item, excluding unknown users
                    final Set<String> userIds = policyItem.getUsers().stream()
                            .map(userIdentity -> getUser(userIdentity))
                            .filter(Objects::nonNull)
                            .map(user -> user.getIdentifier())
                            .collect(Collectors.toSet());

                    // get all groups for this policy item, excluding unknown groups
                    final Set<String> groupIds = policyItem.getGroups().stream()
                            .map(groupName -> getGroup(groupName))
                            .filter(Objects::nonNull)
                            .map(group -> group.getIdentifier())
                            .collect(Collectors.toSet());

                    // check if this policy item is a delegate admin
                    final boolean isDelegateAdmin = Boolean.TRUE.equals(policyItem.getDelegateAdmin());

                    policyItem.getAccesses().forEach(access -> {
                        try {
                            // interpret the request action
                            final RequestAction action = RequestAction.valueOf(access.getType());

                            // function for creating an access policy
                            final Function<String, AccessPolicy> createPolicy = resource -> new AccessPolicy.Builder()
                                    .identifierGenerateFromSeed(resource + access.getType())
                                    .resource(resource)
                                    .action(action)
                                    .addUsers(userIds)
                                    .addGroups(groupIds)
                                    .build();

                            resources.forEach(resource -> {
                                // create the access policy for the specified resource
                                final AccessPolicy accessPolicy = createPolicy.apply(resource);
                                policiesByIdentifier.put(accessPolicy.getIdentifier(), accessPolicy);
                                policiesByResource.computeIfAbsent(resource, r -> new HashMap<>()).put(action, accessPolicy);

                                // if this is a delegate admin, also create the admin policy for the specified resource
                                if (isDelegateAdmin) {
                                    //  build the admin resource identifier
                                    final String adminResource;
                                    if (resource.startsWith("/")) {
                                        adminResource = "/policies" + resource;
                                    } else {
                                        adminResource = "/policies/" + resource;
                                    }

                                    final AccessPolicy adminAccessPolicy = createPolicy.apply(adminResource);
                                    policiesByIdentifier.put(adminAccessPolicy.getIdentifier(), adminAccessPolicy);
                                    policiesByResource.computeIfAbsent(adminResource, ar -> new HashMap<>()).put(action, adminAccessPolicy);
                                }
                            });
                        } catch (final IllegalArgumentException e) {
                            logger.warn(String.format("Unrecognized request action '%s'. Skipping policy for viewing purposes. Will still be used for access decisions.", access.getType()));
                        }
                    });
                });
            }
        });

        return new PolicyLookup(policiesByIdentifier, policiesByResource);
    }

    private User getUser(final String identity) {
        if (userGroupProvider == null) {
            // generate the user deterministically when running outside of the ManagedRangerAuthorizer
            return new User.Builder().identifierGenerateFromSeed(identity).identity(identity).build();
        } else {
            // find the user in question
            final User user = userGroupProvider.getUserByIdentity(identity);

            if (user == null) {
                logger.warn(String.format("Cannot find user '%s' in the configured User Group Provider. Skipping user for viewing purposes. Will still be used for access decisions.", identity));
            }

            return user;
        }
    }

    private Group getGroup(final String name) {
        if (userGroupProvider == null) {
            // generate the group deterministically when running outside of the ManagedRangerAuthorizer
            return new Group.Builder().identifierGenerateFromSeed(name).name(name).build();
        } else {
            // find the group in question
            final Group group = userGroupProvider.getGroups().stream().filter(g -> g.getName().equals(name)).findFirst().orElse(null);

            if (group == null) {
                logger.warn(String.format("Cannot find group '%s' in the configured User Group Provider. Skipping group for viewing purposes. Will still be used for access decisions.", name));
            }

            return group;
        }
    }

    private static class PolicyLookup {

        private final Map<String, AccessPolicy> policiesByIdentifier;
        private final Map<String, Map<RequestAction, AccessPolicy>> policiesByResource;
        private final Set<AccessPolicy> allPolicies;

        private PolicyLookup() {
            this(null, null);
        }

        private PolicyLookup(final Map<String, AccessPolicy> policiesByIdentifier, final Map<String, Map<RequestAction, AccessPolicy>> policiesByResource) {
            if (policiesByIdentifier == null) {
                allPolicies = Collections.EMPTY_SET;
            } else {
                allPolicies = Collections.unmodifiableSet(new HashSet<>(policiesByIdentifier.values()));
            }

            this.policiesByIdentifier = policiesByIdentifier;
            this.policiesByResource = policiesByResource;
        }

        private Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
            return allPolicies;
        }

        private AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
            if (policiesByIdentifier == null) {
                return null;
            }

            return policiesByIdentifier.get(identifier);
        }

        private AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
            if (policiesByResource == null) {
                return null;
            }

            final Map<RequestAction, AccessPolicy> policiesForResource = policiesByResource.get(resourceIdentifier);

            if (policiesForResource != null) {
                return policiesForResource.get(action);
            }

            return null;
        }
    }

}
