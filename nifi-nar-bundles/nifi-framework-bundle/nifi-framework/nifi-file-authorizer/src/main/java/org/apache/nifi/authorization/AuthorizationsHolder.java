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


import org.apache.nifi.authorization.file.generated.Authorizations;
import org.apache.nifi.authorization.file.generated.Policies;
import org.apache.nifi.authorization.file.tenants.generated.Groups;
import org.apache.nifi.authorization.file.tenants.generated.Tenants;
import org.apache.nifi.authorization.file.tenants.generated.Users;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A holder to provide atomic access to data structures.
 */
public class AuthorizationsHolder implements UsersAndAccessPolicies {

    private final Tenants tenants;
    private final Authorizations authorizations;

    private final Set<AccessPolicy> allPolicies;
    private final Map<String, Set<AccessPolicy>> policiesByResource;
    private final Map<String, AccessPolicy> policiesById;

    private final Set<User> allUsers;
    private final Map<String,User> usersById;
    private final Map<String,User> usersByIdentity;

    private final Set<Group> allGroups;
    private final Map<String,Group> groupsById;
    private final Map<String, Set<Group>> groupsByUserIdentity;

    /**
     * Creates a new holder and populates all convenience data structures.
     *
     * @param authorizations the current authorizations instance
     */
    public AuthorizationsHolder(final Authorizations authorizations, final Tenants tenants) {
        this.authorizations = authorizations;
        this.tenants = tenants;

        // load all users
        final Users users = tenants.getUsers();
        final Set<User> allUsers = Collections.unmodifiableSet(createUsers(users));

        // load all groups
        final Groups groups = tenants.getGroups();
        final Set<Group> allGroups = Collections.unmodifiableSet(createGroups(groups, users));

        // load all access policies
        final Policies policies = authorizations.getPolicies();
        final Set<AccessPolicy> allPolicies = Collections.unmodifiableSet(createAccessPolicies(policies));

        // create a convenience map to retrieve a user by id
        final Map<String, User> userByIdMap = Collections.unmodifiableMap(createUserByIdMap(allUsers));

        // create a convenience map to retrieve a user by identity
        final Map<String, User> userByIdentityMap = Collections.unmodifiableMap(createUserByIdentityMap(allUsers));

        // create a convenience map to retrieve a group by id
        final Map<String, Group> groupByIdMap = Collections.unmodifiableMap(createGroupByIdMap(allGroups));

        // create a convenience map to retrieve the groups for a user identity
        final Map<String, Set<Group>> groupsByUserIdentityMap = Collections.unmodifiableMap(createGroupsByUserIdentityMap(allGroups, allUsers));

        // create a convenience map from resource id to policies
        final Map<String, Set<AccessPolicy>> policiesByResourceMap = Collections.unmodifiableMap(createResourcePolicyMap(allPolicies));

        // create a convenience map from policy id to policy
        final Map<String, AccessPolicy> policiesByIdMap = Collections.unmodifiableMap(createPoliciesByIdMap(allPolicies));

        // set all the holders
        this.allUsers = allUsers;
        this.allGroups = allGroups;
        this.allPolicies = allPolicies;
        this.usersById = userByIdMap;
        this.usersByIdentity = userByIdentityMap;
        this.groupsById = groupByIdMap;
        this.groupsByUserIdentity = groupsByUserIdentityMap;
        this.policiesByResource = policiesByResourceMap;
        this.policiesById = policiesByIdMap;
    }

    /**
     * Creates AccessPolicies from the JAXB Policies.
     *
     * @param policies the JAXB Policies element
     * @return a set of AccessPolicies corresponding to the provided Resources
     */
    private Set<AccessPolicy> createAccessPolicies(org.apache.nifi.authorization.file.generated.Policies policies) {
        Set<AccessPolicy> allPolicies = new HashSet<>();
        if (policies == null || policies.getPolicy() == null) {
            return allPolicies;
        }

        // load the new authorizations
        for (final org.apache.nifi.authorization.file.generated.Policy policy : policies.getPolicy()) {
            final String policyIdentifier = policy.getIdentifier();
            final String resourceIdentifier = policy.getResource();

            // start a new builder and set the policy and resource identifiers
            final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                    .identifier(policyIdentifier)
                    .resource(resourceIdentifier);

            // add each user identifier
            for (org.apache.nifi.authorization.file.generated.Policy.User user : policy.getUser()) {
                builder.addUser(user.getIdentifier());
            }

            // add each group identifier
            for (org.apache.nifi.authorization.file.generated.Policy.Group group : policy.getGroup()) {
                builder.addGroup(group.getIdentifier());
            }

            // add the appropriate request actions
            final String authorizationCode = policy.getAction();
            if (authorizationCode.equals(FileAuthorizer.READ_CODE)) {
                builder.action(RequestAction.READ);
            } else if (authorizationCode.equals(FileAuthorizer.WRITE_CODE)){
                builder.action(RequestAction.WRITE);
            } else {
                throw new IllegalStateException("Unknown Policy Action: " + authorizationCode);
            }

            // build the policy and add it to the map
            allPolicies.add(builder.build());
        }

        return allPolicies;
    }

    /**
     * Creates a set of Users from the JAXB Users.
     *
     * @param users the JAXB Users
     * @return a set of API Users matching the provided JAXB Users
     */
    private Set<User> createUsers(org.apache.nifi.authorization.file.tenants.generated.Users users) {
        Set<User> allUsers = new HashSet<>();
        if (users == null || users.getUser() == null) {
            return allUsers;
        }

        for (org.apache.nifi.authorization.file.tenants.generated.User user : users.getUser()) {
            final User.Builder builder = new User.Builder()
                    .identity(user.getIdentity())
                    .identifier(user.getIdentifier());

            allUsers.add(builder.build());
        }

        return allUsers;
    }

    /**
     * Creates a set of Groups from the JAXB Groups.
     *
     * @param groups the JAXB Groups
     * @return a set of API Groups matching the provided JAXB Groups
     */
    private Set<Group> createGroups(org.apache.nifi.authorization.file.tenants.generated.Groups groups,
                                    org.apache.nifi.authorization.file.tenants.generated.Users users) {
        Set<Group> allGroups = new HashSet<>();
        if (groups == null || groups.getGroup() == null) {
            return allGroups;
        }

        for (org.apache.nifi.authorization.file.tenants.generated.Group group : groups.getGroup()) {
            final Group.Builder builder = new Group.Builder()
                    .identifier(group.getIdentifier())
                    .name(group.getName());

            for (org.apache.nifi.authorization.file.tenants.generated.Group.User groupUser : group.getUser()) {
                builder.addUser(groupUser.getIdentifier());
            }

            allGroups.add(builder.build());
        }

        return allGroups;
    }

    /**
     * Creates a map from resource identifier to the set of policies for the given resource.
     *
     * @param allPolicies the set of all policies
     * @return a map from resource identifier to policies
     */
    private Map<String, Set<AccessPolicy>> createResourcePolicyMap(final Set<AccessPolicy> allPolicies) {
        Map<String, Set<AccessPolicy>> resourcePolicies = new HashMap<>();

        for (AccessPolicy policy : allPolicies) {
            Set<AccessPolicy> policies = resourcePolicies.get(policy.getResource());
            if (policies == null) {
                policies = new HashSet<>();
                resourcePolicies.put(policy.getResource(), policies);
            }
            policies.add(policy);
        }

        return resourcePolicies;
    }

    /**
     * Creates a Map from user identifier to User.
     *
     * @param users the set of all users
     * @return the Map from user identifier to User
     */
    private Map<String,User> createUserByIdMap(final Set<User> users) {
        Map<String,User> usersMap = new HashMap<>();
        for (User user : users) {
            usersMap.put(user.getIdentifier(), user);
        }
        return usersMap;
    }

    /**
     * Creates a Map from user identity to User.
     *
     * @param users the set of all users
     * @return the Map from user identity to User
     */
    private Map<String,User> createUserByIdentityMap(final Set<User> users) {
        Map<String,User> usersMap = new HashMap<>();
        for (User user : users) {
            usersMap.put(user.getIdentity(), user);
        }
        return usersMap;
    }

    /**
     * Creates a Map from group identifier to Group.
     *
     * @param groups the set of all groups
     * @return the Map from group identifier to Group
     */
    private Map<String,Group> createGroupByIdMap(final Set<Group> groups) {
        Map<String,Group> groupsMap = new HashMap<>();
        for (Group group : groups) {
            groupsMap.put(group.getIdentifier(), group);
        }
        return groupsMap;
    }

    /**
     * Creates a Map from user identity to the set of Groups for that identity.
     *
     * @param groups all groups
     * @param users all users
     * @return a Map from User identity to the set of Groups for that identity
     */
    private Map<String, Set<Group>> createGroupsByUserIdentityMap(final Set<Group> groups, final Set<User> users) {
        Map<String, Set<Group>> groupsByUserIdentity = new HashMap<>();

        for (User user : users) {
            Set<Group> userGroups = new HashSet<>();
            for (Group group : groups) {
                for (String groupUser : group.getUsers()) {
                    if (groupUser.equals(user.getIdentifier())) {
                        userGroups.add(group);
                    }
                }
            }

            groupsByUserIdentity.put(user.getIdentity(), userGroups);
        }

        return groupsByUserIdentity;
    }

    /**
     * Creates a Map from policy identifier to AccessPolicy.
     *
     * @param policies the set of all access policies
     * @return the Map from policy identifier to AccessPolicy
     */
    private Map<String, AccessPolicy> createPoliciesByIdMap(final Set<AccessPolicy> policies) {
        Map<String,AccessPolicy> policyMap = new HashMap<>();
        for (AccessPolicy policy : policies) {
            policyMap.put(policy.getIdentifier(), policy);
        }
        return policyMap;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public Tenants getTenants() {
        return tenants;
    }

    public Set<AccessPolicy> getAllPolicies() {
        return allPolicies;
    }

    public Map<String, Set<AccessPolicy>> getPoliciesByResource() {
        return policiesByResource;
    }

    public Map<String, AccessPolicy> getPoliciesById() {
        return policiesById;
    }

    public Set<User> getAllUsers() {
        return allUsers;
    }

    public Map<String, User> getUsersById() {
        return usersById;
    }

    public Map<String, User> getUsersByIdentity() {
        return usersByIdentity;
    }

    public Set<Group> getAllGroups() {
        return allGroups;
    }

    public Map<String, Group> getGroupsById() {
        return groupsById;
    }

    @Override
    public AccessPolicy getAccessPolicy(final String resourceIdentifier, final RequestAction action) {
        if (resourceIdentifier == null) {
            throw new IllegalArgumentException("Resource Identifier cannot be null");
        }

        final Set<AccessPolicy> resourcePolicies = policiesByResource.get(resourceIdentifier);
        if (resourcePolicies == null) {
            return null;
        }

        for (AccessPolicy accessPolicy : resourcePolicies) {
            if (accessPolicy.getAction() == action) {
                return accessPolicy;
            }
        }

        return null;
    }

    @Override
    public User getUser(String identity) {
        if (identity == null) {
            throw new IllegalArgumentException("Identity cannot be null");
        }
        return usersByIdentity.get(identity);
    }

    @Override
    public Set<Group> getGroups(String userIdentity) {
        if (userIdentity == null) {
            throw new IllegalArgumentException("User Identity cannot be null");
        }
        return groupsByUserIdentity.get(userIdentity);
    }

}
