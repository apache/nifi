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

import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mock implementation of AbstractPolicyBasedAuthorizer.
 */
public class MockPolicyBasedAuthorizer extends AbstractPolicyBasedAuthorizer implements AuthorizationAuditor {

    private Set<Group> groups = new HashSet<>();
    private Set<User> users = new HashSet<>();
    private Set<AccessPolicy> policies = new HashSet<>();

    private Set<AuthorizationRequest> audited = new HashSet<>();

    public MockPolicyBasedAuthorizer() {

    }

    public MockPolicyBasedAuthorizer(final Set<Group> groups, final Set<User> users, final Set<AccessPolicy> policies) {
        if (groups != null) {
            this.groups.addAll(groups);
        }
        if (users != null) {
            this.users.addAll(users);
        }
        if (policies != null) {
            this.policies.addAll(policies);
        }
    }

    @Override
    public Group doAddGroup(final Group group) throws AuthorizationAccessException {
        groups.add(group);
        return group;
    }

    @Override
    public Group getGroup(final String identifier) throws AuthorizationAccessException {
        return groups.stream().filter(g -> g.getIdentifier().equals(identifier)).findFirst().get();
    }

    @Override
    public Group getGroupByName(final String name) throws AuthorizationAccessException {
        return groups.stream().filter(g -> g.getName().equals(name)).findFirst().get();
    }

    @Override
    public Group doUpdateGroup(final Group group) throws AuthorizationAccessException {
        deleteGroup(group);
        return addGroup(group);
    }

    @Override
    public Group deleteGroup(final Group group) throws AuthorizationAccessException {
        groups.remove(group);
        return group;
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return groups;
    }

    @Override
    public User doAddUser(final User user) throws AuthorizationAccessException {
        users.add(user);
        return user;
    }

    @Override
    public User getUser(final String identifier) throws AuthorizationAccessException {
        return users.stream().filter(u -> u.getIdentifier().equals(identifier)).findFirst().get();
    }

    @Override
    public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
        return users.stream().filter(u -> u.getIdentity().equals(identity)).findFirst().get();
    }

    @Override
    public User doUpdateUser(final User user) throws AuthorizationAccessException {
        deleteUser(user);
        return addUser(user);
    }

    @Override
    public User deleteUser(final User user) throws AuthorizationAccessException {
        users.remove(user);
        return user;
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return users;
    }

    @Override
    protected AccessPolicy doAddAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        policies.add(accessPolicy);
        return accessPolicy;
    }

    @Override
    public AccessPolicy getAccessPolicy(final String identifier) throws AuthorizationAccessException {
        return policies.stream().filter(p -> p.getIdentifier().equals(identifier)).findFirst().get();
    }

    @Override
    public AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        deleteAccessPolicy(accessPolicy);
        return addAccessPolicy(accessPolicy);
    }

    @Override
    public AccessPolicy deleteAccessPolicy(final AccessPolicy policy) throws AuthorizationAccessException {
        policies.remove(policy);
        return policy;
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return policies;
    }

    @Override
    public UsersAndAccessPolicies getUsersAndAccessPolicies() throws AuthorizationAccessException {
        return new UsersAndAccessPolicies() {
            @Override
            public AccessPolicy getAccessPolicy(final String resourceIdentifier, final RequestAction action) {
                return policies.stream()
                        .filter(policy -> policy.getResource().equals(resourceIdentifier) && policy.getAction().equals(action))
                        .findFirst().orElse(null);
            }

            @Override
            public User getUser(final String identity) {
                return getUserByIdentity(identity);
            }

            @Override
            public Set<Group> getGroups(final String userIdentity) {
                final User user = getUserByIdentity(userIdentity);
                if (user == null) {
                    return new HashSet<>();
                } else {
                    return groups.stream()
                            .filter(g -> g.getUsers().contains(user.getIdentifier()))
                            .collect(Collectors.toSet());
                }
            }
        };
    }

    @Override
    public void auditAccessAttempt(final AuthorizationRequest request, final AuthorizationResult result) {
        audited.add(request);
    }

    public boolean isAudited(final AuthorizationRequest request) {
        return audited.contains(request);
    }

    @Override
    public void initialize(final AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {

    }

    @Override
    public void doOnConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {

    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {

    }

    @Override
    public void backupPoliciesUsersAndGroups() {
    }

    @Override
    public void purgePoliciesUsersAndGroups() {
        groups.clear();
        users.clear();
        policies.clear();
    }
}
