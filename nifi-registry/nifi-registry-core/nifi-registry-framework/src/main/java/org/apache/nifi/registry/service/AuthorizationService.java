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
package org.apache.nifi.registry.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.authorization.AccessPolicySummary;
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.authorization.Permissions;
import org.apache.nifi.registry.authorization.Resource;
import org.apache.nifi.registry.authorization.ResourcePermissions;
import org.apache.nifi.registry.authorization.Tenant;
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.exception.ResourceNotFoundException;
import org.apache.nifi.registry.security.authorization.AccessPolicyProvider;
import org.apache.nifi.registry.security.authorization.AccessPolicyProviderInitializationContext;
import org.apache.nifi.registry.security.authorization.AuthorizableLookup;
import org.apache.nifi.registry.security.authorization.Authorizer;
import org.apache.nifi.registry.security.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.ConfigurableAccessPolicyProvider;
import org.apache.nifi.registry.security.authorization.ConfigurableUserGroupProvider;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.ManagedAuthorizer;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.UntrustedProxyException;
import org.apache.nifi.registry.security.authorization.UserAndGroups;
import org.apache.nifi.registry.security.authorization.UserGroupProvider;
import org.apache.nifi.registry.security.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.registry.security.authorization.exception.AccessDeniedException;
import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.authorization.resource.Authorizable;
import org.apache.nifi.registry.security.authorization.resource.ResourceFactory;
import org.apache.nifi.registry.security.authorization.resource.ResourceType;
import org.apache.nifi.registry.security.authorization.user.NiFiUser;
import org.apache.nifi.registry.security.authorization.user.NiFiUserUtils;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service for performing operations on users, groups, and policies.
 */
@Service
public class AuthorizationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationService.class);

    public static final String MSG_NON_MANAGED_AUTHORIZER = "This NiFi Registry is not configured to internally manage users, groups, or policies. Please contact your system administrator.";
    public static final String MSG_NON_CONFIGURABLE_POLICIES = "This NiFi Registry is not configured to allow configurable policies. Please contact your system administrator.";
    public static final String MSG_NON_CONFIGURABLE_USERS = "This NiFi Registry is not configured to allow configurable users and groups. Please contact your system administrator.";

    private AuthorizableLookup authorizableLookup;
    private Authorizer authorizer;
    private RegistryService registryService;
    private UserGroupProvider userGroupProvider;
    private AccessPolicyProvider accessPolicyProvider;

    @Autowired
    public AuthorizationService(
            final AuthorizableLookup authorizableLookup,
            final Authorizer authorizer,
            final RegistryService registryService) {
        this.authorizableLookup = authorizableLookup;
        this.authorizer = authorizer;
        this.registryService = registryService;

        if (AuthorizerCapabilityDetection.isManagedAuthorizer(this.authorizer)) {
            this.accessPolicyProvider = ((ManagedAuthorizer) authorizer).getAccessPolicyProvider();
        } else {
            this.accessPolicyProvider = createExceptionThrowingAccessPolicyProvider();
        }
        this.userGroupProvider = accessPolicyProvider.getUserGroupProvider();
    }


    // ---------------------- Authorization methods -------------------------------------

    public AuthorizableLookup getAuthorizableLookup() {
        return authorizableLookup;
    }

    public void authorize(Authorizable authorizable, RequestAction action) throws AccessDeniedException {
        authorizable.authorize(authorizer, action, NiFiUserUtils.getNiFiUser());
    }

    public boolean isManagedAuthorizer() {
        return AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer);
    }

    public boolean isConfigurableUserGroupProvider() {
        return AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer);
    }

    public boolean isConfigurableAccessPolicyProvider() {
        return AuthorizerCapabilityDetection.isConfigurableAccessPolicyProvider(authorizer);
    }

    public void verifyAuthorizerIsManaged() {
        if (!isManagedAuthorizer()) {
            throw new IllegalStateException(AuthorizationService.MSG_NON_MANAGED_AUTHORIZER);
        }
    }

    public void verifyAuthorizerSupportsConfigurablePolicies() {
        if (!isConfigurableAccessPolicyProvider()) {
            verifyAuthorizerIsManaged();
            throw new IllegalStateException(AuthorizationService.MSG_NON_CONFIGURABLE_POLICIES);
        }
    }

    public void verifyAuthorizerSupportsConfigurableUserGroups() {
        if (!isConfigurableUserGroupProvider()) {
            throw new IllegalStateException(AuthorizationService.MSG_NON_CONFIGURABLE_USERS);
        }
    }

    // ---------------------- Permissions methods ---------------------------------------

    public CurrentUser getCurrentUser() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final CurrentUser currentUser = new CurrentUser();
        currentUser.setIdentity(user.getIdentity());
        currentUser.setAnonymous(user.isAnonymous());
        currentUser.setResourcePermissions(getTopLevelPermissions());
        return currentUser;
    }

    public Permissions getPermissionsForResource(Authorizable authorizableResource) {
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Permissions permissions = new Permissions();
        permissions.setCanRead(authorizableResource.isAuthorized(authorizer, RequestAction.READ, user));
        permissions.setCanWrite(authorizableResource.isAuthorized(authorizer, RequestAction.WRITE, user));
        permissions.setCanDelete(authorizableResource.isAuthorized(authorizer, RequestAction.DELETE, user));
        return permissions;
    }

    public Permissions getPermissionsForResource(Authorizable authorizableResource, Permissions knownParentAuthorizablePermissions) {
        if (knownParentAuthorizablePermissions == null) {
            return getPermissionsForResource(authorizableResource);
        }

        final Permissions permissions = new Permissions(knownParentAuthorizablePermissions);
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        if (!permissions.getCanRead()) {
            permissions.setCanRead(authorizableResource.isAuthorized(authorizer, RequestAction.READ, user));
        }

        if (!permissions.getCanWrite()) {
            permissions.setCanWrite(authorizableResource.isAuthorized(authorizer, RequestAction.WRITE, user));
        }

        if (!permissions.getCanDelete()) {
            permissions.setCanDelete(authorizableResource.isAuthorized(authorizer, RequestAction.DELETE, user));
        }

        return permissions;
    }

    private ResourcePermissions getTopLevelPermissions() {

        NiFiUser user = NiFiUserUtils.getNiFiUser();
        ResourcePermissions resourcePermissions = new ResourcePermissions();

        final Permissions bucketsPermissions = getPermissionsForResource(authorizableLookup.getBucketsAuthorizable());
        resourcePermissions.setBuckets(bucketsPermissions);

        final Permissions policiesPermissions = getPermissionsForResource(authorizableLookup.getPoliciesAuthorizable());
        resourcePermissions.setPolicies(policiesPermissions);

        final Permissions tenantsPermissions = getPermissionsForResource(authorizableLookup.getTenantsAuthorizable());
        resourcePermissions.setTenants(tenantsPermissions);

        final Permissions proxyPermissions = getPermissionsForResource(authorizableLookup.getProxyAuthorizable());
        resourcePermissions.setProxy(proxyPermissions);

        return resourcePermissions;
    }

    // ---------------------- User methods ----------------------------------------------

    public User createUser(final User user) {
        verifyUserGroupProviderIsConfigurable();

        if (StringUtils.isBlank(user.getIdentity())) {
            throw new IllegalArgumentException("User identity must be specified when creating a new user.");
        }

        final org.apache.nifi.registry.security.authorization.User createdUser =
                configurableUserGroupProvider().addUser(userFromDTO(user));
        return userToDTO(createdUser);
    }

    public List<User> getUsers() {
        return userGroupProvider.getUsers().stream().map(this::userToDTO).collect(Collectors.toList());
    }

    public User getUser(final String identifier) {
        final org.apache.nifi.registry.security.authorization.User user = userGroupProvider.getUser(identifier);
        if (user == null) {
            LOGGER.warn("The specified user id [{}] does not exist.", identifier);
            throw new ResourceNotFoundException("The specified user ID does not exist in this registry.");
        }

        return userToDTO(user);
    }

    public User getUserByIdentity(final String identity) {
        final org.apache.nifi.registry.security.authorization.User user = userGroupProvider.getUserByIdentity(identity);
        if (user == null) {
            LOGGER.warn("The specified user identity [{}] does not exist.", identity);
            throw new ResourceNotFoundException("The specified user ID does not exist in this registry.");
        }

        return userToDTO(user);
    }

    public void verifyUserExists(final String identifier) {
        final org.apache.nifi.registry.security.authorization.User user = userGroupProvider.getUser(identifier);
        if (user == null) {
            LOGGER.warn("The specified user id [{}] does not exist.", identifier);
            throw new ResourceNotFoundException("The specified user ID does not exist in this registry.");
        }
    }

    public User updateUser(final User user) {
        verifyUserGroupProviderIsConfigurable();

        final org.apache.nifi.registry.security.authorization.User updatedUser =
                configurableUserGroupProvider().updateUser(userFromDTO(user));

        if (updatedUser == null) {
            LOGGER.warn("The specified user id [{}] does not exist.", user.getIdentifier());
            throw new ResourceNotFoundException("The specified user ID does not exist in this registry.");
        }

        return userToDTO(updatedUser);
    }

    public User deleteUser(final String identifier) {
        verifyUserGroupProviderIsConfigurable();

        final org.apache.nifi.registry.security.authorization.User user = userGroupProvider.getUser(identifier);
        if (user == null) {
            LOGGER.warn("The specified user id [{}] does not exist.", identifier);
            throw new ResourceNotFoundException("The specified user ID does not exist in this registry.");
        }

        configurableUserGroupProvider().deleteUser(user);
        return userToDTO(user);
    }

    // ---------------------- User Group methods --------------------------------------

    public UserGroup createUserGroup(final UserGroup userGroup) {
        verifyUserGroupProviderIsConfigurable();

        if (StringUtils.isBlank(userGroup.getIdentity())) {
            throw new IllegalArgumentException("User group identity must be specified when creating a new group.");
        }

        final org.apache.nifi.registry.security.authorization.Group createdGroup =
                configurableUserGroupProvider().addGroup(userGroupFromDTO(userGroup));
        return userGroupToDTO(createdGroup);
    }

    public List<UserGroup> getUserGroups() {
        return userGroupProvider.getGroups().stream().map(this::userGroupToDTO).collect(Collectors.toList());
    }

    public UserGroup getUserGroup(final String identifier) {
        final org.apache.nifi.registry.security.authorization.Group group = userGroupProvider.getGroup(identifier);

        if (group == null) {
            LOGGER.warn("The specified user group id [{}] does not exist.", identifier);
            throw new ResourceNotFoundException("The specified user group ID does not exist in this registry.");
        }

        return userGroupToDTO(group);
    }

    public void verifyUserGroupExists(final String identifier) {
        final org.apache.nifi.registry.security.authorization.Group group = userGroupProvider.getGroup(identifier);
        if (group == null) {
            LOGGER.warn("The specified user group id [{}] does not exist.", identifier);
            throw new ResourceNotFoundException("The specified user group ID does not exist in this registry.");
        }
    }

    public UserGroup updateUserGroup(final UserGroup userGroup) {
        verifyUserGroupProviderIsConfigurable();

        final org.apache.nifi.registry.security.authorization.Group updatedGroup =
                configurableUserGroupProvider().updateGroup(userGroupFromDTO(userGroup));

        if (updatedGroup == null) {
            LOGGER.warn("The specified user group id [{}] does not exist.", userGroup.getIdentifier());
            throw new ResourceNotFoundException("The specified user group ID does not exist in this registry.");
        }

        return userGroupToDTO(updatedGroup);
    }

    public UserGroup deleteUserGroup(final String identifier) {
        verifyUserGroupProviderIsConfigurable();

        final Group group = userGroupProvider.getGroup(identifier);
        if (group == null) {
            LOGGER.warn("The specified user group id [{}] does not exist.", group.getIdentifier());
            throw new ResourceNotFoundException("The specified user group ID does not exist in this registry.");
        }

        configurableUserGroupProvider().deleteGroup(group);
        return userGroupToDTO(group);
    }


    // ---------------------- Access Policy methods ----------------------------------------

    public AccessPolicy createAccessPolicy(final AccessPolicy accessPolicy) {
        verifyAccessPolicyProviderIsConfigurable();

        if (accessPolicy.getResource() == null) {
            throw new IllegalArgumentException("Resource must be specified when creating a new access policy.");
        }

        RequestAction.valueOfValue(accessPolicy.getAction());

        final org.apache.nifi.registry.security.authorization.AccessPolicy createdAccessPolicy =
                configurableAccessPolicyProvider().addAccessPolicy(accessPolicyFromDTO(accessPolicy));
        return accessPolicyToDTO(createdAccessPolicy);
    }

    public AccessPolicy getAccessPolicy(final String identifier) {
        final org.apache.nifi.registry.security.authorization.AccessPolicy accessPolicy =
                accessPolicyProvider.getAccessPolicy(identifier);

        if (accessPolicy == null) {
            LOGGER.warn("The specified access policy id [{}] does not exist.", identifier);
            throw new ResourceNotFoundException("The specified policy does not exist in this registry.");
        }

        return accessPolicyToDTO(accessPolicy);
    }

    public AccessPolicy getAccessPolicy(final String resource, final RequestAction action) {
        final org.apache.nifi.registry.security.authorization.AccessPolicy accessPolicy =
                accessPolicyProvider.getAccessPolicy(resource, action);

        if (accessPolicy == null) {
            throw new ResourceNotFoundException("No policy found for action='" + action + "', resource='" + resource + "'");
        }

        return accessPolicyToDTO(accessPolicy);
    }

    public List<AccessPolicy> getAccessPolicies() {
        return accessPolicyProvider.getAccessPolicies().stream().map(this::accessPolicyToDTO).collect(Collectors.toList());
    }

    public List<AccessPolicySummary> getAccessPolicySummaries() {
        return accessPolicyProvider.getAccessPolicies().stream().map(this::accessPolicyToSummaryDTO).collect(Collectors.toList());
    }

    private List<AccessPolicySummary> getAccessPolicySummariesForUser(String userIdentifier) {
        return accessPolicyProvider.getAccessPolicies().stream()
                .filter(accessPolicy -> {
                    if (accessPolicy.getUsers().contains(userIdentifier)) {
                        return true;
                    }
                    return accessPolicy.getGroups().stream().anyMatch(g -> {
                        final Group group = userGroupProvider.getGroup(g);
                        return group != null && group.getUsers().contains(userIdentifier);
                    });
                })
                .map(this::accessPolicyToSummaryDTO)
                .collect(Collectors.toList());
    }

    private List<AccessPolicySummary> getAccessPolicySummariesForUserGroup(String userGroupIdentifier) {
        return accessPolicyProvider.getAccessPolicies().stream()
                .filter(accessPolicy -> accessPolicy.getGroups().contains(userGroupIdentifier))
                .map(this::accessPolicyToSummaryDTO)
                .collect(Collectors.toList());
    }

    public void verifyAccessPolicyExists(final String identifier) {
        final org.apache.nifi.registry.security.authorization.AccessPolicy accessPolicy =
                accessPolicyProvider.getAccessPolicy(identifier);
        if (accessPolicy == null) {
            LOGGER.warn("The specified access policy id [{}] does not exist.", identifier);
            throw new ResourceNotFoundException("The specified policy does not exist in this registry.");
        }
    }

    public AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) {
        verifyAccessPolicyProviderIsConfigurable();

        // Don't allow changing action or resource of existing policy (should only be adding/removing users/groups)
        final org.apache.nifi.registry.security.authorization.AccessPolicy currentAccessPolicy =
                accessPolicyProvider.getAccessPolicy(accessPolicy.getIdentifier());

        if (currentAccessPolicy == null) {
            LOGGER.warn("The specified access policy id [{}] does not exist.", accessPolicy.getIdentifier());
            throw new ResourceNotFoundException("The specified policy does not exist in this registry.");
        }

        accessPolicy.setResource(currentAccessPolicy.getResource());
        accessPolicy.setAction(currentAccessPolicy.getAction().toString());

        final org.apache.nifi.registry.security.authorization.AccessPolicy updatedAccessPolicy =
                configurableAccessPolicyProvider().updateAccessPolicy(accessPolicyFromDTO(accessPolicy));

        if (updatedAccessPolicy == null) {
            LOGGER.warn("The specified access policy id [{}] does not exist.", accessPolicy.getIdentifier());
            throw new ResourceNotFoundException("The specified policy does not exist in this registry.");
        }

        return accessPolicyToDTO(updatedAccessPolicy);
    }

    public AccessPolicy deleteAccessPolicy(final String identifier) {
        verifyAccessPolicyProviderIsConfigurable();

        final org.apache.nifi.registry.security.authorization.AccessPolicy accessPolicy =
                accessPolicyProvider.getAccessPolicy(identifier);

        if (accessPolicy == null) {
            LOGGER.warn("The specified access policy id [{}] does not exist.", identifier);
            throw new ResourceNotFoundException("The specified policy does not exist in this registry.");
        }

        configurableAccessPolicyProvider().deleteAccessPolicy(accessPolicy);
        return accessPolicyToDTO(accessPolicy);
    }


    // ---------------------- Resource Lookup methods --------------------------------------

    public List<Resource> getResources() {
        final List<Resource> dtoResources =
                getAuthorizableResources()
                        .stream()
                        .map(AuthorizationService::resourceToDTO)
                        .collect(Collectors.toList());
        return dtoResources;
    }

    public List<Resource> getAuthorizedResources(RequestAction actionType) {
        return getAuthorizedResources(actionType, null);
    }

    public List<Resource> getAuthorizedResources(RequestAction actionType, ResourceType resourceType) {
        final List<Resource> authorizedResources =
                getAuthorizableResources(resourceType)
                        .stream()
                        .filter(resource -> {
                            String resourceId = resource.getIdentifier();
                            try {
                                authorizableLookup
                                        .getAuthorizableByResource(resource.getIdentifier())
                                        .authorize(authorizer, actionType, NiFiUserUtils.getNiFiUser());
                                return true;
                            } catch (AccessDeniedException | UntrustedProxyException e) {
                                return false;
                            }
                        })
                        .map(AuthorizationService::resourceToDTO)
                        .collect(Collectors.toList());

        return authorizedResources;
    }

    // ---------------------- Private Helper methods --------------------------------------

    private ConfigurableUserGroupProvider configurableUserGroupProvider() {
        return ((ConfigurableUserGroupProvider) userGroupProvider);
    }

    private ConfigurableAccessPolicyProvider configurableAccessPolicyProvider() {
        return ((ConfigurableAccessPolicyProvider) accessPolicyProvider);
    }

    private void verifyUserGroupProviderIsConfigurable() {
        if (!(userGroupProvider instanceof ConfigurableUserGroupProvider)) {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_USERS);
        }
    }

    private void verifyAccessPolicyProviderIsConfigurable() {
        if (!(accessPolicyProvider instanceof ConfigurableAccessPolicyProvider)) {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_POLICIES);
        }
    }

    private ResourcePermissions getTopLevelPermissions(String tenantIdentifier) {
        ResourcePermissions resourcePermissions = new ResourcePermissions();

        final Permissions bucketsPermissions = getPermissionsForResource(tenantIdentifier, ResourceFactory.getBucketsResource());
        resourcePermissions.setBuckets(bucketsPermissions);

        final Permissions policiesPermissions = getPermissionsForResource(tenantIdentifier, ResourceFactory.getPoliciesResource());
        resourcePermissions.setPolicies(policiesPermissions);

        final Permissions tenantsPermissions = getPermissionsForResource(tenantIdentifier, ResourceFactory.getTenantsResource());
        resourcePermissions.setTenants(tenantsPermissions);

        final Permissions proxyPermissions = getPermissionsForResource(tenantIdentifier, ResourceFactory.getProxyResource());
        resourcePermissions.setProxy(proxyPermissions);

        return resourcePermissions;
    }

    private Permissions getPermissionsForResource(String tenantIdentifier, org.apache.nifi.registry.security.authorization.Resource resource) {

        Permissions permissions = new Permissions();
        permissions.setCanRead(checkTenantBelongsToPolicy(tenantIdentifier, resource, RequestAction.READ));
        permissions.setCanWrite(checkTenantBelongsToPolicy(tenantIdentifier, resource, RequestAction.WRITE));
        permissions.setCanDelete(checkTenantBelongsToPolicy(tenantIdentifier, resource, RequestAction.DELETE));
        return permissions;

    }

    private boolean checkTenantBelongsToPolicy(String tenantIdentifier, org.apache.nifi.registry.security.authorization.Resource resource, RequestAction action) {
        org.apache.nifi.registry.security.authorization.AccessPolicy policy =
                accessPolicyProvider.getAccessPolicy(resource.getIdentifier(), action);

        if (policy == null) {
            return false;
        }

        boolean tenantInPolicy = policy.getUsers().contains(tenantIdentifier) || policy.getGroups().contains(tenantIdentifier);
        return tenantInPolicy;
    }

    private List<org.apache.nifi.registry.security.authorization.Resource> getAuthorizableResources() {
        return getAuthorizableResources(null);
    }

    private List<org.apache.nifi.registry.security.authorization.Resource> getAuthorizableResources(ResourceType includeFilter) {

        final List<org.apache.nifi.registry.security.authorization.Resource> resources = new ArrayList<>();

        if (includeFilter == null || includeFilter.equals(ResourceType.Policy)) {
            resources.add(ResourceFactory.getPoliciesResource());
        }
        if (includeFilter == null || includeFilter.equals(ResourceType.Tenant)) {
            resources.add(ResourceFactory.getTenantsResource());
        }
        if (includeFilter == null || includeFilter.equals(ResourceType.Proxy)) {
            resources.add(ResourceFactory.getProxyResource());
        }
        if (includeFilter == null || includeFilter.equals(ResourceType.Actuator)) {
            resources.add(ResourceFactory.getActuatorResource());
        }
        if (includeFilter == null || includeFilter.equals(ResourceType.Swagger)) {
            resources.add(ResourceFactory.getSwaggerResource());
        }
        if (includeFilter == null || includeFilter.equals(ResourceType.Bucket)) {
            resources.add(ResourceFactory.getBucketsResource());
            // add all buckets
            for (final Bucket bucket : registryService.getBuckets()) {
                resources.add(ResourceFactory.getBucketResource(bucket.getIdentifier(), bucket.getName()));
            }
        }

        return resources;
    }

    private User userToDTO(
            final org.apache.nifi.registry.security.authorization.User user) {
        if (user == null) {
            return null;
        }
        String userIdentifier = user.getIdentifier();

        Collection<Tenant> groupsContainingUser = userGroupProvider.getGroups().stream()
                .filter(group -> group.getUsers().contains(userIdentifier))
                .map(this::tenantToDTO)
                .collect(Collectors.toList());
        Collection<AccessPolicySummary> accessPolicySummaries = getAccessPolicySummariesForUser(userIdentifier);

        User userDTO = new User(user.getIdentifier(), user.getIdentity());
        userDTO.setConfigurable(AuthorizerCapabilityDetection.isUserConfigurable(authorizer, user));
        userDTO.setResourcePermissions(getTopLevelPermissions(userDTO.getIdentifier()));
        userDTO.addUserGroups(groupsContainingUser);
        userDTO.addAccessPolicies(accessPolicySummaries);
        return userDTO;
    }

    private UserGroup userGroupToDTO(
            final org.apache.nifi.registry.security.authorization.Group userGroup) {
        if (userGroup == null) {
            return null;
        }

        Collection<Tenant> userTenants = userGroup.getUsers() != null
                ? userGroup.getUsers().stream().map(this::tenantIdToDTO).filter(Objects::nonNull).collect(Collectors.toSet()) : null;
        Collection<AccessPolicySummary> accessPolicySummaries = getAccessPolicySummariesForUserGroup(userGroup.getIdentifier());

        UserGroup userGroupDTO = new UserGroup(userGroup.getIdentifier(), userGroup.getName());
        userGroupDTO.setConfigurable(AuthorizerCapabilityDetection.isGroupConfigurable(authorizer, userGroup));
        userGroupDTO.setResourcePermissions(getTopLevelPermissions(userGroupDTO.getIdentifier()));
        userGroupDTO.addUsers(userTenants);
        userGroupDTO.addAccessPolicies(accessPolicySummaries);
        return userGroupDTO;
    }

    private AccessPolicy accessPolicyToDTO(
            final org.apache.nifi.registry.security.authorization.AccessPolicy accessPolicy) {
        if (accessPolicy == null) {
            return null;
        }

        Collection<Tenant> users = accessPolicy.getUsers() != null
                ? accessPolicy.getUsers().stream().map(this::tenantIdToDTO).filter(Objects::nonNull).collect(Collectors.toList()) : null;
        Collection<Tenant> userGroups = accessPolicy.getGroups() != null
                ? accessPolicy.getGroups().stream().map(this::tenantIdToDTO).filter(Objects::nonNull).collect(Collectors.toList()) : null;

        Boolean isConfigurable = AuthorizerCapabilityDetection.isAccessPolicyConfigurable(authorizer, accessPolicy);

        return accessPolicyToDTO(accessPolicy, userGroups, users, isConfigurable);
    }

    private Tenant tenantIdToDTO(String identifier) {
        final org.apache.nifi.registry.security.authorization.User user = userGroupProvider.getUser(identifier);
        if (user != null) {
            return tenantToDTO(user);
        } else {
            org.apache.nifi.registry.security.authorization.Group group = userGroupProvider.getGroup(identifier);
            return tenantToDTO(group);
        }
    }

    private AccessPolicySummary accessPolicyToSummaryDTO(
            final org.apache.nifi.registry.security.authorization.AccessPolicy accessPolicy) {
        if (accessPolicy == null) {
            return null;
        }

        Boolean isConfigurable = AuthorizerCapabilityDetection.isAccessPolicyConfigurable(authorizer, accessPolicy);

        final AccessPolicySummary accessPolicySummaryDTO = new AccessPolicySummary();
        accessPolicySummaryDTO.setIdentifier(accessPolicy.getIdentifier());
        accessPolicySummaryDTO.setAction(accessPolicy.getAction().toString());
        accessPolicySummaryDTO.setResource(accessPolicy.getResource());
        accessPolicySummaryDTO.setConfigurable(isConfigurable);
        return accessPolicySummaryDTO;
    }

    private Tenant tenantToDTO(org.apache.nifi.registry.security.authorization.User user) {
        if (user == null) {
            return null;
        }
        Tenant tenantDTO = new Tenant(user.getIdentifier(), user.getIdentity());
        tenantDTO.setConfigurable(AuthorizerCapabilityDetection.isUserConfigurable(authorizer, user));
        return tenantDTO;
    }

    private Tenant tenantToDTO(org.apache.nifi.registry.security.authorization.Group group) {
        if (group == null) {
            return null;
        }
        Tenant tenantDTO = new Tenant(group.getIdentifier(), group.getName());
        tenantDTO.setConfigurable(AuthorizerCapabilityDetection.isGroupConfigurable(authorizer, group));
        return tenantDTO;
    }

    private static Resource resourceToDTO(org.apache.nifi.registry.security.authorization.Resource resource) {
        if (resource == null) {
            return null;
        }
        Resource resourceDto = new Resource();
        resourceDto.setIdentifier(resource.getIdentifier());
        resourceDto.setName(resource.getName());
        return resourceDto;
    }

    private static org.apache.nifi.registry.security.authorization.User userFromDTO(
            final User userDTO) {
        if (userDTO == null) {
            return null;
        }
        return new org.apache.nifi.registry.security.authorization.User.Builder()
                .identifier(userDTO.getIdentifier())
                .identity(userDTO.getIdentity())
                .build();
    }

    private static org.apache.nifi.registry.security.authorization.Group userGroupFromDTO(
            final UserGroup userGroupDTO) {
        if (userGroupDTO == null) {
            return null;
        }
        org.apache.nifi.registry.security.authorization.Group.Builder groupBuilder = new org.apache.nifi.registry.security.authorization.Group.Builder()
                .identifier(userGroupDTO.getIdentifier())
                .name(userGroupDTO.getIdentity());
        Set<Tenant> users = userGroupDTO.getUsers();
        if (users != null) {
            groupBuilder.addUsers(users.stream().map(Tenant::getIdentifier).collect(Collectors.toSet()));
        }
        return groupBuilder.build();
    }

    private static org.apache.nifi.registry.security.authorization.AccessPolicy accessPolicyFromDTO(
            final AccessPolicy accessPolicyDTO) {
        org.apache.nifi.registry.security.authorization.AccessPolicy.Builder accessPolicyBuilder =
                new org.apache.nifi.registry.security.authorization.AccessPolicy.Builder()
                        .identifier(accessPolicyDTO.getIdentifier())
                        .resource(accessPolicyDTO.getResource())
                        .action(RequestAction.valueOfValue(accessPolicyDTO.getAction()));

        Set<Tenant> dtoUsers = accessPolicyDTO.getUsers();
        if (accessPolicyDTO.getUsers() != null) {
            accessPolicyBuilder.addUsers(dtoUsers.stream().map(Tenant::getIdentifier).collect(Collectors.toSet()));
        }

        Set<Tenant> dtoUserGroups = accessPolicyDTO.getUserGroups();
        if (dtoUserGroups != null) {
            accessPolicyBuilder.addGroups(dtoUserGroups.stream().map(Tenant::getIdentifier).collect(Collectors.toSet()));
        }

        return accessPolicyBuilder.build();
    }

    private static AccessPolicy accessPolicyToDTO(
            final org.apache.nifi.registry.security.authorization.AccessPolicy accessPolicy,
            final Collection<? extends Tenant> userGroups,
            final Collection<? extends Tenant> users,
            final Boolean isConfigurable) {

        if (accessPolicy == null) {
            return null;
        }

        final AccessPolicy accessPolicyDTO = new AccessPolicy();
        accessPolicyDTO.setIdentifier(accessPolicy.getIdentifier());
        accessPolicyDTO.setAction(accessPolicy.getAction().toString());
        accessPolicyDTO.setResource(accessPolicy.getResource());
        accessPolicyDTO.setConfigurable(isConfigurable);
        accessPolicyDTO.addUsers(users);
        accessPolicyDTO.addUserGroups(userGroups);
        return accessPolicyDTO;
    }

    private static AccessPolicyProvider createExceptionThrowingAccessPolicyProvider() {

        return new AccessPolicyProvider() {
            @Override
            public Set<org.apache.nifi.registry.security.authorization.AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
            }

            @Override
            public org.apache.nifi.registry.security.authorization.AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
            }

            @Override
            public org.apache.nifi.registry.security.authorization.AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
                throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
            }

            @Override
            public UserGroupProvider getUserGroupProvider() {
                return new UserGroupProvider() {
                    @Override
                    public Set<org.apache.nifi.registry.security.authorization.User> getUsers() throws AuthorizationAccessException {
                        throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                    }

                    @Override
                    public org.apache.nifi.registry.security.authorization.User getUser(String identifier) throws AuthorizationAccessException {
                        throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                    }

                    @Override
                    public org.apache.nifi.registry.security.authorization.User getUserByIdentity(String identity) throws AuthorizationAccessException {
                        throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                    }

                    @Override
                    public Set<Group> getGroups() throws AuthorizationAccessException {
                        throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                    }

                    @Override
                    public Group getGroup(String identifier) throws AuthorizationAccessException {
                        throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                    }

                    @Override
                    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
                        throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                    }

                    @Override
                    public void initialize(UserGroupProviderInitializationContext initializationContext) throws SecurityProviderCreationException {

                    }

                    @Override
                    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {

                    }

                    @Override
                    public void preDestruction() throws SecurityProviderDestructionException {

                    }
                };
            }

            @Override
            public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws SecurityProviderCreationException {
            }

            @Override
            public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
            }

            @Override
            public void preDestruction() throws SecurityProviderDestructionException {
            }
        };

    }

}
