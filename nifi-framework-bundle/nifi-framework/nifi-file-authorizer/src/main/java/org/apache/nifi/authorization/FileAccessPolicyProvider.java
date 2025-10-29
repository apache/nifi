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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.FlowInfo;
import org.apache.nifi.util.FlowParser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.xml.processing.ProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.nifi.authorization.RequestAction.READ;
import static org.apache.nifi.authorization.RequestAction.WRITE;

public class FileAccessPolicyProvider implements ConfigurableAccessPolicyProvider {

    private static final Logger logger = LoggerFactory.getLogger(FileAccessPolicyProvider.class);

    static final String PROP_NODE_IDENTITY_PREFIX = "Node Identity ";
    static final String PROP_NODE_GROUP_NAME = "Node Group";
    static final String PROP_USER_GROUP_PROVIDER = "User Group Provider";
    static final String PROP_AUTHORIZATIONS_FILE = "Authorizations File";
    static final String PROP_INITIAL_ADMIN_IDENTITY = "Initial Admin Identity";
    static final String PROP_INITIAL_ADMIN_GROUP = "Initial Admin Group";
    static final Pattern NODE_IDENTITY_PATTERN = Pattern.compile(PROP_NODE_IDENTITY_PREFIX + "\\S+");

    private NiFiProperties properties;
    private File authorizationsFile;
    private File restoreAuthorizationsFile;
    private String rootGroupId;
    private String initialAdminIdentity;
    private String initialAdminGroup;
    private Set<String> nodeIdentities;
    private String nodeGroupIdentifier;

    private UserGroupProvider userGroupProvider;
    private UserGroupProviderLookup userGroupProviderLookup;
    private final AtomicReference<AuthorizationsHolder> authorizationsHolder = new AtomicReference<>();

    private final AccessPolicyMapper fileAccessPolicyMapper = new FileAccessPolicyMapper();
    private final AccessPolicyMapper fingerprintAccessPolicyMapper = new FingerprintAccessPolicyMapper();

    @Override
    public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {
        userGroupProviderLookup = initializationContext.getUserGroupProviderLookup();
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        try {
            final PropertyValue userGroupProviderIdentifier = configurationContext.getProperty(PROP_USER_GROUP_PROVIDER);
            if (!userGroupProviderIdentifier.isSet()) {
                throw new AuthorizerCreationException("The user group provider must be specified.");
            }

            userGroupProvider = userGroupProviderLookup.getUserGroupProvider(userGroupProviderIdentifier.getValue());
            if (userGroupProvider == null) {
                throw new AuthorizerCreationException("Unable to locate user group provider with identifier " + userGroupProviderIdentifier.getValue());
            }

            final PropertyValue authorizationsPath = configurationContext.getProperty(PROP_AUTHORIZATIONS_FILE);
            if (StringUtils.isBlank(authorizationsPath.getValue())) {
                throw new AuthorizerCreationException("The authorizations file must be specified.");
            }

            // get the authorizations file and ensure it exists
            authorizationsFile = new File(authorizationsPath.getValue());
            if (!authorizationsFile.exists()) {
                logger.info("Creating new authorizations file at {}", authorizationsFile.getAbsolutePath());
                saveAuthorizations(new ArrayList<>(), authorizationsFile);
            }

            final File authorizationsFileDirectory = authorizationsFile.getAbsoluteFile().getParentFile();

            // the restore directory is optional and may be null
            final File restoreDirectory = properties.getRestoreDirectory();
            if (restoreDirectory != null) {
                // sanity check that restore directory is a directory, creating it if necessary
                FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

                // check that restore directory is not the same as the authorizations directory
                if (authorizationsFileDirectory.getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
                    throw new AuthorizerCreationException(String.format("Authorizations file directory '%s' is the same as restore directory '%s' ",
                            authorizationsFileDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
                }

                // the restore copy will have same file name, but reside in a different directory
                restoreAuthorizationsFile = new File(restoreDirectory, authorizationsFile.getName());

                try {
                    // sync the primary copy with the restore copy
                    FileUtils.syncWithRestore(authorizationsFile, restoreAuthorizationsFile, logger);
                } catch (final IOException | IllegalStateException ioe) {
                    throw new AuthorizerCreationException(ioe);
                }
            }

            // extract the identity mappings from nifi.properties if any are provided
            List<IdentityMapping> identityMappings = Collections.unmodifiableList(IdentityMappingUtil.getIdentityMappings(properties));

            // get the value of the initial admin identity
            final PropertyValue initialAdminIdentityProp = configurationContext.getProperty(PROP_INITIAL_ADMIN_IDENTITY);
            initialAdminIdentity = initialAdminIdentityProp.isSet() ? IdentityMappingUtil.mapIdentity(initialAdminIdentityProp.getValue(), identityMappings) : null;

            // get the value of the initial admin group
            final PropertyValue initialAdminGroupProp = configurationContext.getProperty(PROP_INITIAL_ADMIN_GROUP);
            initialAdminGroup = initialAdminGroupProp.isSet() ? IdentityMappingUtil.mapIdentity(initialAdminGroupProp.getValue(), identityMappings) : null;

            // extract any node identities
            nodeIdentities = new HashSet<>();
            for (Map.Entry<String, String> entry : configurationContext.getProperties().entrySet()) {
                Matcher matcher = NODE_IDENTITY_PATTERN.matcher(entry.getKey());
                if (matcher.matches() && !StringUtils.isBlank(entry.getValue())) {
                    final String mappedNodeIdentity = IdentityMappingUtil.mapIdentity(entry.getValue(), identityMappings);
                    nodeIdentities.add(mappedNodeIdentity);
                    logger.info("Added mapped node {} (raw node identity {})", mappedNodeIdentity, entry.getValue());
                }
            }

            // read node group name
            PropertyValue nodeGroupNameProp = configurationContext.getProperty(PROP_NODE_GROUP_NAME);
            String nodeGroupName = (nodeGroupNameProp != null && nodeGroupNameProp.isSet()) ? nodeGroupNameProp.getValue() : null;

            // look up node group identifier using node group name
            nodeGroupIdentifier = null;

            if (nodeGroupName != null) {
                if (!StringUtils.isBlank(nodeGroupName)) {
                    logger.debug("Trying to load node group '{}' from the underlying userGroupProvider", nodeGroupName);
                    for (Group group : userGroupProvider.getGroups()) {
                        if (group.getName().equals(nodeGroupName)) {
                            nodeGroupIdentifier = group.getIdentifier();
                            break;
                        }
                    }

                    if (nodeGroupIdentifier == null) {
                        throw new AuthorizerCreationException(String.format(
                            "Authorizations node group '%s' could not be found", nodeGroupName));
                    }
                } else {
                    logger.debug("Empty node group name provided");
                }
            }

            // load the authorizations
            load();

            // if we've copied the authorizations file to a restore directory synchronize it
            if (restoreAuthorizationsFile != null) {
                FileUtils.copyFile(authorizationsFile, restoreAuthorizationsFile, false, false, logger);
            }

            logger.debug("Authorizations file loaded");
        } catch (IOException | AuthorizerCreationException | IllegalStateException e) {
            throw new AuthorizerCreationException(e);
        }
    }

    @Override
    public UserGroupProvider getUserGroupProvider() {
        return userGroupProvider;
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return authorizationsHolder.get().getAllPolicies();
    }

    @Override
    public synchronized AccessPolicy addAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        addAccessPolicies(Collections.singletonList(accessPolicy));
        return authorizationsHolder.get().getPoliciesById().get(accessPolicy.getIdentifier());
    }

    private synchronized void addAccessPolicies(final List<AccessPolicy> accessPolicies) throws AuthorizationAccessException {
        if (accessPolicies == null) {
            throw new IllegalArgumentException("AccessPolicies cannot be null");
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        final List<AccessPolicy> currentPolicies = holder.getPolicies();
        currentPolicies.addAll(accessPolicies);

        saveAndRefreshHolder(currentPolicies);
    }

    public synchronized void purgePolicies(final boolean save) {
        final AuthorizationsHolder holder = authorizationsHolder.get();
        final List<AccessPolicy> currentPolicies = holder.getPolicies();

        currentPolicies.clear();

        if (save) {
            saveAndRefreshHolder(currentPolicies);
        }
    }

    public void backupPolicies() {
        final AuthorizationsHolder holder = authorizationsHolder.get();
        final List<AccessPolicy> policies = holder.getPolicies();

        final String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").format(OffsetDateTime.now());

        final File backupFile = new File(authorizationsFile.getParentFile(), authorizationsFile.getName() + "." + timestamp);
        logger.info("Writing backup of Policies to {}", backupFile.getAbsolutePath());
        saveAuthorizations(policies, backupFile);
    }

    @Override
    public AccessPolicy getAccessPolicy(final String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getPoliciesById().get(identifier);
    }

    @Override
    public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
        return authorizationsHolder.get().getAccessPolicy(resourceIdentifier, action);
    }

    @Override
    public synchronized AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        Objects.requireNonNull(accessPolicy, "Access Policy required");

        final List<AccessPolicy> updatedPolicies = new ArrayList<>();

        AccessPolicy updatedPolicy = null;
        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        for (final AccessPolicy currentPolicy : holder.getPolicies()) {
            if (currentPolicy.getIdentifier().equals(accessPolicy.getIdentifier())) {
                updatedPolicy = new AccessPolicy.Builder()
                        .identifier(currentPolicy.getIdentifier())
                        .resource(currentPolicy.getResource())
                        .action(currentPolicy.getAction())
                        // Set Users and Groups from new Access Policy
                        .addUsers(accessPolicy.getUsers())
                        .addGroups(accessPolicy.getGroups())
                        .build();

                updatedPolicies.add(updatedPolicy);
            } else {
                updatedPolicies.add(currentPolicy);
            }
        }

        if (updatedPolicy != null) {
            saveAndRefreshHolder(updatedPolicies);
        }

        return updatedPolicy;
    }

    @Override
    public synchronized AccessPolicy deleteAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        Objects.requireNonNull(accessPolicy, "Access Policy required");

        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        final List<AccessPolicy> policies = holder.getPolicies();

        // find the matching Policy and remove it
        boolean deletedPolicy = false;
        final Iterator<AccessPolicy> currentPolicies = policies.iterator();
        while (currentPolicies.hasNext()) {
            final AccessPolicy policy = currentPolicies.next();
            if (policy.getIdentifier().equals(accessPolicy.getIdentifier())) {
                currentPolicies.remove();
                deletedPolicy = true;
                break;
            }
        }

        // never found a matching Policy so return null
        if (!deletedPolicy) {
            return null;
        }

        saveAndRefreshHolder(policies);
        return accessPolicy;
    }

    @AuthorizerContext
    public void setNiFiProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public synchronized void inheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        final List<AccessPolicy> accessPolicies = parsePolicies(fingerprint);
        addAccessPolicies(accessPolicies);
    }

    @Override
    public synchronized void forciblyInheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        final List<AccessPolicy> accessPolicies = parsePolicies(fingerprint);

        if (isInheritable()) {
            logger.debug("Inherited Access Policies [{}]", accessPolicies.size());
            addAccessPolicies(accessPolicies);
        } else {
            logger.info("Cannot directly inherit cluster's Access Policies. Will create backup of existing policies and replace with proposed policies");

            try {
                backupPolicies();
            } catch (final Exception e) {
                throw new AuthorizationAccessException("Failed to backup existing policies so will not inherit any policies", e);
            }

            purgePolicies(false);
            addAccessPolicies(accessPolicies);
        }
    }

    @Override
    public void checkInheritability(final String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
        // ensure we are in a proper state to inherit the fingerprint
        if (!isInheritable()) {
            throw new UninheritableAuthorizationsException("Proposed fingerprint is not inheritable because the current access policies is not empty.");
        }
    }

    private boolean isInheritable() {
        return getAccessPolicies().isEmpty();
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        final List<AccessPolicy> policies = new ArrayList<>(getAccessPolicies());

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            fingerprintAccessPolicyMapper.writeAccessPolicies(policies, outputStream);
            outputStream.flush();
            return outputStream.toString(StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new AuthorizationAccessException("Failed to generate fingerprint for Authorizations", e);
        }
    }

    private List<AccessPolicy> parsePolicies(final String fingerprint) {
        final byte[] fingerprintBytes = fingerprint.getBytes(StandardCharsets.UTF_8);
        try (final InputStream inputStream = new ByteArrayInputStream(fingerprintBytes)) {
            return fingerprintAccessPolicyMapper.readAccessPolicies(inputStream);
        } catch (final ProcessingException | IOException e) {
            throw new AuthorizationAccessException("Unable to parse fingerprint", e);
        }
    }

    /**
     * Loads the authorizations file and populates the AuthorizationsHolder, only called during start-up.
     *
     * @throws IOException              Unable to sync file with restore
     */
    private synchronized void load() throws IOException {
        final List<AccessPolicy> policies = new ArrayList<>();
        try (InputStream inputStream = new FileInputStream(authorizationsFile)) {
            final List<AccessPolicy> accessPolicies = fileAccessPolicyMapper.readAccessPolicies(inputStream);
            policies.addAll(accessPolicies);
        } catch (final ProcessingException e) {
            throw new AuthorizerCreationException("Failed to read Authorizations from [%s]".formatted(authorizationsFile), e);
        }

        final AuthorizationsHolder authorizationsHolder = new AuthorizationsHolder(policies);
        final boolean emptyAuthorizations = authorizationsHolder.getAllPolicies().isEmpty();
        final boolean hasInitialAdminIdentity = (initialAdminIdentity != null && !StringUtils.isBlank(initialAdminIdentity));
        final boolean hasInitialAdminGroup = (initialAdminGroup != null && !StringUtils.isBlank(initialAdminGroup));

        // if we are starting fresh then we might need to populate an initial admin, admin group or convert legacy users
        if (emptyAuthorizations) {
            parseFlow();

            if (hasInitialAdminIdentity) {
                logger.info("Populating authorizations for Initial Admin [{}]", initialAdminIdentity);
                populateInitialAdmin(policies, hasInitialAdminGroup);
            }
            if (hasInitialAdminGroup) {
                logger.info("Populating authorizations for Initial Admin Group [{}]", initialAdminGroup);
                populateInitialAdminGroup(policies);
            }

            populateNodes(policies);

            // save any changes that were made and repopulate the holder
            saveAndRefreshHolder(policies);
        } else {
            this.authorizationsHolder.set(authorizationsHolder);
        }
    }

    private void saveAuthorizations(final List<AccessPolicy> policies, final File destinationFile) {
        try (OutputStream outputStream = new FileOutputStream(destinationFile)) {
            fileAccessPolicyMapper.writeAccessPolicies(policies, outputStream);
        } catch (final IOException | ProcessingException e) {
            throw new AuthorizerCreationException("Write Authorization [%s] failed".formatted(destinationFile), e);
        }
    }

    /**
     * Try to parse the flow configuration file to extract the root group id and port information.
     */
    private void parseFlow() {
        final FlowParser flowParser = new FlowParser();
        final File flowConfigurationFile = properties.getFlowConfigurationFile();
        final FlowInfo flowInfo = flowParser.parse(flowConfigurationFile);

        if (flowInfo != null) {
            rootGroupId = flowInfo.getRootGroupId();
        }
    }

    /**
     *  Grants the initial admin user the policies for accessing the flow and managing users and policies.
     * <p>
     *  Either by creating the policies for the user itself or by making it a member of the initial admin group.
     */
    private void populateInitialAdmin(final List<AccessPolicy> policies, boolean hasInitialAdminGroup) {
        final User initialAdmin = userGroupProvider.getUserByIdentity(initialAdminIdentity);
        if (initialAdmin == null) {
            throw new AuthorizerCreationException("Unable to locate initial admin " + initialAdminIdentity + " to seed policies");
        }

        if (hasInitialAdminGroup && userGroupProvider instanceof ConfigurableUserGroupProvider configurableProvider) {
            final Group initialAdminGroup = userGroupProvider.getGroupByName(this.initialAdminGroup);
            if (initialAdminGroup == null) {
                throw new AuthorizerCreationException("Unable to locate initial admin group " + this.initialAdminGroup + " to seed policies");
            }

            if (configurableProvider.isConfigurable(initialAdminGroup)) {
                final Group updatedAdminGroup = new Group.Builder(initialAdminGroup)
                        .addUser(initialAdmin.getIdentifier())
                        .build();

                configurableProvider.updateGroup(updatedAdminGroup);
                return; // user has access through membership; no need to add policies to the user explicitly
            }
        }

        // grant the user read access to the /flow resource
        addUserToAccessPolicy(policies, ResourceType.Flow.getValue(), initialAdmin.getIdentifier(), READ);

        // grant the user read access to the root process group resource
        if (rootGroupId != null) {
            addUserToAccessPolicy(policies, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdmin.getIdentifier(), READ);
            addUserToAccessPolicy(policies, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdmin.getIdentifier(), WRITE);

            addUserToAccessPolicy(policies, ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdmin.getIdentifier(), READ);
            addUserToAccessPolicy(policies, ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdmin.getIdentifier(), WRITE);
        }

        // grant the user write to restricted components
        addUserToAccessPolicy(policies, ResourceType.RestrictedComponents.getValue(), initialAdmin.getIdentifier(), WRITE);

        // grant the user read/write access to the /tenants resource
        addUserToAccessPolicy(policies, ResourceType.Tenant.getValue(), initialAdmin.getIdentifier(), READ);
        addUserToAccessPolicy(policies, ResourceType.Tenant.getValue(), initialAdmin.getIdentifier(), WRITE);

        // grant the user read/write access to the /policies resource
        addUserToAccessPolicy(policies, ResourceType.Policy.getValue(), initialAdmin.getIdentifier(), READ);
        addUserToAccessPolicy(policies, ResourceType.Policy.getValue(), initialAdmin.getIdentifier(), WRITE);

        // grant the user read/write access to the /controller resource
        addUserToAccessPolicy(policies, ResourceType.Controller.getValue(), initialAdmin.getIdentifier(), READ);
        addUserToAccessPolicy(policies, ResourceType.Controller.getValue(), initialAdmin.getIdentifier(), WRITE);
    }

    /**
     *  Grants the initial admin group the policies for accessing the flow and managing users and policies.
     */
    private void populateInitialAdminGroup(final List<AccessPolicy> policies) {
        final Group initialAdminGroup = userGroupProvider.getGroupByName(this.initialAdminGroup);
        if (initialAdminGroup == null) {
            throw new AuthorizerCreationException("Unable to locate initial admin group " + this.initialAdminGroup + " to seed policies");
        }

        // grant the group read access to the /flow resource
        addGroupToAccessPolicy(policies, ResourceType.Flow.getValue(), initialAdminGroup.getIdentifier(), READ);

        // grant the group read access to the root process group resource
        if (rootGroupId != null) {
            addGroupToAccessPolicy(policies, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdminGroup.getIdentifier(), READ);
            addGroupToAccessPolicy(policies, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdminGroup.getIdentifier(), WRITE);

            addGroupToAccessPolicy(policies, ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdminGroup.getIdentifier(), READ);
            addGroupToAccessPolicy(policies, ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdminGroup.getIdentifier(), WRITE);
        }

        // grant the group write to restricted components
        addGroupToAccessPolicy(policies, ResourceType.RestrictedComponents.getValue(), initialAdminGroup.getIdentifier(), WRITE);

        // grant the group read/write access to the /tenants resource
        addGroupToAccessPolicy(policies, ResourceType.Tenant.getValue(), initialAdminGroup.getIdentifier(), READ);
        addGroupToAccessPolicy(policies, ResourceType.Tenant.getValue(), initialAdminGroup.getIdentifier(), WRITE);

        // grant the group read/write access to the /policies resource
        addGroupToAccessPolicy(policies, ResourceType.Policy.getValue(), initialAdminGroup.getIdentifier(), READ);
        addGroupToAccessPolicy(policies, ResourceType.Policy.getValue(), initialAdminGroup.getIdentifier(), WRITE);

        // grant the group read/write access to the /controller resource
        addGroupToAccessPolicy(policies, ResourceType.Controller.getValue(), initialAdminGroup.getIdentifier(), READ);
        addGroupToAccessPolicy(policies, ResourceType.Controller.getValue(), initialAdminGroup.getIdentifier(), WRITE);
    }

    /**
     * Creates a user for each node and gives the nodes write permission to /proxy.
     *
     * @param policies Current Policies
     */
    private void populateNodes(final List<AccessPolicy> policies) {
        // authorize static nodes
        for (String nodeIdentity : nodeIdentities) {
            final User node = userGroupProvider.getUserByIdentity(nodeIdentity);
            if (node == null) {
                throw new AuthorizerCreationException("Unable to locate node " + nodeIdentity + " to seed policies.");
            }
            logger.debug("Populating default authorizations for node '{}' ({})", node.getIdentity(), node.getIdentifier());
            // grant access to the proxy resource
            addUserToAccessPolicy(policies, ResourceType.Proxy.getValue(), node.getIdentifier(), WRITE);
            // grant access to read controller for syncing custom NARs
            addUserToAccessPolicy(policies, ResourceType.Controller.getValue(), node.getIdentifier(), READ);

            // grant the user read/write access data of the root group
            if (rootGroupId != null) {
                addUserToAccessPolicy(policies, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, node.getIdentifier(), READ);
                addUserToAccessPolicy(policies, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, node.getIdentifier(), WRITE);
            }
        }

        // authorize dynamic nodes (node group)
        if (nodeGroupIdentifier != null) {
            logger.debug("Populating default authorizations for group '{}' ({})", userGroupProvider.getGroup(nodeGroupIdentifier).getName(), nodeGroupIdentifier);
            addGroupToAccessPolicy(policies, ResourceType.Proxy.getValue(), nodeGroupIdentifier, WRITE);

            if (rootGroupId != null) {
                addGroupToAccessPolicy(policies, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, nodeGroupIdentifier, READ);
                addGroupToAccessPolicy(policies, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, nodeGroupIdentifier, WRITE);
            }
        }
    }

    /**
     * Creates and adds an access policy for the given resource, identity, and actions to the specified authorizations.
     *
     * @param policies Current Policies
     * @param resource the resource for the policy
     * @param userIdentifier the identifier for the user to add to the policy
     * @param action the action for the policy
     */
    private void addUserToAccessPolicy(final List<AccessPolicy> policies, final String resource, final String userIdentifier, final RequestAction action) {
        final ListIterator<AccessPolicy> accessPolicies = policies.listIterator();

        boolean updated = false;

        while (accessPolicies.hasNext()) {
            final AccessPolicy accessPolicy = accessPolicies.next();
            if (accessPolicy.getResource().equals(resource) && action == accessPolicy.getAction()) {
                accessPolicies.remove();
                final AccessPolicy updatedPolicy = new AccessPolicy.Builder(accessPolicy).addUser(userIdentifier).build();
                accessPolicies.add(updatedPolicy);
                updated = true;
                break;
            }
        }

        if (!updated) {
            final String uuidSeed = resource + action;
            final AccessPolicy accessPolicy = new AccessPolicy.Builder()
                    .identifierGenerateFromSeed(uuidSeed)
                    .resource(resource)
                    .action(action)
                    .addUser(userIdentifier)
                    .build();

            policies.add(accessPolicy);
        }
    }

    /**
     * Creates and adds an access policy for the given resource, group identity, and actions to the specified authorizations.
     *
     * @param policies Current Policies
     * @param resource       the resource for the policy
     * @param groupIdentifier the identifier for the group to add to the policy
     * @param action         the action for the policy
     */
    private void addGroupToAccessPolicy(final List<AccessPolicy> policies, final String resource, final String groupIdentifier, final RequestAction action) {
        final ListIterator<AccessPolicy> accessPolicies = policies.listIterator();
        boolean updated = false;

        while (accessPolicies.hasNext()) {
            final AccessPolicy accessPolicy = accessPolicies.next();
            if (accessPolicy.getResource().equals(resource) && action == accessPolicy.getAction()) {
                accessPolicies.remove();
                final AccessPolicy updatedPolicy = new AccessPolicy.Builder(accessPolicy).addGroup(groupIdentifier).build();
                accessPolicies.add(updatedPolicy);
                updated = true;
                break;
            }
        }

        if (!updated) {
            final String uuidSeed = resource + action;
            final AccessPolicy accessPolicy = new AccessPolicy.Builder()
                    .identifierGenerateFromSeed(uuidSeed)
                    .resource(resource)
                    .action(action)
                    .addGroup(groupIdentifier)
                    .build();

            policies.add(accessPolicy);
        }
    }

    /**
     * Saves the Authorizations instance by marshalling to a file, then re-populates the
     * in-memory data structures and sets the new holder.
     * Synchronized to ensure only one thread writes the file at a time.
     *
     * @param policies Current Policies
     * @throws AuthorizationAccessException if an error occurs saving the authorizations
     */
    private synchronized void saveAndRefreshHolder(final List<AccessPolicy> policies) throws AuthorizationAccessException {
        try (OutputStream outputStream = new FileOutputStream(authorizationsFile)) {
            fileAccessPolicyMapper.writeAccessPolicies(policies, outputStream);
            authorizationsHolder.set(new AuthorizationsHolder(policies));
        } catch (final IOException e) {
            throw new AuthorizationAccessException("Unable to save Authorizations to [%s]".formatted(authorizationsFile), e);
        }
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
    }
}
