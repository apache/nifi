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
import org.apache.nifi.authorization.file.generated.Authorizations;
import org.apache.nifi.authorization.file.generated.Groups;
import org.apache.nifi.authorization.file.generated.Policies;
import org.apache.nifi.authorization.file.generated.Policy;
import org.apache.nifi.authorization.file.generated.Users;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides identity checks and grants authorities.
 */
public class FileAuthorizer extends AbstractPolicyBasedAuthorizer {

    private static final Logger logger = LoggerFactory.getLogger(FileAuthorizer.class);
    private static final String USERS_XSD = "/authorizations.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authorization.file.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    static final String READ_CODE = "R";
    static final String WRITE_CODE = "W";

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, FileAuthorizer.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private Schema schema;
    private SchemaFactory schemaFactory;
    private NiFiProperties properties;
    private File authorizationsFile;
    private File restoreAuthorizationsFile;
    private String rootGroupId;

    private final AtomicReference<AuthorizationsHolder> authorizationsHolder = new AtomicReference<>();

    @Override
    public void initialize(final AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
        try {
            schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            schema = schemaFactory.newSchema(FileAuthorizer.class.getResource(USERS_XSD));
        } catch (Exception e) {
            throw new AuthorizerCreationException(e);
        }
    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        try {
            final PropertyValue authorizationsPath = configurationContext.getProperty("Authorizations File");
            if (StringUtils.isBlank(authorizationsPath.getValue())) {
                throw new AuthorizerCreationException("The authorizations file must be specified.");
            }

            // get the authorizations file and ensure it exists
            authorizationsFile = new File(authorizationsPath.getValue());
            if (!authorizationsFile.exists()) {
                throw new AuthorizerCreationException("The authorizations file must exist.");
            }

            final File authorizationsFileDirectory = authorizationsFile.getAbsoluteFile().getParentFile();

            // the restore directory is optional and may be null
            final File restoreDirectory = properties.getRestoreDirectory();
            if (restoreDirectory != null) {
                // sanity check that restore directory is a directory, creating it if necessary
                FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

                // check that restore directory is not the same as the primary directory
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

            final PropertyValue initialAdminIdentityProp = configurationContext.getProperty("Initial Admin Identity");
            final String initialAdminIdentity = initialAdminIdentityProp == null ? null : initialAdminIdentityProp.getValue();

            // load the authorizations
            load(initialAdminIdentity);

            // if we've copied the authorizations file to a restore directory synchronize it
            if (restoreAuthorizationsFile != null) {
                FileUtils.copyFile(authorizationsFile, restoreAuthorizationsFile, false, false, logger);
            }

            logger.info(String.format("Authorizations file loaded at %s", new Date().toString()));

            this.rootGroupId = configurationContext.getRootGroupId();

        } catch (IOException | AuthorizerCreationException | JAXBException | IllegalStateException e) {
            throw new AuthorizerCreationException(e);
        }
    }

    /**
     * Loads the authorizations file and populates the AuthorizationsHolder, only called during start-up.
     *
     * @throws JAXBException            Unable to reload the authorized users file
     * @throws IOException              Unable to sync file with restore
     * @throws IllegalStateException    Unable to sync file with restore
     */
    private synchronized void load(final String initialAdminIdentity) throws JAXBException, IOException, IllegalStateException {
        // attempt to unmarshal
        final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
        unmarshaller.setSchema(schema);
        final JAXBElement<Authorizations> element = unmarshaller.unmarshal(new StreamSource(authorizationsFile), Authorizations.class);

        final Authorizations authorizations = element.getValue();

        if (authorizations.getUsers() == null) {
            authorizations.setUsers(new Users());
        }
        if (authorizations.getGroups() == null) {
            authorizations.setGroups(new Groups());
        }
        if (authorizations.getPolicies() == null) {
            authorizations.setPolicies(new Policies());
        }

        final AuthorizationsHolder authorizationsHolder = new AuthorizationsHolder(authorizations);
        final boolean hasInitialAdminIdentity = (initialAdminIdentity != null && !StringUtils.isBlank(initialAdminIdentity));

        // if an initial admin was provided and there are no users or policies then automatically create the admin user & policies
        if (hasInitialAdminIdentity && authorizationsHolder.getAllUsers().isEmpty() && authorizationsHolder.getAllPolicies().isEmpty()) {
            populateInitialAdmin(authorizations, initialAdminIdentity);
            saveAndRefreshHolder(authorizations);
        } else {
            this.authorizationsHolder.set(authorizationsHolder);
        }
    }

    /**
     *  Creates the initial admin user and policies for access the flow and managing users and policies.
     *
     * @param adminIdentity the identity of the admin user
     */
    private void populateInitialAdmin(final Authorizations authorizations, final String adminIdentity) {
        // generate an identifier and add a User with the given identifier and identity
        final UUID adminIdentifier = UUID.nameUUIDFromBytes(adminIdentity.getBytes(StandardCharsets.UTF_8));
        final User adminUser = new User.Builder().identifier(adminIdentifier.toString()).identity(adminIdentity).build();

        final org.apache.nifi.authorization.file.generated.User jaxbAdminUser = createJAXBUser(adminUser);
        authorizations.getUsers().getUser().add(jaxbAdminUser);

        // grant the user read access to the /flow resource
        final AccessPolicy flowPolicy = createInitialAdminPolicy("/flow", adminUser.getIdentifier(), RequestAction.READ);
        final Policy jaxbFlowPolicy = createJAXBPolicy(flowPolicy);
        authorizations.getPolicies().getPolicy().add(jaxbFlowPolicy);

        // grant the user read/write access to the /users resource
        final AccessPolicy usersPolicy = createInitialAdminPolicy("/users", adminUser.getIdentifier(), RequestAction.READ, RequestAction.WRITE);
        final Policy jaxbUsersPolicy = createJAXBPolicy(usersPolicy);
        authorizations.getPolicies().getPolicy().add(jaxbUsersPolicy);

        // grant the user read/write access to the /groups resource
        final AccessPolicy groupsPolicy = createInitialAdminPolicy("/groups", adminUser.getIdentifier(), RequestAction.READ, RequestAction.WRITE);
        final Policy jaxbGroupsPolicy = createJAXBPolicy(groupsPolicy);
        authorizations.getPolicies().getPolicy().add(jaxbGroupsPolicy);

        // grant the user read/write access to the /policies resource
        final AccessPolicy policiesPolicy = createInitialAdminPolicy("/policies", adminUser.getIdentifier(), RequestAction.READ, RequestAction.WRITE);
        final Policy jaxbPoliciesPolicy = createJAXBPolicy(policiesPolicy);
        authorizations.getPolicies().getPolicy().add(jaxbPoliciesPolicy);
    }

    /**
     * Creates an AccessPolicy based on the given parameters, generating an identifier from the resource and admin identity.
     *
     * @param resource the resource for the policy
     * @param adminIdentity the identity of the admin user to add to the policy
     * @param actions the actions for the policy
     * @return the AccessPolicy based on the given parameters
     */
    private AccessPolicy createInitialAdminPolicy(final String resource, final String adminIdentity, final RequestAction ... actions) {
        final String uuidSeed = resource + adminIdentity;
        final UUID flowPolicyIdentifier = UUID.nameUUIDFromBytes(uuidSeed.getBytes(StandardCharsets.UTF_8));

        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier(flowPolicyIdentifier.toString())
                .resource(resource)
                .addUser(adminIdentity);

        for (RequestAction action : actions) {
            builder.addAction(action);
        }

        return builder.build();
    }

    /**
     * Saves the Authorizations instance by marshalling to a file, then re-populates the
     * in-memory data structures and sets the new holder.
     *
     * Synchronized to ensure only one thread writes the file at a time.
     *
     * @param authorizations the authorizations to save and populate from
     * @throws AuthorizationAccessException if an error occurs saving the authorizations
     */
    private synchronized void saveAndRefreshHolder(final Authorizations authorizations) throws AuthorizationAccessException {
        try {
            final Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
            marshaller.setSchema(schema);
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.marshal(authorizations, authorizationsFile);

            final AuthorizationsHolder authorizationsHolder = new AuthorizationsHolder(authorizations);
            this.authorizationsHolder.set(authorizationsHolder);
        } catch (JAXBException e) {
            throw new AuthorizationAccessException("Unable to save Authorizations", e);
        }
    }

    @AuthorizerContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public void preDestruction() {

    }

    // ------------------ Groups ------------------

    @Override
    public synchronized Group addGroup(Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        // create a new JAXB Group based on the incoming Group
        final org.apache.nifi.authorization.file.generated.Group jaxbGroup = new org.apache.nifi.authorization.file.generated.Group();
        jaxbGroup.setIdentifier(group.getIdentifier());
        jaxbGroup.setName(group.getName());

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        authorizations.getGroups().getGroup().add(jaxbGroup);
        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        return holder.getGroupsById().get(group.getIdentifier());
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }
        return authorizationsHolder.get().getGroupsById().get(identifier);
    }

    @Override
    public synchronized Group updateGroup(Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        final List<org.apache.nifi.authorization.file.generated.Group> groups = authorizations.getGroups().getGroup();

        // find the group that needs to be update
        org.apache.nifi.authorization.file.generated.Group updateGroup = null;
        for (org.apache.nifi.authorization.file.generated.Group jaxbGroup : groups) {
            if (jaxbGroup.getIdentifier().equals(group.getIdentifier())) {
                updateGroup = jaxbGroup;
                break;
            }
        }

        // if the group wasn't found return null, otherwise update the group and save changes
        if (updateGroup == null) {
            return null;
        }

        updateGroup.setName(group.getName());
        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        return holder.getGroupsById().get(group.getIdentifier());
    }

    @Override
    public synchronized Group deleteGroup(Group group) throws AuthorizationAccessException {
        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        final List<org.apache.nifi.authorization.file.generated.Group> groups = authorizations.getGroups().getGroup();

        // for each user iterate over the group references and remove the group reference if it matches the group being deleted
        for (org.apache.nifi.authorization.file.generated.User user : authorizations.getUsers().getUser()) {
            Iterator<org.apache.nifi.authorization.file.generated.User.Group> userGroupIter = user.getGroup().iterator();
            while (userGroupIter.hasNext()) {
                org.apache.nifi.authorization.file.generated.User.Group userGroup = userGroupIter.next();
                if (userGroup.getIdentifier().equals(group.getIdentifier())) {
                    userGroupIter.remove();
                    break;
                }
            }
        }

        // for each policy iterate over the group reference and remove the group reference if it matches the group being deleted
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            Iterator<Policy.Group> policyGroupIter = policy.getGroup().iterator();
            while (policyGroupIter.hasNext()) {
                Policy.Group policyGroup = policyGroupIter.next();
                if (policyGroup.getIdentifier().equals(group.getIdentifier())) {
                    policyGroupIter.remove();
                    break;
                }
            }
        }

        // now remove the actual group from the top-level list of groups
        boolean removedGroup = false;
        Iterator<org.apache.nifi.authorization.file.generated.Group> iter = groups.iterator();
        while (iter.hasNext()) {
            org.apache.nifi.authorization.file.generated.Group jaxbGroup = iter.next();
            if (group.getIdentifier().equals(jaxbGroup.getIdentifier())) {
                iter.remove();
                removedGroup = true;
                break;
            }
        }

        if (removedGroup) {
            saveAndRefreshHolder(authorizations);
            return group;
        } else {
            return null;
        }
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return authorizationsHolder.get().getAllGroups();
    }

    // ------------------ Users ------------------

    @Override
    public synchronized User addUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final org.apache.nifi.authorization.file.generated.User jaxbUser = createJAXBUser(user);

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        authorizations.getUsers().getUser().add(jaxbUser);

        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getUsersById().get(user.getIdentifier());
    }

    private org.apache.nifi.authorization.file.generated.User createJAXBUser(User user) {
        final org.apache.nifi.authorization.file.generated.User jaxbUser = new org.apache.nifi.authorization.file.generated.User();
        jaxbUser.setIdentifier(user.getIdentifier());
        jaxbUser.setIdentity(user.getIdentity());

        for (String groupIdentifier : user.getGroups()) {
            org.apache.nifi.authorization.file.generated.User.Group group = new org.apache.nifi.authorization.file.generated.User.Group();
            group.setIdentifier(groupIdentifier);
            jaxbUser.getGroup().add(group);
        }
        return jaxbUser;
    }

    @Override
    public User getUser(final String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getUsersById().get(identifier);
    }

    @Override
    public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
        if (identity == null) {
            return null;
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getUsersByIdentity().get(identity);
    }

    @Override
    public synchronized User updateUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        final List<org.apache.nifi.authorization.file.generated.User> users = authorizations.getUsers().getUser();

        // fine the User that needs to be updated
        org.apache.nifi.authorization.file.generated.User updateUser = null;
        for (org.apache.nifi.authorization.file.generated.User jaxbUser : users) {
            if (user.getIdentifier().equals(jaxbUser.getIdentifier())) {
                updateUser = jaxbUser;
                break;
            }
        }

        // if user wasn't found return null, otherwise update the user and save changes
        if (updateUser == null) {
            return null;
        } else {
            updateUser.setIdentity(user.getIdentity());

            updateUser.getGroup().clear();
            for (String groupIdentifier : user.getGroups()) {
                org.apache.nifi.authorization.file.generated.User.Group group = new org.apache.nifi.authorization.file.generated.User.Group();
                group.setIdentifier(groupIdentifier);
                updateUser.getGroup().add(group);
            }

            saveAndRefreshHolder(authorizations);

            final AuthorizationsHolder holder = authorizationsHolder.get();
            return holder.getUsersById().get(user.getIdentifier());
        }
    }

    @Override
    public synchronized User deleteUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        final List<org.apache.nifi.authorization.file.generated.User> users = authorizations.getUsers().getUser();

        // remove any references to the user being deleted from policies
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            Iterator<Policy.User> policyUserIter = policy.getUser().iterator();
            while (policyUserIter.hasNext()) {
                Policy.User policyUser = policyUserIter.next();
                if (policyUser.getIdentifier().equals(user.getIdentifier())) {
                    policyUserIter.remove();
                    break;
                }
            }
        }

        // remove the actual user if it exists
        boolean removedUser = false;
        Iterator<org.apache.nifi.authorization.file.generated.User> iter = users.iterator();
        while (iter.hasNext()) {
            org.apache.nifi.authorization.file.generated.User jaxbUser = iter.next();
            if (user.getIdentifier().equals(jaxbUser.getIdentifier())) {
                iter.remove();
                removedUser = true;
                break;
            }
        }

        if (removedUser) {
            saveAndRefreshHolder(authorizations);
            return user;
        } else {
            return null;
        }
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return authorizationsHolder.get().getAllUsers();
    }

    // ------------------ AccessPolicies ------------------

    @Override
    public synchronized AccessPolicy addAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        // create the new JAXB Policy
        final Policy policy = createJAXBPolicy(accessPolicy);

        // add the new Policy to the top-level list of policies
        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        authorizations.getPolicies().getPolicy().add(policy);

        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getPoliciesById().get(accessPolicy.getIdentifier());
    }

    private Policy createJAXBPolicy(final AccessPolicy accessPolicy) {
        final Policy policy = new Policy();
        policy.setIdentifier(accessPolicy.getIdentifier());
        transferState(accessPolicy, policy);
        return policy;
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
    public synchronized AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();

        // try to find an existing Authorization that matches the policy id
        Policy updatePolicy = null;
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            if (policy.getIdentifier().equals(accessPolicy.getIdentifier())) {
                updatePolicy = policy;
                break;
            }
        }

        // no matching Policy so return null
        if (updatePolicy == null) {
            return null;
        }

        // update the Policy, save, reload, and return
        transferState(accessPolicy, updatePolicy);
        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getPoliciesById().get(accessPolicy.getIdentifier());
    }

    @Override
    public synchronized AccessPolicy deleteAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();

        // find the matching Policy and remove it
        boolean deletedPolicy = false;
        Iterator<Policy> policyIter = authorizations.getPolicies().getPolicy().iterator();
        while (policyIter.hasNext()) {
            final Policy policy = policyIter.next();
            if (policy.getIdentifier().equals(accessPolicy.getIdentifier())) {
                policyIter.remove();
                deletedPolicy = true;
                break;
            }
        }

        // never found a matching Policy so return null
        if (!deletedPolicy) {
            return null;
        }

        saveAndRefreshHolder(authorizations);
        return accessPolicy;
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return authorizationsHolder.get().getAllPolicies();
    }

    @Override
    public UsersAndAccessPolicies getUsersAndAccessPolicies() throws AuthorizationAccessException {
        return authorizationsHolder.get();
    }

    /**
     * Sets the given Policy to the state of the provided AccessPolicy. Users and Groups will be cleared and
     * set to match the AccessPolicy, the resource and action will be set to match the AccessPolicy.
     *
     * Does not set the identifier.
     *
     * @param accessPolicy the AccessPolicy to transfer state from
     * @param policy the Policy to transfer state to
     */
    private void transferState(AccessPolicy accessPolicy, Policy policy) {
        policy.setResource(accessPolicy.getResource());

        // add users to the policy
        policy.getUser().clear();
        for (String userIdentifier : accessPolicy.getUsers()) {
            Policy.User policyUser = new Policy.User();
            policyUser.setIdentifier(userIdentifier);
            policy.getUser().add(policyUser);
        }

        // add groups to the policy
        policy.getGroup().clear();
        for (String groupIdentifier : accessPolicy.getGroups()) {
            Policy.Group policyGroup = new Policy.Group();
            policyGroup.setIdentifier(groupIdentifier);
            policy.getGroup().add(policyGroup);
        }

        // add the action to the policy
        boolean containsRead = accessPolicy.getActions().contains(RequestAction.READ);
        boolean containsWrite = accessPolicy.getActions().contains(RequestAction.WRITE);

        if (containsRead && containsWrite) {
            policy.setAction(READ_CODE + WRITE_CODE);
        } else if (containsRead) {
            policy.setAction(READ_CODE);
        } else {
            policy.setAction(WRITE_CODE);
        }
    }

}
