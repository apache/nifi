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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides identity checks and grants authorities.
 */
public class FileAuthorizer extends AbstractPolicyBasedAuthorizer {

    private static final Logger logger = LoggerFactory.getLogger(FileAuthorizer.class);
    private static final String READ_CODE = "R";
    private static final String WRITE_CODE = "W";
    private static final String USERS_XSD = "/authorizations.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authorization.file.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

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

    private final AtomicReference<Authorizations> authorizations = new AtomicReference<>();

    private final AtomicReference<Set<AccessPolicy>> allPolicies = new AtomicReference<>();
    private final AtomicReference<Map<String, Set<AccessPolicy>>> policiesByResource = new AtomicReference<>();
    private final AtomicReference<Map<String, AccessPolicy>> policiesById = new AtomicReference<>();

    private final AtomicReference<Set<User>> allUsers = new AtomicReference<>();
    private final AtomicReference<Map<String,User>> usersById = new AtomicReference<>();
    private final AtomicReference<Map<String,User>> usersByIdentity = new AtomicReference<>();

    private final AtomicReference<Set<Group>> allGroups = new AtomicReference<>();
    private final AtomicReference<Map<String,Group>> groupsById = new AtomicReference<>();

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

            // load the authorizations
            load();

            // if there are no users or policies then see if an initial admin was provided
            if (allUsers.get().isEmpty() && allPolicies.get().isEmpty()) {
                final PropertyValue initialAdminIdentity = configurationContext.getProperty("Initial Admin Identity");
                if (initialAdminIdentity != null && !StringUtils.isBlank(initialAdminIdentity.getValue())) {
                    populateInitialAdmin(initialAdminIdentity.getValue());
                }
            }

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
     * Reloads the authorized users file.
     *
     * @throws JAXBException            Unable to reload the authorized users file
     * @throws IOException              Unable to sync file with restore
     * @throws IllegalStateException    Unable to sync file with restore
     */
    private void load() throws JAXBException, IOException, IllegalStateException {
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

        this.authorizations.set(authorizations);
        load(authorizations);
    }

    /**
     * Loads the internal data structures from the given Authorizations.
     *
     * @param authorizations the Authorizations to populate from
     */
    private void load(final Authorizations authorizations) {
        // load all users
        final Users users = authorizations.getUsers();
        final Set<User> allUsers = Collections.unmodifiableSet(createUsers(users));

        // load all groups
        final Groups groups = authorizations.getGroups();
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

        // create a convenience map from resource id to policies
        final Map<String, Set<AccessPolicy>> resourcePolicies = Collections.unmodifiableMap(createResourcePolicyMap(allPolicies));

        // create a convenience map from policy id to policy
        final Map<String, AccessPolicy> policiesByIdMap = Collections.unmodifiableMap(createPoliciesByIdMap(allPolicies));

        // set all the holders
        this.allUsers.set(allUsers);
        this.allGroups.set(allGroups);
        this.allPolicies.set(allPolicies);
        this.usersById.set(userByIdMap);
        this.usersByIdentity.set(userByIdentityMap);
        this.groupsById.set(groupByIdMap);
        this.policiesByResource.set(resourcePolicies);
        this.policiesById.set(policiesByIdMap);
    }

    /**
     * Saves the Authorizations instance by marshalling to a file.
     *
     * @param authorizations the authorizations to save
     * @throws AuthorizationAccessException if an error occurs saving the authorizations
     */
    private void save(final Authorizations authorizations) throws AuthorizationAccessException {
        try {
            final Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
            marshaller.setSchema(schema);
            marshaller.marshal(authorizations, authorizationsFile);
            this.authorizations.set(authorizations);
        } catch (JAXBException e) {
            throw new AuthorizationAccessException("Unable to save Authorizations", e);
        }
    }

    /**
     * Saves the Authorizations instance by marshalling to a file, then re-populates the
     * in-memory data structures.
     *
     * @param authorizations the authorizations to save and populate from
     * @throws AuthorizationAccessException if an error occurs saving the authorizations
     */
    private void saveAndLoad(final Authorizations authorizations) throws AuthorizationAccessException {
        save(authorizations);
        load(authorizations);
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
            if (authorizationCode.contains(READ_CODE)) {
                builder.addAction(RequestAction.READ);
            }
            if (authorizationCode.contains(WRITE_CODE)) {
                builder.addAction(RequestAction.WRITE);
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
    private Set<User> createUsers(org.apache.nifi.authorization.file.generated.Users users) {
        Set<User> allUsers = new HashSet<>();
        if (users == null || users.getUser() == null) {
            return allUsers;
        }

        for (org.apache.nifi.authorization.file.generated.User user : users.getUser()) {
            final User.Builder builder = new User.Builder()
                    .identity(user.getIdentity())
                    .identifier(user.getIdentifier());

            if (user.getGroup() != null) {
                for (org.apache.nifi.authorization.file.generated.User.Group group : user.getGroup()) {
                    builder.addGroup(group.getIdentifier());
                }
            }

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
    private Set<Group> createGroups(org.apache.nifi.authorization.file.generated.Groups groups,
                                    org.apache.nifi.authorization.file.generated.Users users) {
        Set<Group> allGroups = new HashSet<>();
        if (groups == null || groups.getGroup() == null) {
            return allGroups;
        }

        for (org.apache.nifi.authorization.file.generated.Group group : groups.getGroup()) {
            final Group.Builder builder = new Group.Builder()
                    .identifier(group.getIdentifier())
                    .name(group.getName());

            // need to figured out what users are in this group by going through the users list
            final Set<String> groupUsers = getUsersForGroup(users, group.getIdentifier());
            builder.addUsers(groupUsers);

            allGroups.add(builder.build());
        }

        return allGroups;
    }

    /**
     * Gets the set of user identifiers that are part of the given group.
     *
     * @param users the JAXB Users element
     * @param groupId the group id to get the users for
     * @return the user identifiers that belong to the group with the given identifier
     */
    private Set<String> getUsersForGroup(org.apache.nifi.authorization.file.generated.Users users, final String groupId) {
        Set<String> groupUsers = new HashSet<>();

        if (users != null && users.getUser()!= null) {
            for (org.apache.nifi.authorization.file.generated.User user : users.getUser()) {
                if (user.getGroup() != null) {
                    for (org.apache.nifi.authorization.file.generated.User.Group group : user.getGroup()) {
                        if (group.getIdentifier().equals(groupId)) {
                            groupUsers.add(user.getIdentifier());
                            break;
                        }
                    }
                }
            }
        }

        return groupUsers;
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

    /**
     *  Creates the initial admin user and policies for access the flow and managing users and policies.
     *
     * @param adminIdentity the identity of the admin user
     */
    private void populateInitialAdmin(final String adminIdentity) {
        // generate an identifier and add a User with the given identifier and identity
        final UUID adminIdentifier = UUID.nameUUIDFromBytes(adminIdentity.getBytes(StandardCharsets.UTF_8));
        final User adminUser = new User.Builder().identifier(adminIdentifier.toString()).identity(adminIdentity).build();
        addUser(adminUser);

        // grant the user read access to the /flow resource
        final AccessPolicy flowPolicy = createInitialAdminPolicy("/flow", adminIdentity, RequestAction.READ);
        addAccessPolicy(flowPolicy);

        // grant the user read/write access to the /users resource
        final AccessPolicy usersPolicy = createInitialAdminPolicy("/users", adminIdentity, RequestAction.READ, RequestAction.WRITE);
        addAccessPolicy(usersPolicy);

        // grant the user read/write access to the /policies resource
        final AccessPolicy policiesPolicy = createInitialAdminPolicy("/policies", adminIdentity, RequestAction.READ, RequestAction.WRITE);
        addAccessPolicy(policiesPolicy);
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

    @AuthorizerContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public void preDestruction() {

    }

    // ------------------ Groups ------------------

    @Override
    public Group addGroup(Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        // create a new JAXB Group based on the incoming Group
        final org.apache.nifi.authorization.file.generated.Group jaxbGroup = new org.apache.nifi.authorization.file.generated.Group();
        jaxbGroup.setIdentifier(group.getIdentifier());
        jaxbGroup.setName(group.getName());

        final Authorizations authorizations = this.authorizations.get();
        authorizations.getGroups().getGroup().add(jaxbGroup);
        saveAndLoad(authorizations);
        return groupsById.get().get(group.getIdentifier());
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }
        return groupsById.get().get(identifier);
    }

    @Override
    public Group updateGroup(Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        final Authorizations authorizations = this.authorizations.get();
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
        } else {
            updateGroup.setName(group.getName());
            saveAndLoad(authorizations);
            return groupsById.get().get(group.getIdentifier());
        }
    }

    @Override
    public Group deleteGroup(Group group) throws AuthorizationAccessException {
        final Authorizations authorizations = this.authorizations.get();
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
            saveAndLoad(authorizations);
            return group;
        } else {
            return null;
        }
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return this.allGroups.get();
    }

    // ------------------ Users ------------------

    @Override
    public User addUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final org.apache.nifi.authorization.file.generated.User jaxbUser = new org.apache.nifi.authorization.file.generated.User();
        jaxbUser.setIdentifier(user.getIdentifier());
        jaxbUser.setIdentity(user.getIdentity());

        for (String groupIdentifier : user.getGroups()) {
            org.apache.nifi.authorization.file.generated.User.Group group = new org.apache.nifi.authorization.file.generated.User.Group();
            group.setIdentifier(groupIdentifier);
            jaxbUser.getGroup().add(group);
        }

        final Authorizations authorizations = this.authorizations.get();
        authorizations.getUsers().getUser().add(jaxbUser);
        saveAndLoad(authorizations);
        return usersById.get().get(user.getIdentifier());
    }

    @Override
    public User getUser(final String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }
        return usersById.get().get(identifier);
    }

    @Override
    public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
        if (identity == null) {
            return null;
        }
        return usersByIdentity.get().get(identity);
    }

    @Override
    public User updateUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final Authorizations authorizations = this.authorizations.get();
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

            saveAndLoad(authorizations);
            return usersById.get().get(user.getIdentifier());
        }
    }

    @Override
    public User deleteUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final Authorizations authorizations = this.authorizations.get();
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
            saveAndLoad(authorizations);
            return user;
        } else {
            return null;
        }
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return this.allUsers.get();
    }

    // ------------------ AccessPolicies ------------------

    @Override
    public AccessPolicy addAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        // create the new JAXB Policy
        final Policy policy = new Policy();
        policy.setIdentifier(accessPolicy.getIdentifier());
        transferState(accessPolicy, policy);

        // add the new Policy to the top-level list of policies
        final Authorizations authorizations = this.authorizations.get();
        authorizations.getPolicies().getPolicy().add(policy);

        saveAndLoad(authorizations);
        return policiesById.get().get(accessPolicy.getIdentifier());
    }

    @Override
    public AccessPolicy getAccessPolicy(final String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }
        return policiesById.get().get(identifier);
    }

    @Override
    public AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        final Authorizations authorizations = this.authorizations.get();

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
        saveAndLoad(authorizations);
        return policiesById.get().get(accessPolicy.getIdentifier());
    }

    @Override
    public AccessPolicy deleteAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        final Authorizations authorizations = this.authorizations.get();

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

        saveAndLoad(authorizations);
        return accessPolicy;
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return allPolicies.get();
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies(final String resourceIdentifier) throws AuthorizationAccessException {
        if (resourceIdentifier == null) {
            return null;
        }
        return policiesByResource.get().get(resourceIdentifier);
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
