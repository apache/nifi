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
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.components.PropertyValue;
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
import java.io.UncheckedIOException;
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
import java.util.regex.Pattern;

public class FileUserGroupProvider implements ConfigurableUserGroupProvider {

    private static final Logger logger = LoggerFactory.getLogger(FileUserGroupProvider.class);

    static final String PROP_TENANTS_FILE = "Users File";
    static final String PROP_INITIAL_USER_IDENTITY_PREFIX = "Initial User Identity ";
    static final Pattern INITIAL_USER_IDENTITY_PATTERN = Pattern.compile(PROP_INITIAL_USER_IDENTITY_PREFIX + "\\S+");
    static final String PROP_INITIAL_GROUP_IDENTITY_PREFIX = "Initial Group Identity ";
    static final Pattern INITIAL_GROUP_IDENTITY_PATTERN = Pattern.compile(PROP_INITIAL_GROUP_IDENTITY_PREFIX + "\\S+");

    private NiFiProperties properties;
    private File tenantsFile;
    private File restoreTenantsFile;
    private Set<String> initialUserIdentities;
    private Set<String> initialGroupIdentities;

    private final AtomicReference<UserGroupHolder> userGroupHolder = new AtomicReference<>();

    private final AuthorizedUserGroupsMapper fileAuthorizedUserGroupsMapper = new FileAuthorizedUserGroupsMapper();
    private final AuthorizedUserGroupsMapper fingerprintAuthorizedUserGroupMapper = new FingerprintAuthorizedUserGroupsMapper();

    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {

    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        try {
            final PropertyValue tenantsPath = configurationContext.getProperty(PROP_TENANTS_FILE);
            if (StringUtils.isBlank(tenantsPath.getValue())) {
                throw new AuthorizerCreationException("The users file must be specified.");
            }

            // get the tenants file and ensure it exists
            tenantsFile = new File(tenantsPath.getValue());
            if (!tenantsFile.exists()) {
                logger.info("Creating new users file at {}", tenantsFile.getAbsolutePath());
                saveAuthorizedUserGroups(new AuthorizedUserGroups(List.of(), List.of()), tenantsFile);
            }

            final File tenantsFileDirectory = tenantsFile.getAbsoluteFile().getParentFile();

            // the restore directory is optional and may be null
            final File restoreDirectory = properties.getRestoreDirectory();
            if (restoreDirectory != null) {
                // sanity check that restore directory is a directory, creating it if necessary
                FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

                // check that restore directory is not the same as the user's directory
                if (tenantsFileDirectory.getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
                    throw new AuthorizerCreationException(String.format("Users file directory '%s' is the same as restore directory '%s' ",
                            tenantsFileDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
                }

                // the restore copy will have same file name, but reside in a different directory
                restoreTenantsFile = new File(restoreDirectory, tenantsFile.getName());

                try {
                    // sync the primary copy with the restore copy
                    FileUtils.syncWithRestore(tenantsFile, restoreTenantsFile, logger);
                } catch (final IOException | IllegalStateException ioe) {
                    throw new AuthorizerCreationException(ioe);
                }
            }

            // extract the identity and group mappings from nifi.properties if any are provided
            List<IdentityMapping> identityMappings = Collections.unmodifiableList(IdentityMappingUtil.getIdentityMappings(properties));

            // extract any node identities
            initialUserIdentities = new HashSet<>();
            initialGroupIdentities = new HashSet<>();
            for (Map.Entry<String, String> entry : configurationContext.getProperties().entrySet()) {
                if (INITIAL_USER_IDENTITY_PATTERN.matcher(entry.getKey()).matches()) {
                    if (StringUtils.isNotBlank(entry.getValue())) {
                        initialUserIdentities.add(IdentityMappingUtil.mapIdentity(entry.getValue(), identityMappings));
                    }
                    continue;
                }

                if (INITIAL_GROUP_IDENTITY_PATTERN.matcher(entry.getKey()).matches()) {
                    if (StringUtils.isNotBlank(entry.getValue())) {
                        initialGroupIdentities.add(IdentityMappingUtil.mapIdentity(entry.getValue(), identityMappings));
                    }
                }
            }

            load();

            // if we've copied the authorizations file to a restore directory synchronize it
            if (restoreTenantsFile != null) {
                FileUtils.copyFile(tenantsFile, restoreTenantsFile, false, false, logger);
            }

            logger.debug("Users/Groups file loaded");
        } catch (IOException | AuthorizerCreationException | IllegalStateException e) {
            throw new AuthorizerCreationException(e);
        }
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return userGroupHolder.get().getAllUsers();
    }

    @Override
    public synchronized User addUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final AuthorizedUserGroups authorizedUserGroups = new AuthorizedUserGroups(List.of(user), List.of());
        addUsersAndGroups(authorizedUserGroups);

        return userGroupHolder.get().getUsersById().get(user.getIdentifier());
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }

        final UserGroupHolder holder = userGroupHolder.get();
        return holder.getUsersById().get(identifier);
    }

    @Override
    public synchronized User updateUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final String identifier = user.getIdentifier();
        final UserGroupHolder holder = userGroupHolder.get();
        final User foundUser = holder.getUsersById().get(identifier);
        if (foundUser == null) {
            return null;
        }

        final List<User> users = new ArrayList<>(holder.getAllUsers());
        final User updatedUser = new User.Builder().identifier(identifier).identity(user.getIdentity()).build();
        final ListIterator<User> updatedUsers = users.listIterator();
        while (updatedUsers.hasNext()) {
            final User currentUser = updatedUsers.next();
            if (identifier.equals(currentUser.getIdentifier())) {
                updatedUsers.remove();
                updatedUsers.add(updatedUser);
                break;
            }
        }

        final List<Group> groups = new ArrayList<>(holder.getAllGroups());
        final AuthorizedUserGroups authorizedUserGroups = new AuthorizedUserGroups(users, groups);
        saveAndRefreshHolder(authorizedUserGroups);
        return userGroupHolder.get().getUsersById().get(identifier);

    }

    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        if (identity == null) {
            return null;
        }

        final UserGroupHolder holder = userGroupHolder.get();
        return holder.getUsersByIdentity().get(identity);
    }

    @Override
    public synchronized User deleteUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final UserGroupHolder holder = userGroupHolder.get();

        final String identifier = user.getIdentifier();
        final User deletedUser = holder.getUsersById().get(identifier);
        if (deletedUser == null) {
            return null;
        }

        final List<Group> groups = new ArrayList<>(holder.getAllGroups());
        final ListIterator<Group> updatedGroups = groups.listIterator();
        while (updatedGroups.hasNext()) {
            final Group currentGroup = updatedGroups.next();
            final Set<String> updatedUsers = new HashSet<>(currentGroup.getUsers());
            if (updatedUsers.remove(identifier)) {
                updatedGroups.remove();

                final Group updatedGroup = new Group.Builder()
                        .identifier(currentGroup.getIdentifier())
                        .name(currentGroup.getName())
                        .addUsers(updatedUsers)
                        .build();
                updatedGroups.add(updatedGroup);
            }
        }

        final List<User> users = new ArrayList<>(holder.getAllUsers());
        final Iterator<User> updatedUsers = users.iterator();
        while (updatedUsers.hasNext()) {
            final User updatedUser = updatedUsers.next();
            if (identifier.equals(updatedUser.getIdentifier())) {
                updatedUsers.remove();
                break;
            }
        }

        final AuthorizedUserGroups authorizedUserGroups = new AuthorizedUserGroups(users, groups);
        saveAndRefreshHolder(authorizedUserGroups);
        return deletedUser;
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return userGroupHolder.get().getAllGroups();
    }

    private synchronized void addUsersAndGroups(final AuthorizedUserGroups authorizedUserGroups) {
        final UserGroupHolder holder = userGroupHolder.get();
        final AuthorizedUserGroups currentUserGroups = holder.getAuthorizedUserGroups();
        currentUserGroups.users().addAll(authorizedUserGroups.users());
        currentUserGroups.groups().addAll(authorizedUserGroups.groups());
        saveAndRefreshHolder(currentUserGroups);
    }

    @Override
    public synchronized Group addGroup(final Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        final AuthorizedUserGroups authorizedUserGroups = new AuthorizedUserGroups(List.of(), List.of(group));
        addUsersAndGroups(authorizedUserGroups);
        return userGroupHolder.get().getGroupsById().get(group.getIdentifier());
    }

    @Override
    public Group getGroup(final String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }
        return userGroupHolder.get().getGroupsById().get(identifier);
    }

    @Override
    public Group getGroupByName(final String name) throws AuthorizationAccessException {
        if (name == null) {
            return null;
        }

        return userGroupHolder.get().getGroupsByName().get(name);
    }

    @Override
    public UserAndGroups getUserAndGroups(final String identity) throws AuthorizationAccessException {
        final UserGroupHolder holder = userGroupHolder.get();
        final User user = holder.getUser(identity);
        final Set<Group> groups = holder.getGroups(identity);

        return new UserAndGroups() {
            @Override
            public User getUser() {
                return user;
            }

            @Override
            public Set<Group> getGroups() {
                return groups;
            }
        };
    }

    @Override
    public synchronized Group updateGroup(final Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        final UserGroupHolder holder = userGroupHolder.get();
        final String identifier = group.getIdentifier();
        final Group foundGroup = holder.getGroupsById().get(identifier);
        if (foundGroup == null) {
            return null;
        }

        final Group updatedGroup = new Group.Builder()
                .identifier(identifier)
                .name(group.getName())
                .addUsers(group.getUsers())
                .build();

        final List<Group> groups = new ArrayList<>(holder.getAllGroups());
        final ListIterator<Group> updatedGroups = groups.listIterator();
        while (updatedGroups.hasNext()) {
            final Group currentGroup = updatedGroups.next();
            if (currentGroup.getIdentifier().equals(identifier)) {
                updatedGroups.remove();
                updatedGroups.add(updatedGroup);
                break;
            }
        }

        final List<User> users = new ArrayList<>(holder.getAllUsers());
        final AuthorizedUserGroups authorizedUserGroups = new AuthorizedUserGroups(users, groups);
        saveAndRefreshHolder(authorizedUserGroups);
        return userGroupHolder.get().getGroupsById().get(identifier);
    }

    @Override
    public synchronized Group deleteGroup(final Group group) throws AuthorizationAccessException {
        Objects.requireNonNull(group, "Group required");

        final UserGroupHolder holder = userGroupHolder.get();
        final String identifier = group.getIdentifier();
        final Group foundGroup = holder.getGroupsById().get(identifier);
        if (foundGroup == null) {
            return null;
        }

        final List<Group> groups = new ArrayList<>(holder.getAllGroups());
        final ListIterator<Group> updatedGroups = groups.listIterator();
        while (updatedGroups.hasNext()) {
            final Group currentGroup = updatedGroups.next();
            if (identifier.equals(currentGroup.getIdentifier())) {
                updatedGroups.remove();
                break;
            }
        }

        final List<User> users = new ArrayList<>(holder.getAllUsers());
        final AuthorizedUserGroups authorizedUserGroups = new AuthorizedUserGroups(users, groups);
        saveAndRefreshHolder(authorizedUserGroups);
        return foundGroup;
    }

    @AuthorizerContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public synchronized void inheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        final AuthorizedUserGroups authorizedUserGroups = parseUsersAndGroups(fingerprint);
        inherit(authorizedUserGroups);
    }

    private synchronized void inherit(final AuthorizedUserGroups authorizedUserGroups) {
        addUsersAndGroups(authorizedUserGroups);
    }

    @Override
    public synchronized void forciblyInheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        if (fingerprint == null || fingerprint.isBlank()) {
            logger.info("Inheriting empty Users and Groups: Backup of current Users and Groups started");
            backupUsersAndGroups();
            purgeUsersAndGroups();
            return;
        }

        final AuthorizedUserGroups authorizedUserGroups = parseUsersAndGroups(fingerprint);
        if (isInheritable()) {
            logger.debug("Inheriting Users and Groups");
            inherit(authorizedUserGroups);
        } else {
            logger.info("Failed to inherit Users and Groups: Backup of current Users and Groups before replacing configuration");
            backupUsersAndGroups();
            purgeUsersAndGroups();
            addUsersAndGroups(authorizedUserGroups);
        }
    }

    public void backupUsersAndGroups() throws AuthorizationAccessException {
        final UserGroupHolder holder = userGroupHolder.get();
        final AuthorizedUserGroups authorizedUserGroups = holder.getAuthorizedUserGroups();

        final String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").format(OffsetDateTime.now());
        final File destinationFile = new File(tenantsFile.getParentFile(), tenantsFile.getName() + "." + timestamp);
        logger.info("Writing backup of Users & Groups to {}", destinationFile.getAbsolutePath());

        try {
            saveAuthorizedUserGroups(authorizedUserGroups, destinationFile);
        } catch (final ProcessingException e) {
            throw new AuthorizationAccessException("Could not backup existing Users and Groups so will not inherit new Users and Groups", e);
        }
    }

    public synchronized void purgeUsersAndGroups() {
        final AuthorizedUserGroups authorizedUserGroups = new AuthorizedUserGroups(List.of(), List.of());
        saveAndRefreshHolder(authorizedUserGroups);
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException {
        // ensure we are in a proper state to inherit the fingerprint
        if (!isInheritable()) {
            throw new UninheritableAuthorizationsException("Proposed fingerprint is not inheritable because the current users and groups is not empty");
        }
    }

    private boolean isInheritable() {
        final UserGroupHolder usersAndGroups = userGroupHolder.get();
        return usersAndGroups.getAllUsers().isEmpty() && usersAndGroups.getAllGroups().isEmpty();
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        final UserGroupHolder holder = userGroupHolder.get();
        final AuthorizedUserGroups authorizedUserGroups = holder.getAuthorizedUserGroups();
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            fingerprintAuthorizedUserGroupMapper.writeUserGroups(authorizedUserGroups, outputStream);
            outputStream.flush();
            return outputStream.toString(StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new UncheckedIOException("User Group Fingerprint generation failed", e);
        }
    }

    private AuthorizedUserGroups parseUsersAndGroups(final String fingerprint) {
        final byte[] fingerprintBytes = fingerprint.getBytes(StandardCharsets.UTF_8);
        try (final InputStream inputStream = new ByteArrayInputStream(fingerprintBytes)) {
            return fingerprintAuthorizedUserGroupMapper.readUserGroups(inputStream);
        } catch (final ProcessingException | IOException e) {
            throw new AuthorizationAccessException("User Group Fingerprint parsing failed", e);
        }
    }

    private synchronized void load() {
        final AuthorizedUserGroups authorizedUserGroups;
        try (InputStream inputStream = new FileInputStream(tenantsFile)) {
            authorizedUserGroups = fileAuthorizedUserGroupsMapper.readUserGroups(inputStream);
        } catch (final IOException | ProcessingException e) {
            throw new IllegalStateException("Loading Users and Groups [%s] failed".formatted(tenantsFile), e);
        }

        if (authorizedUserGroups.users().isEmpty() && authorizedUserGroups.groups().isEmpty()) {
            final List<User> users = new ArrayList<>();
            for (final String identity : initialUserIdentities) {
                final User user = new User.Builder().identity(identity).identifierGenerateFromSeed(identity).build();
                users.add(user);
            }

            final List<Group> groups = new ArrayList<>();
            for (final String name : initialGroupIdentities) {
                final Group group = new Group.Builder().name(name).identifierGenerateFromSeed(name).build();
                groups.add(group);
            }

            final AuthorizedUserGroups initialAuthorizedUserGroups = new AuthorizedUserGroups(users, groups);
            saveAndRefreshHolder(initialAuthorizedUserGroups);
        } else {
            final UserGroupHolder userGroupHolder = new UserGroupHolder(authorizedUserGroups);
            this.userGroupHolder.set(userGroupHolder);
        }
    }

    private void saveAuthorizedUserGroups(final AuthorizedUserGroups authorizedUserGroups, final File destinationFile) {
        try (OutputStream outputStream = new FileOutputStream(destinationFile)) {
            fileAuthorizedUserGroupsMapper.writeUserGroups(authorizedUserGroups, outputStream);
        } catch (final IOException e) {
            throw new ProcessingException("Failed to save Tenants [%s]".formatted(destinationFile), e);
        }
    }

    /**
     * Saves the Authorizations instance by marshalling to a file, then re-populates the
     * in-memory data structures and sets the new holder.
     * Synchronized to ensure only one thread writes the file at a time.
     *
     * @param authorizedUserGroups Authorized Users and Groups
     * @throws AuthorizationAccessException if an error occurs saving the authorizations
     */
    private synchronized void saveAndRefreshHolder(final AuthorizedUserGroups authorizedUserGroups) throws AuthorizationAccessException {
        try {
            saveAuthorizedUserGroups(authorizedUserGroups, tenantsFile);
            userGroupHolder.set(new UserGroupHolder(authorizedUserGroups));
        } catch (final ProcessingException e) {
            throw new AuthorizationAccessException("Unable to save Users and Groups", e);
        }
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
    }
}
