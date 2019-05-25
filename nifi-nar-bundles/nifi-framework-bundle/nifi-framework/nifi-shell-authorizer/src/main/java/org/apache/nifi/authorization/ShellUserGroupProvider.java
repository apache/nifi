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
import org.apache.nifi.authorization.util.ShellRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/*
 * ShellUserGroupProvider implements UserGroupProvider by way of shell commands.
 */
public class ShellUserGroupProvider implements UserGroupProvider {
    private final static Logger logger = LoggerFactory.getLogger(ShellUserGroupProvider.class);

    private final static String OS_TYPE_ERROR = "Unsupported operating system.";
    private final static String SYS_CHECK_ERROR = "System check failed - cannot provide users and groups.";
    private final Map<String, User> usersById = new HashMap<>();   // id == identifier
    private final Map<String, User> usersByName = new HashMap<>(); // name == identity
    private final Map<String, Group> groupsById = new HashMap<>();

    // Our scheduler has one thread for users, one for groups:
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // Our shell timeout, in seconds:
    @SuppressWarnings("FieldCanBeLocal")
    private final Integer shellTimeout = 10;

    // Commands selected during initialization:
    private ShellCommandsProvider selectedShellCommands;

    // Start of the UserGroupProvider implementation.  Docstrings
    // copied from the interface definition for reference.

    /**
     * Retrieves all users. Must be non null
     *
     * @return a list of users
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        synchronized (usersById) {
            logger.debug("getUsers has user set of size: " + usersById.size());
            return new HashSet<>(usersById.values());
        }
    }

    /**
     * Retrieves the user with the given identifier.
     *
     * @param identifier the id of the user to retrieve
     * @return the user with the given id, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        synchronized (usersById) {
            User user = usersById.get(identifier);
            if (user == null) {
                logger.debug("getUser user not found: " + identifier);
            } else {
                logger.debug("getUser has user: " + user);
            }
            return user;
        }
    }

    /**
     * Retrieves the user with the given identity.
     *
     * @param identity the identity of the user to retrieve
     * @return the user with the given identity, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        synchronized (usersByName) {
            User user = usersByName.get(identity);
            logger.debug("getUserByIdentity has user: " + user);
            return user;
        }
    }

    /**
     * Retrieves all groups. Must be non null
     *
     * @return a list of groups
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        synchronized (groupsById) {
            logger.debug("getGroups has group set of size: " + groupsById.size());
            return new HashSet<>(groupsById.values());
        }
    }

    /**
     * Retrieves a Group by id.
     *
     * @param identifier the identifier of the Group to retrieve
     * @return the Group with the given identifier, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        synchronized (groupsById) {
            Group group = groupsById.get(identifier);
            logger.debug("getGroup has group: " + group);
            return group;
        }
    }

    /**
     * Gets a user and their groups. Must be non null. If the user is not known the UserAndGroups.getUser() and
     * UserAndGroups.getGroups() should return null
     *
     * @return the UserAndGroups for the specified identity
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    @Override
    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
        User user = getUser(identity);
        Set<Group> groups = new HashSet<>();

        for (Group g: getGroups()) {
            if (user != null && g.getUsers().contains(user.getIdentity())) {
                groups.add(g);
            }
        }

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

    /**
     * Called immediately after instance creation for implementers to perform additional setup
     *
     * @param initializationContext in which to initialize
     */
    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
    }

    /**
     * Called to configure the Authorizer.
     *
     * @param configurationContext at the time of configuration
     * @throws AuthorizerCreationException for any issues configuring the provider
     */
    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        // Our first init step is to select the command set based on the
        // operating system name:
        final String osName = System.getProperty("os.name");
        ShellCommandsProvider commands = getCommandsProvider();

        if (commands == null) {
            if (osName.startsWith("Linux")) {
                logger.debug("Selected Linux command set.");
                commands = new NssShellCommands();
            } else if (osName.startsWith("Mac OS X")) {
                logger.debug("Selected OSX command set.");
                commands = new OsxShellCommands();
            } else {
                throw new AuthorizerCreationException(OS_TYPE_ERROR);
            }
            setCommandsProvider(commands);
        }

        // Our second init step is to run the system check from that
        // command set to determine if the other commands will work on
        // this host or not.
        try {
            ShellRunner.runShell(commands.getSystemCheck());
        } catch (final IOException ioexc) {
            logger.error("initialize exception: " + ioexc + " system check command: " + commands.getSystemCheck());
            throw new AuthorizerCreationException(SYS_CHECK_ERROR, ioexc.getCause());
        }

        // With our command set selected, and our system check passed,
        // we can pull in the users and groups:
        refreshUsers();
        refreshGroups();

        // Our last init step is to fire off the refresh threads:
        Integer initialDelay = 30, fixedDelay = 30;
        scheduler.scheduleWithFixedDelay(this::refreshUsers, initialDelay, fixedDelay, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::refreshGroups, initialDelay, fixedDelay, TimeUnit.SECONDS);
    }

    /**
     * Called immediately before instance destruction for implementers to release resources.
     *
     * @throws AuthorizerDestructionException If pre-destruction fails.
     */
    @Override
    public void preDestruction() throws AuthorizerDestructionException {
        try {
            scheduler.shutdownNow();
        } catch (final Exception ignored) {
        }
    }

    public ShellCommandsProvider getCommandsProvider() {
        return selectedShellCommands;
    }

    public void setCommandsProvider(ShellCommandsProvider commandsProvider) {
        selectedShellCommands = commandsProvider;
    }

    private void refreshUsers() {
        Map<String, User> byId = new HashMap<>();
        Map<String, User> byName = new HashMap<>();
        List<String> lines;

        try {
            lines = ShellRunner.runShell(selectedShellCommands.getUsersList());
        } catch (final IOException ioexc)  {
            logger.error("refreshUsers shell exception: " + ioexc);
            return;
        }

        lines.forEach(line -> {
                String[] record = line.split(":");
                if (record.length > 1) {
                    String name = record[0],
                        id = record[1];
                    if (name != null && id != null && !name.equals("") && !id.equals("")) {
                        User user = new User.Builder().identity(name).identifier(id).build();
                        byId.put(id, user);
                        byName.put(name, user);
                        logger.debug("refreshed user: " + user);
                    } else {
                        logger.warn("null or empty user name: " + name + " or id: " + id);
                    }
                }
            });

        synchronized (usersById) {
            usersById.clear();
            usersById.putAll(byId);
        }

        synchronized (usersByName) {
            usersByName.clear();
            usersByName.putAll(byName);
            logger.debug("refreshUsers users now size: " + usersByName.size());
        }
    }

    private void refreshGroups() {
        Map<String, Group> groups = new HashMap<>();
        List<String> lines;

        try {
            lines = ShellRunner.runShell(selectedShellCommands.getGroupsList());
        } catch (final IOException ioexc) {
            logger.error("refreshGroups list groups shell exception: " + ioexc);
            return;
        }

        lines.forEach(line -> {
                String[] record = line.split(":");
                if (record.length > 1) {
                    Set<String> users = new HashSet<>();
                    String name = record[0], id = record[1];

                    try {
                        List<String> userLines = ShellRunner.runShell(String.format(selectedShellCommands.getGroupMembers(), name));
                        if (userLines.size() > 0) {
                            users.addAll(Arrays.asList(userLines.get(0).split(",")));
                        }
                    } catch (final IOException ioexc) {
                        logger.error("refreshGroups list membership shell exception: " + ioexc);
                    }

                    if (name != null && id != null && !name.equals("") && !id.equals("")) {
                        Group group = new Group.Builder().name(name).identifier(id).addUsers(users).build();
                        groups.put(id, group);
                        logger.debug("refreshed group: " + group);
                    } else {
                        logger.warn("null or empty group name: " + name + " or id: " + id);
                    }
                }
            });

        List<String> sublines;
        try {
            sublines = ShellRunner.runShell(selectedShellCommands.getUsersList());
        } catch (final IOException ioexc) {
            logger.error("refreshGroups list groups shell exception: " + ioexc);
            return;
        }

        sublines.forEach(line -> {
                String[] subrecord = line.split(":");

                if (subrecord.length > 2) {
                    String primaryGid = subrecord[2];
                    Group primaryGroup = groups.get(primaryGid);
                    Group group;

                    if (primaryGroup == null) {
                        logger.warn("user: " + subrecord[0] + " primary group not found");
                    } else {
                        Set<String> groupUsers = primaryGroup.getUsers();
                        if (!groupUsers.contains(subrecord[0])) {
                            Set<String> secondSet = new HashSet<>();
                            secondSet.addAll(groupUsers);
                            secondSet.add(subrecord[0]);
                            group = new Group.Builder().name( primaryGroup.getName() ).identifier(primaryGid).addUsers(secondSet).build();
                            groups.put(primaryGid, group);
                        }
                    }
                }
            });


        synchronized (groupsById) {
            groupsById.clear();
            groupsById.putAll(groups);
            logger.debug("refreshGroups groups now size: " + groupsById.size());
        }
    }
}
