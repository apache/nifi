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

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.FormatUtils;

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
    private final static Map<String, User> usersById = new HashMap<>();   // id == identifier
    private final static Map<String, User> usersByName = new HashMap<>(); // name == identity
    private final static Map<String, Group> groupsById = new HashMap<>();

    public static final String INITIAL_REFRESH_DELAY_PROPERTY = "Initial Refresh Delay";
    public static final String REFRESH_DELAY_PROPERTY = "Refresh Delay";

    private static final long MINIMUM_SYNC_INTERVAL_MILLISECONDS = 10_000;
    private long initialDelay;
    private long fixedDelay;

    // Our scheduler has one thread for users, one for groups:
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // Our shell timeout, in seconds:
    @SuppressWarnings("FieldCanBeLocal")
    private final Integer shellTimeout = 10;

    // Commands selected during initialization:
    private ShellCommandsProvider selectedShellCommands;

    // Start of the UserGroupProvider implementation.  Javadoc strings
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
        // Our first init step is to fire off the refresh threads:
        initialDelay = getDelayProperty(configurationContext, INITIAL_REFRESH_DELAY_PROPERTY, "30 secs");
        fixedDelay = getDelayProperty(configurationContext, REFRESH_DELAY_PROPERTY, "30 secs");

        // Our next init step is to select the command set based on the operating system name:
        ShellCommandsProvider commands = getCommandsProvider();

        if (commands == null) {
            commands = getCommandsProviderFromName(null);
            setCommandsProvider(commands);
        }

        // Our next init step is to run the system check from that command set to determine if the other commands
        // will work on this host or not.
        try {
            ShellRunner.runShell(commands.getSystemCheck());
        } catch (final IOException ioexc) {
            logger.error("initialize exception: " + ioexc + " system check command: " + commands.getSystemCheck());
            throw new AuthorizerCreationException(SYS_CHECK_ERROR, ioexc.getCause());
        }

        // With our command set selected, and our system check passed, we can pull in the users and groups:
        refreshUsersAndGroups();

        // And finally, our last init step is to fire off the refresh thread:
        scheduler.scheduleWithFixedDelay(this::refreshUsersAndGroups, initialDelay, fixedDelay, TimeUnit.SECONDS);
    }

    private static ShellCommandsProvider getCommandsProviderFromName(String osName) {
        if (osName == null) {
            osName = System.getProperty("os.name");
        }

        ShellCommandsProvider commands;
        if (osName.startsWith("Linux")) {
            logger.debug("Selected Linux command set.");
            commands = new NssShellCommands();
        } else if (osName.startsWith("Mac OS X")) {
            logger.debug("Selected OSX command set.");
            commands = new OsxShellCommands();
        } else {
            throw new AuthorizerCreationException(OS_TYPE_ERROR);
        }
        return commands;
    }

    private long getDelayProperty(AuthorizerConfigurationContext context, String name, String defaultValue) {
        final PropertyValue intervalProperty = context.getProperty(name);
        final long syncInterval;
        final String propValue;

        if (intervalProperty.isSet()) {
            propValue = intervalProperty.getValue();
        } else {
            propValue = defaultValue;
        }

        try {
            syncInterval = Math.round(FormatUtils.getPreciseTimeDuration(propValue, TimeUnit.MILLISECONDS));
        } catch (final IllegalArgumentException ignored) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is not a valid time interval.", name, propValue));
        }

        if (syncInterval < MINIMUM_SYNC_INTERVAL_MILLISECONDS) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is below the minimum value of '%d ms'", name, propValue, MINIMUM_SYNC_INTERVAL_MILLISECONDS));
        }
        return syncInterval;
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

    /**
     * This is our entry point for user and group refresh.  This method runs the top-level
     * `getUserList()` and `getGroupsList()` shell commands, then passes those results to the
     * other methods for record parse, extract, and object construction.
     */
    private void refreshUsersAndGroups() {
        Map<String, User> uidToUser = new HashMap<>();
        Map<String, User> usernameToUser = new HashMap<>();
        Map<String, User> gidToUser = new HashMap<>();
        Map<String, Group> gidToGroup = new HashMap<>();

        List<String> userLines;
        List<String> groupLines;

        try {
            userLines = ShellRunner.runShell(selectedShellCommands.getUsersList(), "Get Users List");
            groupLines = ShellRunner.runShell(selectedShellCommands.getGroupsList(), "Get Groups List");
        } catch (final IOException ioexc)  {
            logger.error("refreshUsersAndGroups shell exception: " + ioexc);
            return;
        }

        rebuildUsers(userLines, uidToUser, usernameToUser, gidToUser);
        rebuildGroups(groupLines, gidToGroup);
        reconcilePrimaryGroups(gidToUser, gidToGroup);

        synchronized (usersById) {
            usersById.clear();
            usersById.putAll(uidToUser);
        }

        synchronized (usersByName) {
            usersByName.clear();
            usersByName.putAll(usernameToUser);
            logger.debug("users now size: " + usersByName.size());
        }

        synchronized (groupsById) {
            groupsById.clear();
            groupsById.putAll(gidToGroup);
            logger.debug("groups now size: " + groupsById.size());
        }
    }

    /**
     * This method parses the output of the `getUsersList()` shell command, where we expect the output
     * to look like `user-name:user-id:primary-group-id`.
     *
     * This method splits each output line on the ":" and attempts to build a User object
     * from the resulting name, uid, and primary gid.  Unusable records are logged.
     */
    private void rebuildUsers(List<String> userLines, Map<String, User> idToUser, Map<String, User> usernameToUser, Map<String, User> gidToUser) {
        userLines.forEach(line -> {
                String[] record = line.split(":");
                if (record.length > 2) {
                    String name = record[0], id = record[1], gid = record[2];

                    if (name != null && id != null && !name.equals("") && !id.equals("")) {

                        User user = new User.Builder().identity(name).identifier(id).build();
                        idToUser.put(id, user);
                        usernameToUser.put(name, user);

                        if (gid != null && !gid.equals("")) {
                            gidToUser.put(gid, user);
                        } else {
                            logger.warn("Null or empty primary group id for: " + name);
                        }

                    } else {
                        logger.warn("Null or empty user name: " + name + " or id: " + id);
                    }
                } else {
                    logger.warn("Unexpected record format.  Expected 3 or more colon separated values per line.");
                }
            });
    }

    /**
     * This method parses the output of the `getGroupsList()` shell command, where we expect the output
     * to look like `group-name:group-id`.
     *
     * This method splits each output line on the ":" and attempts to build a Group object
     * from the resulting name and gid.  Unusable records are logged.
     *
     * This command also runs the `getGroupMembers(username)` command once per group.  The expected output
     * of that command should look like `group-name-1,group-name-2`.
     */
    private void rebuildGroups(List<String> groupLines, Map<String, Group> groupsById) {
        groupLines.forEach(line -> {
                String[] record = line.split(":");
                if (record.length > 1) {
                    Set<String> users = new HashSet<>();
                    String name = record[0], id = record[1];

                    try {
                        List<String> memberLines = ShellRunner.runShell(selectedShellCommands.getGroupMembers(name));
                        // Use the first line only, and log if the line count isn't exactly one:
                        if (!memberLines.isEmpty()) {
                            users.addAll(Arrays.asList(memberLines.get(0).split(",")));
                        } else {
                            logger.error("refreshGroup list membership returned zero lines.");
                        }
                        if (memberLines.size() > 1) {
                            logger.error("refreshGroup list membership returned too many lines, only used the first.");
                        }

                    } catch (final IOException ioexc) {
                        logger.error("refreshUsersAndGroups list membership shell exception: " + ioexc);
                    }

                    if (name != null && id != null && !name.equals("") && !id.equals("")) {
                        Group group = new Group.Builder().name(name).identifier(id).addUsers(users).build();
                        groupsById.put(id, group);
                        logger.debug("Refreshed group: " + group);
                    } else {
                        logger.warn("Null or empty group name: " + name + " or id: " + id);
                    }
                } else {
                    logger.warn("Unexpected record format.  Expected 1 or more comma separated values.");
                }
            });
    }

    /**
     * This method parses the output of the `getGroupsList()` shell command, where we expect the output
     * to look like `group-name:group-id`.
     *
     * This method splits each output line on the ":" and attempts to build a Group object
     * from the resulting name and gid.
     */
    private void reconcilePrimaryGroups(Map<String, User> uidToUser, Map<String, Group> gidToGroup) {
        uidToUser.forEach((primaryGid, primaryUser) -> {
            Group primaryGroup = gidToGroup.get(primaryGid);

            if (primaryGroup == null) {
                logger.warn("user: " + primaryUser + " primary group not found");
            } else {
                Set<String> groupUsers = primaryGroup.getUsers();
                if (!groupUsers.contains(primaryUser.getIdentity())) {
                    Set<String> secondSet = new HashSet<>(groupUsers);
                    secondSet.add(primaryUser.getIdentity());
                    Group group = new Group.Builder().name(primaryGroup.getName()).identifier(primaryGid).addUsers(secondSet).build();
                    gidToGroup.put(primaryGid, group);
                }
            }
        });
    }

    public long getInitialRefreshDelay() {
        return initialDelay;
    }


    public long getRefreshDelay() {
        return fixedDelay;
    }


}
