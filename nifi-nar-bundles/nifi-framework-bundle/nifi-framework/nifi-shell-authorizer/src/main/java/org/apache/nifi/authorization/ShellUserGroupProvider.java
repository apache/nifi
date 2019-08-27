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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.util.ShellRunner;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

    public static final String REFRESH_DELAY_PROPERTY = "Refresh Delay";
    private static final long MINIMUM_SYNC_INTERVAL_MILLISECONDS = 10_000;

    public static final String EXCLUDE_USER_PROPERTY = "Exclude Users";
    public static final String EXCLUDE_GROUP_PROPERTY = "Exclude Groups";

    private long fixedDelay;
    private Pattern excludeUsers;
    private Pattern excludeGroups;

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
        User user;

        synchronized (usersById) {
            user = usersById.get(identifier);
        }

        if (user == null) {
            logger.debug("getUser (by id) user not found: " + identifier);
        } else {
            logger.debug("getUser (by id) found user: " + user + " for id: " + identifier);
        }
        return user;
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
        User user;

        synchronized (usersByName) {
            user = usersByName.get(identity);
        }

        if (user == null) {
            refreshOneUser(selectedShellCommands.getUserByName(identity), "Get Single User by Name");
            user = usersByName.get(identity);
        }

        if (user == null) {
            logger.debug("getUser (by name) user not found: " + identity);
        } else {
            logger.debug("getUser (by name) found user: " + user + " for name: " + identity);
        }
        return user;
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
     * Retrieves a Group by Id.
     *
     * @param identifier the identifier of the Group to retrieve
     * @return the Group with the given identifier, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        Group group;

        synchronized (groupsById) {
            group = groupsById.get(identifier);
        }

        if (group == null) {
            refreshOneGroup(selectedShellCommands.getGroupById(identifier), "Get Single Group by Id");
            group = groupsById.get(identifier);
        }

        if (group == null) {
            logger.debug("getGroup (by id) group not found: " + identifier);
        } else {
            logger.debug("getGroup (by id) found group: " + group + " for id: " + identifier);
        }
        return group;

    }

    /**
     * Gets a user and their groups.
     *
     * @return the UserAndGroups for the specified identity
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    @Override
    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
        User user = getUserByIdentity(identity);
        logger.debug("Retrieved user {} for identity {}", new Object[]{user, identity});

        Set<Group> groups = new HashSet<>();

        for (Group g : getGroups()) {
            if (user != null && g.getUsers().contains(user.getIdentity())) {
                groups.add(g);
            }
        }

        if (groups.isEmpty()) {
            logger.debug("User {} belongs to no groups", user);
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
        fixedDelay = getDelayProperty(configurationContext, REFRESH_DELAY_PROPERTY, "5 mins");

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

        // The next step is to add the user and group exclude regexes:
        try {
            excludeGroups = Pattern.compile(getProperty(configurationContext, EXCLUDE_GROUP_PROPERTY, ""));
            excludeUsers = Pattern.compile(getProperty(configurationContext, EXCLUDE_USER_PROPERTY, ""));
        } catch (final PatternSyntaxException e) {
            throw new AuthorizerCreationException(e);
        }

        // With our command set selected, and our system check passed, we can pull in the users and groups:
        refreshUsersAndGroups();

        // And finally, our last init step is to fire off the refresh thread:
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                refreshUsersAndGroups();
            }catch (final Throwable t) {
                logger.error("", t);
            }
        }, fixedDelay, fixedDelay, TimeUnit.SECONDS);

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

    private String getProperty(AuthorizerConfigurationContext authContext, String propertyName, String defaultValue) {
        final PropertyValue property = authContext.getProperty(propertyName);
        final String value;

        if (property != null && property.isSet()) {
            value = property.getValue();
        } else {
            value = defaultValue;
        }
        return value;

    }

    private long getDelayProperty(AuthorizerConfigurationContext authContext, String propertyName, String defaultValue) {
        final PropertyValue intervalProperty = authContext.getProperty(propertyName);
        final String propertyValue;
        final long syncInterval;

        if (intervalProperty.isSet()) {
            propertyValue = intervalProperty.getValue();
        } else {
            propertyValue = defaultValue;
        }

        try {
            syncInterval = Math.round(FormatUtils.getPreciseTimeDuration(propertyValue, TimeUnit.MILLISECONDS));
        } catch (final IllegalArgumentException ignored) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is not a valid time interval.", propertyName, propertyValue));
        }

        if (syncInterval < MINIMUM_SYNC_INTERVAL_MILLISECONDS) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is below the minimum value of '%d ms'", propertyName, propertyValue, MINIMUM_SYNC_INTERVAL_MILLISECONDS));
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
     * Refresh a single user.
     *
     * @param command     Shell command to read a single user.  Pre-formatted by caller.
     * @param description Shell command description.
     */
    private void refreshOneUser(String command, String description) {
        if (command != null) {
            Map<String, User> idToUser = new HashMap<>();
            Map<String, User> usernameToUser = new HashMap<>();
            Map<String, User> gidToUser = new HashMap<>();
            List<String> userLines;

            try {
                userLines = ShellRunner.runShell(command, description);
                rebuildUsers(userLines, idToUser, usernameToUser, gidToUser);
            } catch (final IOException ioexc) {
                logger.error("refreshOneUser shell exception: " + ioexc);
            }

            if (idToUser.size() > 0) {
                synchronized (usersById) {
                    usersById.putAll(idToUser);
                }
            }

            if (usernameToUser.size() > 0) {
                synchronized (usersByName) {
                    usersByName.putAll(usernameToUser);
                }
            }
        } else {
            logger.info("Get Single User not supported on this system.");
        }
    }

    /**
     * Refresh a single group.
     *
     * @param command     Shell command to read a single group.  Pre-formatted by caller.
     * @param description Shell command description.
     */
    private void refreshOneGroup(String command, String description) {
        if (command != null) {
            Map<String, Group> gidToGroup = new HashMap<>();
            List<String> groupLines;

            try {
                groupLines = ShellRunner.runShell(command, description);
                rebuildGroups(groupLines, gidToGroup);
            } catch (final IOException ioexc) {
                logger.error("refreshOneGroup shell exception: " + ioexc);
            }

            if (gidToGroup.size() > 0) {
                synchronized (groupsById) {
                    groupsById.putAll(gidToGroup);
                }
            }
        } else {
            logger.info("Get Single Group not supported on this system.");
        }
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
        } catch (final IOException ioexc) {
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
     * <p>
     * This method splits each output line on the ":" and attempts to build a User object
     * from the resulting name, uid, and primary gid.  Unusable records are logged.
     */
    private void rebuildUsers(List<String> userLines, Map<String, User> idToUser, Map<String, User> usernameToUser, Map<String, User> gidToUser) {
        userLines.forEach(line -> {
            String[] record = line.split(":");
            if (record.length > 2) {
                String name = record[0], id = record[1], gid = record[2];

                if (name != null && id != null && !name.equals("") && !id.equals("") && !excludeUsers.matcher(name).matches()) {
                    String identifier = getNameBasedUUID(id);
                    User user = new User.Builder().identity(name).identifier(identifier).build();
                    idToUser.put(identifier, user);
                    usernameToUser.put(name, user);

                    if (gid != null && !gid.equals("")) {
                        String groupIdentifier = getNameBasedUUID(gid);
                        gidToUser.put(groupIdentifier, user);
                    } else {
                        logger.warn("Null or empty primary group id for: " + name);
                    }

                } else {
                    logger.warn("Null, empty, or skipped user name: " + name + " or id: " + id);
                }
            } else {
                logger.warn("Unexpected record format.  Expected 3 or more colon separated values per line.");
            }
        });
    }

    /**
     * This method parses the output of the `getGroupsList()` shell command, where we expect the output
     * to look like `group-name:group-id`.
     * <p>
     * This method splits each output line on the ":" and attempts to build a Group object
     * from the resulting name and gid.  Unusable records are logged.
     * <p>
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
                        logger.debug("list membership returned zero lines.");
                    }
                    if (memberLines.size() > 1) {
                        logger.error("list membership returned too many lines, only used the first.");
                    }

                } catch (final IOException ioexc) {
                    logger.error("list membership shell exception: " + ioexc);
                }

                if (name != null && id != null && !name.equals("") && !id.equals("") && !excludeGroups.matcher(name).matches()) {
                    String groupIdentifier = getNameBasedUUID(id);
                    Group group = new Group.Builder().name(name).identifier(groupIdentifier).addUsers(users).build();
                    groupsById.put(groupIdentifier, group);
                    logger.debug("Refreshed group: " + group);
                } else {
                    logger.warn("Null, empty, or skipped group name: " + name + " or id: " + id);
                }
            } else {
                logger.warn("Unexpected record format.  Expected 1 or more comma separated values.");
            }
        });
    }

    /**
     * This method parses the output of the `getGroupsList()` shell command, where we expect the output
     * to look like `group-name:group-id`.
     * <p>
     * This method splits each output line on the ":" and attempts to build a Group object
     * from the resulting name and gid.
     */
    private void reconcilePrimaryGroups(Map<String, User> uidToUser, Map<String, Group> gidToGroup) {
        uidToUser.forEach((primaryGid, primaryUser) -> {
            Group primaryGroup = gidToGroup.get(primaryGid);

            if (primaryGroup == null) {
                logger.warn("user: " + primaryUser + " primary group not found");
            } else if (!excludeGroups.matcher(primaryGroup.getName()).matches()) {
                Set<String> groupUsers = primaryGroup.getUsers();
                if (!groupUsers.contains(primaryUser.getIdentity())) {
                    Set<String> secondSet = new HashSet<>(groupUsers);
                    secondSet.add(primaryUser.getIdentity());
                    String groupIdentifier = getNameBasedUUID(primaryGid);
                    Group group = new Group.Builder().name(primaryGroup.getName()).identifier(groupIdentifier).addUsers(secondSet).build();
                    gidToGroup.put(groupIdentifier, group);
                }
            }
        });
    }

    /**
     * Returns a deterministic UUID derived from the input value.
     *
     * @param name determines UUID
     * @return string UUID
     */
    private static String getNameBasedUUID(String name) {
        return new Group.Builder().identifierGenerateFromSeed(name).name(name).build().getIdentifier();
    }

    /**
     * @return The fixed refresh delay.
     */
    public long getRefreshDelay() {
        return fixedDelay;
    }

    /**
     * Testing concession for clearing the internal caches.
     */
    void clearCaches() {
        synchronized (usersById) {
            usersById.clear();
        }

        synchronized (usersByName) {
            usersByName.clear();
        }

        synchronized (groupsById) {
            groupsById.clear();
        }
    }

    /**
     * @return The size of the internal user cache.
     */
    public int userCacheSize() {
        return usersById.size();
    }

    /**
     * @return The size of the internal group cache.
     */
    public int groupCacheSize() {
        return groupsById.size();
    }
}
