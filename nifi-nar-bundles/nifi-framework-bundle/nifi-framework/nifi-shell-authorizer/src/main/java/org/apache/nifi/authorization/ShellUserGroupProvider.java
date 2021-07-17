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
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


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
    private final static Map<String, Group> groupsByName = new HashMap<>();

    public static final String REFRESH_DELAY_PROPERTY = "Refresh Delay";
    private static final long MINIMUM_SYNC_INTERVAL_MILLISECONDS = 10_000;

    public static final String EXCLUDE_USER_PROPERTY = "Exclude Users";
    public static final String EXCLUDE_GROUP_PROPERTY = "Exclude Groups";
    public static final String LEGACY_IDENTIFIER_MODE = "Legacy Identifier Mode";
    public static final String COMMAND_TIMEOUT_PROPERTY = "Command Timeout";

    private static final String DEFAULT_COMMAND_TIMEOUT = "60 seconds";

    private long fixedDelay;
    private Pattern excludeUsers;
    private Pattern excludeGroups;
    private boolean legacyIdentifierMode;
    private int timeoutSeconds;

    // Our scheduler has one thread for users, one for groups:
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // Commands selected during initialization:
    private ShellCommandsProvider selectedShellCommands;

    private ShellRunner shellRunner;

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
            logger.debug("getUser (by name) found user: " + user.getIdentity() + " for name: " + identity);
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
            logger.debug("getGroup (by id) found group: {} for id: {}", group.getName(), identifier);
        }
        return group;

    }

    @Override
    public Group getGroupByName(String name) throws AuthorizationAccessException {
        Group group;

        synchronized (groupsByName) {
            group = groupsByName.get(name);
        }

        if (group == null) {
            logger.debug("getGroup (by name) group not found: " + name);
        } else {
            logger.debug("getGroup (by name) found group: {} for name: {}", group.getName(), name);
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
        if (user != null) {
            for (Group g : getGroups()) {
                if (g.getUsers().contains(user.getIdentifier())) {
                    logger.debug("User {} belongs to group {}", new Object[]{user.getIdentity(), g.getName()});
                    groups.add(g);
                }
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
        logger.info("Configuring ShellUserGroupProvider");

        fixedDelay = getDelayProperty(configurationContext, REFRESH_DELAY_PROPERTY, "5 mins");
        timeoutSeconds = getTimeoutProperty(configurationContext, COMMAND_TIMEOUT_PROPERTY, DEFAULT_COMMAND_TIMEOUT);
        shellRunner = new ShellRunner(timeoutSeconds);
        logger.debug("Configured ShellRunner with command timeout of '{}' seconds", new Object[]{timeoutSeconds});

        // Our next init step is to select the command set based on the operating system name:
        ShellCommandsProvider commands = getCommandsProvider();

        if (commands == null) {
            commands = getCommandsProviderFromName(null);
            setCommandsProvider(commands);
        }

        // Our next init step is to run the system check from that command set to determine if the other commands
        // will work on this host or not.
        try {
            shellRunner.runShell(commands.getSystemCheck());
        } catch (final Exception e) {
            logger.error("initialize exception: " + e + " system check command: " + commands.getSystemCheck());
            throw new AuthorizerCreationException(SYS_CHECK_ERROR, e);
        }

        // The next step is to add the user and group exclude regexes:
        try {
            excludeGroups = Pattern.compile(getProperty(configurationContext, EXCLUDE_GROUP_PROPERTY, ""));
            excludeUsers = Pattern.compile(getProperty(configurationContext, EXCLUDE_USER_PROPERTY, ""));
        } catch (final PatternSyntaxException e) {
            throw new AuthorizerCreationException(e);
        }

        // Get the value for Legacy Identifier Mo
        legacyIdentifierMode = Boolean.parseBoolean(getProperty(configurationContext, LEGACY_IDENTIFIER_MODE, "true"));

        // With our command set selected, and our system check passed, we can pull in the users and groups:
        refreshUsersAndGroups();

        // And finally, our last init step is to fire off the refresh thread:
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                refreshUsersAndGroups();
            }catch (final Throwable t) {
                logger.error("", t);
            }
        }, fixedDelay, fixedDelay, TimeUnit.MILLISECONDS);

        logger.info("Completed configuration of ShellUserGroupProvider");
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

    private int getTimeoutProperty(AuthorizerConfigurationContext authContext, String propertyName, String defaultValue) {
        final PropertyValue timeoutProperty = authContext.getProperty(propertyName);

        final String propertyValue;
        if (timeoutProperty.isSet()) {
            propertyValue = timeoutProperty.getValue();
        } else {
            propertyValue = defaultValue;
        }

        final long timeoutValue;
        try {
            timeoutValue = Math.round(FormatUtils.getPreciseTimeDuration(propertyValue, TimeUnit.SECONDS));
        } catch (final IllegalArgumentException ignored) {
            throw new AuthorizerCreationException(String.format("The %s '%s' is not a valid time interval.", propertyName, propertyValue));
        }

        return Math.toIntExact(timeoutValue);
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
        } catch (final Exception e) {
            logger.warn("Error shutting down refresh scheduler: " + e.getMessage(), e);
        }
        try {
            shellRunner.shutdown();
        } catch (final Exception e) {
            logger.warn("Error shutting down ShellRunner: " + e.getMessage(), e);
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
                userLines = shellRunner.runShell(command, description);
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
                groupLines = shellRunner.runShell(command, description);
                rebuildGroups(groupLines, gidToGroup);
            } catch (final IOException ioexc) {
                logger.error("refreshOneGroup shell exception: " + ioexc);
            }

            if (gidToGroup.size() > 0) {
                synchronized (groupsById) {
                    groupsById.putAll(gidToGroup);
                }
                synchronized (groupsByName) {
                    gidToGroup.values().forEach(g -> groupsByName.put(g.getName(), g));
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
        final long startTime = System.currentTimeMillis();

        Map<String, User> uidToUser = new HashMap<>();
        Map<String, User> usernameToUser = new HashMap<>();
        Map<String, User> gidToUser = new HashMap<>();
        Map<String, Group> gidToGroup = new HashMap<>();

        List<String> userLines;
        List<String> groupLines;

        try {
            userLines = shellRunner.runShell(selectedShellCommands.getUsersList(), "Get Users List");
            groupLines = shellRunner.runShell(selectedShellCommands.getGroupsList(), "Get Groups List");
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

            if (logger.isTraceEnabled()) {
                logger.trace("=== Users by id...");
                Set<User> sortedUsers = new TreeSet<>(Comparator.comparing(User::getIdentity));
                sortedUsers.addAll(usersById.values());
                sortedUsers.forEach(u -> logger.trace("=== " + u.toString()));
            }
        }

        synchronized (usersByName) {
            usersByName.clear();
            usersByName.putAll(usernameToUser);
            logger.debug("users now size: " + usersByName.size());
        }

        synchronized (groupsById) {
            groupsById.clear();
            groupsById.putAll(gidToGroup);
            logger.debug("groupsById now size: " + groupsById.size());

            if (logger.isTraceEnabled()) {
                logger.trace("=== Groups by id...");
                Set<Group> sortedGroups = new TreeSet<>(Comparator.comparing(Group::getName));
                sortedGroups.addAll(groupsById.values());
                sortedGroups.forEach(g -> logger.trace("=== " + g.toString()));
            }
        }

        synchronized (groupsByName) {
            groupsByName.clear();
            gidToGroup.values().forEach(g -> groupsByName.put(g.getName(), g));
            logger.debug("groupsByName now size: " + groupsByName.size());
        }

        final long endTime = System.currentTimeMillis();
        logger.info("Refreshed users and groups, took {} seconds", (endTime - startTime) / 1000);
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
            logger.trace("Processing user: {}", new Object[]{line});

            String[] record = line.split(":");
            if (record.length > 2) {
                String userIdentity = record[0], userIdentifier = record[1], primaryGroupIdentifier = record[2];

                if (!StringUtils.isBlank(userIdentifier) && !StringUtils.isBlank(userIdentity) && !excludeUsers.matcher(userIdentity).matches()) {
                    User user = new User.Builder()
                            .identity(userIdentity)
                            .identifierGenerateFromSeed(getUserIdentifierSeed(userIdentity))
                            .build();
                    idToUser.put(user.getIdentifier(), user);
                    usernameToUser.put(userIdentity, user);
                    logger.debug("Refreshed user {}", new Object[]{user});

                    if (!StringUtils.isBlank(primaryGroupIdentifier)) {
                        // create a temporary group to deterministically generate the group id and associate this user
                        Group group = new Group.Builder()
                                .name(primaryGroupIdentifier)
                                .identifierGenerateFromSeed(getGroupIdentifierSeed(primaryGroupIdentifier))
                                .build();
                        gidToUser.put(group.getIdentifier(), user);
                        logger.debug("Associated primary group {} with user {}", new Object[]{group.getIdentifier(), userIdentity});
                    } else {
                        logger.warn("Null or empty primary group id for: " + userIdentity);
                    }

                } else {
                    logger.warn("Null, empty, or skipped user name: " + userIdentity + " or id: " + userIdentifier);
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
            logger.trace("Processing group: {}", new Object[]{line});

            String[] record = line.split(":");
            if (record.length > 1) {
                Set<String> users = new HashSet<>();
                String groupName = record[0], groupIdentifier = record[1];

                try {
                    String groupMembersCommand = selectedShellCommands.getGroupMembers(groupName);
                    List<String> memberLines = shellRunner.runShell(groupMembersCommand);
                    // Use the first line only, and log if the line count isn't exactly one:
                    if (!memberLines.isEmpty()) {
                        String memberLine = memberLines.get(0);
                        if (!StringUtils.isBlank(memberLine)) {
                            String[] members = memberLine.split(",");
                            for (String userIdentity : members) {
                                if (!StringUtils.isBlank(userIdentity)) {
                                    User tempUser = new User.Builder()
                                            .identity(userIdentity)
                                            .identifierGenerateFromSeed(getUserIdentifierSeed(userIdentity))
                                            .build();
                                    users.add(tempUser.getIdentifier());
                                    logger.debug("Added temp user {} for group {}", new Object[]{tempUser, groupName});
                                }
                            }
                        } else {
                            logger.debug("list membership returned no members");
                        }
                    } else {
                        logger.debug("list membership returned zero lines.");
                    }
                    if (memberLines.size() > 1) {
                        logger.error("list membership returned too many lines, only used the first.");
                    }

                } catch (final IOException ioexc) {
                    logger.error("list membership shell exception: " + ioexc);
                }

                if (!StringUtils.isBlank(groupIdentifier) && !StringUtils.isBlank(groupName) && !excludeGroups.matcher(groupName).matches()) {
                    Group group = new Group.Builder()
                            .name(groupName)
                            .identifierGenerateFromSeed(getGroupIdentifierSeed(groupIdentifier))
                            .addUsers(users)
                            .build();
                    groupsById.put(group.getIdentifier(), group);
                    logger.debug("Refreshed group {}", new Object[] {group});
                } else {
                    logger.warn("Null, empty, or skipped group name: " + groupName + " or id: " + groupIdentifier);
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
                logger.warn("Primary group {} not found for {}", new Object[]{primaryGid, primaryUser.getIdentity()});
            } else if (!excludeGroups.matcher(primaryGroup.getName()).matches()) {
                Set<String> groupUsers = primaryGroup.getUsers();
                if (!groupUsers.contains(primaryUser.getIdentifier())) {
                    Set<String> updatedUserIdentifiers = new HashSet<>(groupUsers);
                    updatedUserIdentifiers.add(primaryUser.getIdentifier());

                    Group updatedGroup = new Group.Builder()
                            .identifier(primaryGroup.getIdentifier())
                            .name(primaryGroup.getName())
                            .addUsers(updatedUserIdentifiers)
                            .build();
                    gidToGroup.put(updatedGroup.getIdentifier(), updatedGroup);
                    logger.debug("Added user {} to primary group {}", new Object[]{primaryUser, updatedGroup});
                } else {
                    logger.debug("Primary group {} already contains user {}", new Object[]{primaryGroup, primaryUser});
                }
            } else {
                logger.debug("Primary group {} excluded from matcher for {}", new Object[]{primaryGroup.getName(), primaryUser.getIdentity()});
            }
        });
    }

    private String getUserIdentifierSeed(final String userIdentifier) {
        return legacyIdentifierMode ? userIdentifier : userIdentifier + "-user";
    }

    private String getGroupIdentifierSeed(final String groupIdentifier) {
        return legacyIdentifierMode ? groupIdentifier : groupIdentifier + "-group";
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

        synchronized (groupsByName) {
            groupsByName.clear();
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
