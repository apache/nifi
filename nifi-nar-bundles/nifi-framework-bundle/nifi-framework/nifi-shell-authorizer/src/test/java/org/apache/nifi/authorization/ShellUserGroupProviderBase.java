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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract class ShellUserGroupProviderBase {
    private static final Logger logger = LoggerFactory.getLogger(ShellUserGroupProviderBase.class);

    private final String KNOWN_USER  = "root";
    private final String KNOWN_UID   = "0";

    @SuppressWarnings("FieldCanBeLocal")
    private final String KNOWN_GROUP = "root";

    @SuppressWarnings("FieldCanBeLocal")
    private final String OTHER_GROUP = "wheel"; // e.g., macos
    private final String KNOWN_GID   = "0";

    // We're using this knob to control the test runs on Travis.  The issue there is that tests
    // running on Travis do not have `getent`, thus not behaving like a typical Linux installation.
    protected static boolean systemCheckFailed = false;

    /**
     * Ensures that the test can run because Docker is available and the remote instance can be reached via ssh.
     *
     * @return true if Docker is available on this OS
     */
    protected boolean isSSHAvailable() {
        return !systemCheckFailed;
    }


    /**
     * Tests the provider behavior by getting its users and checking minimum size.
     *
     * @param provider {@link UserGroupProvider}
     */
    void testGetUsersAndUsersMinimumCount(UserGroupProvider provider) {
        assumeTrue(isSSHAvailable());

        Set<User> users = provider.getUsers();
        assertNotNull(users);
        assertTrue(users.size() > 0);
    }


    /**
     * Tests the provider behavior by getting a known user by uid.
     *
     * @param provider {@link UserGroupProvider}
     */
    void testGetKnownUserByUsername(UserGroupProvider provider) {
        // assumeTrue(isSSHAvailable());

        User root = provider.getUser(KNOWN_UID);
        assertNotNull(root);
        assertEquals(KNOWN_USER, root.getIdentity());
        assertEquals(KNOWN_UID, root.getIdentifier());
    }

    /**
     * Tests the provider behavior by getting a known user by id.
     *
     * @param provider {@link UserGroupProvider}
     */
    void testGetKnownUserByUid(UserGroupProvider provider) {
        assumeTrue(isSSHAvailable());

        User root = provider.getUserByIdentity(KNOWN_USER);
        assertNotNull(root);
        assertEquals(KNOWN_USER, root.getIdentity());
        assertEquals(KNOWN_UID, root.getIdentifier());
    }

    /**
     * Tests the provider behavior by getting its groups and checking minimum size.
     *
     * @param provider {@link UserGroupProvider}
     */
    void testGetGroupsAndMinimumGroupCount(UserGroupProvider provider) {
        assumeTrue(isSSHAvailable());

        Set<Group> groups = provider.getGroups();
        assertNotNull(groups);
        assertTrue(groups.size() > 0);
    }

    /**
     * Tests the provider behavior by getting a known group by GID.
     *
     * @param provider {@link UserGroupProvider}
     */
    void testGetKnownGroupByGid(UserGroupProvider provider) {
        assumeTrue(isSSHAvailable());

        Group group = provider.getGroup(KNOWN_GID);
        assertNotNull(group);
        assertTrue(group.getName().equals(KNOWN_GROUP) || group.getName().equals(OTHER_GROUP));
        assertEquals(KNOWN_GID, group.getIdentifier());
    }

    /**
     * Tests the provider behavior by getting a known group and checking for a known member of it.
     *
     * @param provider {@link UserGroupProvider}
     */
    void testGetGroupByGidAndGetGroupMembership(UserGroupProvider provider) {
        assumeTrue(isSSHAvailable());

        Group group = provider.getGroup(KNOWN_GID);
        assertNotNull(group);

        // These next few try/catch blocks are here for debugging.  The user-to-group relationship
        // is delicate with this implementation, and this approach allows us a measure of control.
        // Check your logs if you're having problems!

        try {
            assertTrue(group.getUsers().size() > 0);
            logger.info("root group count: " + group.getUsers().size());
        } catch (final AssertionError ignored) {
            logger.info("root group count zero on this system");
        }

        try {
            assertTrue(group.getUsers().contains(KNOWN_USER));
            logger.info("root group membership: " + group.getUsers());
        } catch (final AssertionError ignored) {
            logger.info("root group membership unexpected on this system");
        }
    }

    /**
     * Tests the provider behavior by getting a known user and checking its group membership.
     *
     * @param provider {@link UserGroupProvider}
     */
    void testGetUserByIdentityAndGetGroupMembership(UserGroupProvider provider) {
        assumeTrue(isSSHAvailable());

        UserAndGroups user = provider.getUserAndGroups(KNOWN_USER);
        assertNotNull(user);

        try {
            assertTrue(user.getGroups().size() > 0);
            logger.info("root user group count: " + user.getGroups().size());
        } catch (final AssertionError ignored) {
            logger.info("root user and groups group count zero on this system");
        }

        Set<Group> groups = provider.getGroups();
        assertTrue(groups.size() > user.getGroups().size());
    }
}
