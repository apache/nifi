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

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

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

    protected boolean isTestableEnvironment() {
        boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("windows");
        return !isWindows && !systemCheckFailed;
    }

    void testGetUsers(UserGroupProvider provider) {
        assumeTrue(isTestableEnvironment());

        Set<User> users = provider.getUsers();
        assertNotNull(users);
        assertTrue(users.size() > 0);
    }

    void testGetUser(UserGroupProvider provider) {
        assumeTrue(isTestableEnvironment());

        User root = provider.getUser(KNOWN_UID);
        assertNotNull(root);
        assertEquals(KNOWN_USER, root.getIdentity());
        assertEquals(KNOWN_UID, root.getIdentifier());
    }

    void testGetUserByIdentity(UserGroupProvider provider) {
        assumeTrue(isTestableEnvironment());

        User root = provider.getUserByIdentity(KNOWN_USER);
        assertNotNull(root);
        assertEquals(KNOWN_USER, root.getIdentity());
        assertEquals(KNOWN_UID, root.getIdentifier());
    }

    void testGetGroups(UserGroupProvider provider) {
        assumeTrue(isTestableEnvironment());

        Set<Group> groups = provider.getGroups();
        assertNotNull(groups);
        assertTrue(groups.size() > 0);
    }

    void testGetGroup(UserGroupProvider provider) {
        assumeTrue(isTestableEnvironment());

        Group group = provider.getGroup(KNOWN_GID);
        assertNotNull(group);
        assertTrue(group.getName().equals(KNOWN_GROUP) || group.getName().equals(OTHER_GROUP));
        assertEquals(KNOWN_GID, group.getIdentifier());
    }

    void testGroupMembership(UserGroupProvider provider) {
        assumeTrue(isTestableEnvironment());

        Group group = provider.getGroup(KNOWN_GID);
        assertNotNull(group);

        // These next few try/catch blocks are here for debugging.  The user-to-group relationship
        // is delicate with this implementation, and this approach allows us a measure of control.
        // Check your logs if you're having problems!

        try {
            assertTrue(group.getUsers().size() > 0);
        } catch (final AssertionError ignored) {
            logger.warn("root group count zero on this system");
        }

        try {
            assertTrue(group.getUsers().contains(KNOWN_USER));
        } catch (final AssertionError ignored) {
            logger.warn("root group membership unexpected on this system");
        }
    }

    void testGetUserAndGroups(UserGroupProvider provider) {
        assumeTrue(isTestableEnvironment());

        UserAndGroups user = provider.getUserAndGroups(KNOWN_UID);
        assertNotNull(user);

        try {
            assertTrue(user.getGroups().size() > 0);
        } catch (final AssertionError ignored) {
            logger.warn("root user and groups group count zero on this system");
        }

        Set<Group> groups = provider.getGroups();
        assertTrue(groups.size() > user.getGroups().size());
    }
}
