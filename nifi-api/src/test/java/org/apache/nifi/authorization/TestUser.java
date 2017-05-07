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

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestUser {

    @Test
    public void testSimpleCreation() {
        final String identifier = "1";
        final String identity = "user1";
        final String group1 = "group1";
        final String group2 = "group2";

        final User user = new User.UserBuilder()
                .identifier(identifier)
                .identity(identity)
                .addGroup(group1)
                .addGroup(group2)
                .build();

        assertEquals(identifier, user.getIdentifier());
        assertEquals(identity, user.getIdentity());

        assertNotNull(user.getGroups());
        assertEquals(2, user.getGroups().size());
        assertTrue(user.getGroups().contains(group1));
        assertTrue(user.getGroups().contains(group2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingIdentifier() {
        new User.UserBuilder()
                .identity("user1")
                .addGroup("group1")
                .addGroup("group2")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingIdentity() {
        new User.UserBuilder()
                .identifier("1")
                .addGroup("group1")
                .addGroup("group2")
                .build();
    }

    @Test
    public void testMissingGroups() {
        final String identifier = "1";
        final String identity = "user1";

        final User user = new User.UserBuilder()
                .identifier(identifier)
                .identity(identity)
                .build();

        assertEquals(identifier, user.getIdentifier());
        assertEquals(identity, user.getIdentity());

        assertNotNull(user.getGroups());
        assertEquals(0, user.getGroups().size());
    }

    @Test
    public void testFromUser() {
        final String identifier = "1";
        final String identity = "user1";
        final String group1 = "group1";
        final String group2 = "group2";

        final User user = new User.UserBuilder()
                .identifier(identifier)
                .identity(identity)
                .addGroup(group1)
                .addGroup(group2)
                .build();

        assertEquals(identifier, user.getIdentifier());
        assertEquals(identity, user.getIdentity());

        assertNotNull(user.getGroups());
        assertEquals(2, user.getGroups().size());
        assertTrue(user.getGroups().contains(group1));
        assertTrue(user.getGroups().contains(group2));

        final User user2 = new User.UserBuilder(user).build();
        assertEquals(user.getIdentifier(), user2.getIdentifier());
        assertEquals(user.getIdentity(), user2.getIdentity());
        assertEquals(user.getGroups(), user2.getGroups());
    }

    @Test(expected = IllegalStateException.class)
    public void testFromUserAndChangeIdentifier() {
        final User user = new User.UserBuilder()
                .identifier("1")
                .identity("user1")
                .addGroup("group1")
                .addGroup("group2")
                .build();

        new User.UserBuilder(user).identifier("2").build();
    }

    @Test
    public void testAddRemoveClearGroups() {
        final User.UserBuilder builder = new User.UserBuilder()
                .identifier("1")
                .identity("user1")
                .addGroup("group1");

        final User user1 = builder.build();
        assertNotNull(user1.getGroups());
        assertEquals(1, user1.getGroups().size());
        assertTrue(user1.getGroups().contains("group1"));

        final Set<String> moreGroups = new HashSet<>();
        moreGroups.add("group2");
        moreGroups.add("group3");
        moreGroups.add("group4");

        final User user2 = builder.addGroups(moreGroups).build();
        assertEquals(4, user2.getGroups().size());
        assertTrue(user2.getGroups().contains("group1"));
        assertTrue(user2.getGroups().contains("group2"));
        assertTrue(user2.getGroups().contains("group3"));
        assertTrue(user2.getGroups().contains("group4"));

        final User user3 = builder.removeGroup("group2").build();
        assertEquals(3, user3.getGroups().size());
        assertTrue(user3.getGroups().contains("group1"));
        assertTrue(user3.getGroups().contains("group3"));
        assertTrue(user3.getGroups().contains("group4"));

        final Set<String> removeGroups = new HashSet<>();
        removeGroups.add("group1");
        removeGroups.add("group4");

        final User user4 = builder.removeGroups(removeGroups).build();
        assertEquals(1, user4.getGroups().size());
        assertTrue(user4.getGroups().contains("group3"));

        final User user5 = builder.clearGroups().build();
        assertEquals(0, user5.getGroups().size());
    }

}
