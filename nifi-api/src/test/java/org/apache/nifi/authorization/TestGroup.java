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

public class TestGroup {

    @Test
    public void testSimpleCreation() {
        final String id = "1";
        final String name = "group1";
        final String user1 = "user1";
        final String user2 = "user2";

        final Group group = new Group.GroupBuilder()
                .identifier(id)
                .name(name)
                .addUser(user1)
                .addUser(user2)
                .build();

        assertEquals(id, group.getIdentifier());
        assertEquals(name, group.getName());

        assertNotNull(group.getUsers());
        assertEquals(2, group.getUsers().size());
        assertTrue(group.getUsers().contains(user1));
        assertTrue(group.getUsers().contains(user2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingId() {
        new Group.GroupBuilder()
                .name("group1")
                .addUser("user1")
                .addUser("user2")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingName() {
        new Group.GroupBuilder()
                .identifier("1")
                .addUser("user1")
                .addUser("user2")
                .build();
    }

    @Test
    public void testMissingUsers() {
        final String id = "1";
        final String name = "group1";

        final Group group = new Group.GroupBuilder()
                .identifier(id)
                .name(name)
                .build();

        assertEquals(id, group.getIdentifier());
        assertEquals(name, group.getName());

        assertNotNull(group.getUsers());
        assertEquals(0, group.getUsers().size());
    }

    @Test
    public void testFromGroup() {
        final String id = "1";
        final String name = "group1";
        final String user1 = "user1";
        final String user2 = "user2";

        final Group group1 = new Group.GroupBuilder()
                .identifier(id)
                .name(name)
                .addUser(user1)
                .addUser(user2)
                .build();

        assertEquals(id, group1.getIdentifier());
        assertEquals(name, group1.getName());

        assertNotNull(group1.getUsers());
        assertEquals(2, group1.getUsers().size());
        assertTrue(group1.getUsers().contains(user1));
        assertTrue(group1.getUsers().contains(user2));

        final Group group2 = new Group.GroupBuilder(group1).build();
        assertEquals(group1.getIdentifier(), group2.getIdentifier());
        assertEquals(group1.getName(), group2.getName());
        assertEquals(group1.getUsers(), group2.getUsers());
    }

    @Test(expected = IllegalStateException.class)
    public void testFromGroupAndChangeIdentifier() {
        final Group group1 = new Group.GroupBuilder()
                .identifier("1")
                .name("group1")
                .addUser("user1")
                .build();

        new Group.GroupBuilder(group1).identifier("2").build();
    }

    @Test
    public void testAddRemoveClearUsers() {
        final Group.GroupBuilder builder = new Group.GroupBuilder()
                .identifier("1")
                .name("group1")
                .addUser("user1");

        final Group group1 = builder.build();
        assertNotNull(group1.getUsers());
        assertEquals(1, group1.getUsers().size());
        assertTrue(group1.getUsers().contains("user1"));

        final Set<String> moreUsers = new HashSet<>();
        moreUsers.add("user2");
        moreUsers.add("user3");
        moreUsers.add("user4");

        final Group group2 = builder.addUsers(moreUsers).build();
        assertEquals(4, group2.getUsers().size());
        assertTrue(group2.getUsers().contains("user1"));
        assertTrue(group2.getUsers().contains("user2"));
        assertTrue(group2.getUsers().contains("user3"));
        assertTrue(group2.getUsers().contains("user4"));

        final Group group3 = builder.removeUser("user2").build();
        assertEquals(3, group3.getUsers().size());
        assertTrue(group3.getUsers().contains("user1"));
        assertTrue(group3.getUsers().contains("user3"));
        assertTrue(group3.getUsers().contains("user4"));

        final Set<String> removeUsers = new HashSet<>();
        removeUsers.add("user1");
        removeUsers.add("user4");

        final Group group4 = builder.removeUsers(removeUsers).build();
        assertEquals(1, group4.getUsers().size());
        assertTrue(group4.getUsers().contains("user3"));

        final Group group5 = builder.clearUsers().build();
        assertEquals(0, group5.getUsers().size());
    }

}
