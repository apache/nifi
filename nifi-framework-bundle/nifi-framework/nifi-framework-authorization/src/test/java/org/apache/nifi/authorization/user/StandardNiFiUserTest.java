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
package org.apache.nifi.authorization.user;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardNiFiUserTest {

    private static final String IDENTITY = "username";

    private static final String MEMBERS_GROUP = "members";

    private static final String ADMINISTRATORS_GROUP = "administrators";

    private static final Set<String> GROUPS = Set.of(MEMBERS_GROUP, ADMINISTRATORS_GROUP);

    private static final String PROVIDED_GROUP = "provided";

    private static final String CLIENT_ADDRESS = "127.0.0.1";

    @Test
    void testBuilderStandardProperties() {
        final NiFiUser user = new StandardNiFiUser.Builder()
                .identity(IDENTITY)
                .groups(GROUPS)
                .clientAddress(CLIENT_ADDRESS)
                .build();

        assertEquals(IDENTITY, user.getIdentity());
        assertEquals(GROUPS, user.getGroups());
        assertEquals(GROUPS, user.getAllGroups());
        assertEquals(CLIENT_ADDRESS, user.getClientAddress());
        assertFalse(user.isAnonymous());
    }

    @Test
    void testAnonymousUser() {
        final NiFiUser user = StandardNiFiUser.ANONYMOUS;

        assertEquals(StandardNiFiUser.ANONYMOUS_IDENTITY, user.getIdentity());
        assertTrue(user.isAnonymous());
        assertNull(user.getGroups());
        assertTrue(user.getAllGroups().isEmpty());
    }

    @Test
    void testToStringContainsIdentityAndGroups() {
        final NiFiUser user = new StandardNiFiUser.Builder()
                .identity(IDENTITY)
                .groups(GROUPS)
                .identityProviderGroups(Set.of(PROVIDED_GROUP))
                .build();

        final String string = user.toString();
        assertTrue(string.contains(IDENTITY), "Identity not found");
        assertTrue(string.contains(MEMBERS_GROUP), "Members Group not found");
        assertTrue(string.contains(ADMINISTRATORS_GROUP), "Administrators Group not found");
        assertTrue(string.contains(PROVIDED_GROUP), "Identity Provider Group not found");
    }
}
