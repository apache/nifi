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
package org.apache.nifi.web.security.saml.impl;

import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.saml.SAMLStateManager;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class TestStandardSAMLStateManager {

    private JwtService jwtService;
    private SAMLStateManager stateManager;

    @Before
    public void setup() {
        jwtService = mock(JwtService.class);
        stateManager = new StandardSAMLStateManager(jwtService);
    }

    @Test
    public void testCreateStateAndCheckIsValid() {
        final String requestId = "request1";

        // create state
        final String state = stateManager.createState(requestId);
        assertNotNull(state);

        // should be valid
        assertTrue(stateManager.isStateValid(requestId, state));

        // should have been invalidated by checking if is valid above
        assertFalse(stateManager.isStateValid(requestId, state));
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateStateWhenExisting() {
        final String requestId = "request1";
        stateManager.createState(requestId);
        stateManager.createState(requestId);
    }

    @Test
    public void testIsValidWhenDoesNotExist() {
        final String requestId = "request1";
        assertFalse(stateManager.isStateValid(requestId, "some-state-value"));
    }

    @Test
    public void testCreateAndGetJwt() {
        final String requestId = "request1";
        final LoginAuthenticationToken token = new LoginAuthenticationToken("user1", "user1", 10000, "nifi");

        // create the jwt and cache it
        final String fakeJwt = "fake-jwt";
        when(jwtService.generateSignedToken(token)).thenReturn(fakeJwt);
        stateManager.createJwt(requestId, token);

        // should return the jwt above
        final String jwt = stateManager.getJwt(requestId);
        assertEquals(fakeJwt, jwt);

        // should no longer exist after retrieving above
        assertNull(stateManager.getJwt(requestId));
    }

}
