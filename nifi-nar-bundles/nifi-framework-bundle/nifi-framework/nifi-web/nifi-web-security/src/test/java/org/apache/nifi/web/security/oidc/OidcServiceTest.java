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
package org.apache.nifi.web.security.oidc;

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.id.State;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OidcServiceTest {

    public static final String TEST_REQUEST_IDENTIFIER = "test-request-identifier";
    public static final String TEST_STATE = "test-state";

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledCreateState() throws Exception {
        final OidcService service = getServiceWithNoOidcSupport();
        service.createState(TEST_REQUEST_IDENTIFIER);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateStateMultipleInvocations() throws Exception {
        final OidcService service = getServiceWithOidcSupport();
        service.createState(TEST_REQUEST_IDENTIFIER);
        service.createState(TEST_REQUEST_IDENTIFIER);
    }

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledValidateState() throws Exception {
        final OidcService service = getServiceWithNoOidcSupport();
        service.isStateValid(TEST_REQUEST_IDENTIFIER, new State(TEST_STATE));
    }

    @Test
    public void testOidcUnknownState() throws Exception {
        final OidcService service = getServiceWithOidcSupport();
        assertFalse(service.isStateValid(TEST_REQUEST_IDENTIFIER, new State(TEST_STATE)));
    }

    @Test
    public void testValidateState() throws Exception {
        final OidcService service = getServiceWithOidcSupport();
        final State state = service.createState(TEST_REQUEST_IDENTIFIER);
        assertTrue(service.isStateValid(TEST_REQUEST_IDENTIFIER, state));
    }

    @Test
    public void testValidateStateExpiration() throws Exception {
        final OidcService service = getServiceWithOidcSupportAndCustomExpiration(1, TimeUnit.SECONDS);
        final State state = service.createState(TEST_REQUEST_IDENTIFIER);

        Thread.sleep(3 * 1000);

        assertFalse(service.isStateValid(TEST_REQUEST_IDENTIFIER, state));
    }

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledExchangeCode() throws Exception {
        final OidcService service = getServiceWithNoOidcSupport();
        service.exchangeAuthorizationCode(TEST_REQUEST_IDENTIFIER, getAuthorizationCodeGrant());
    }

    @Test(expected = IllegalStateException.class)
    public void testExchangeCodeMultipleInvocation() throws Exception {
        final OidcService service = getServiceWithOidcSupport();
        service.exchangeAuthorizationCode(TEST_REQUEST_IDENTIFIER, getAuthorizationCodeGrant());
        service.exchangeAuthorizationCode(TEST_REQUEST_IDENTIFIER, getAuthorizationCodeGrant());
    }

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledGetJwt() throws Exception {
        final OidcService service = getServiceWithNoOidcSupport();
        service.getJwt(TEST_REQUEST_IDENTIFIER);
    }

    @Test
    public void testGetJwt() throws Exception {
        final OidcService service = getServiceWithOidcSupport();
        service.exchangeAuthorizationCode(TEST_REQUEST_IDENTIFIER, getAuthorizationCodeGrant());
        assertNotNull(service.getJwt(TEST_REQUEST_IDENTIFIER));
    }

    @Test
    public void testGetJwtExpiration() throws Exception {
        final OidcService service = getServiceWithOidcSupportAndCustomExpiration(1, TimeUnit.SECONDS);
        service.exchangeAuthorizationCode(TEST_REQUEST_IDENTIFIER, getAuthorizationCodeGrant());

        Thread.sleep(3 * 1000);

        assertNull(service.getJwt(TEST_REQUEST_IDENTIFIER));
    }

    private OidcService getServiceWithNoOidcSupport() {
        final OidcIdentityProvider provider = mock(OidcIdentityProvider.class);
        when(provider.isOidcEnabled()).thenReturn(false);

        final OidcService service = new OidcService(provider);
        assertFalse(service.isOidcEnabled());

        return service;
    }

    private OidcService getServiceWithOidcSupport() throws Exception {
        final OidcIdentityProvider provider = mock(OidcIdentityProvider.class);
        when(provider.isOidcEnabled()).thenReturn(true);
        when(provider.exchangeAuthorizationCode(any())).then(invocation -> UUID.randomUUID().toString());

        final OidcService service = new OidcService(provider);
        assertTrue(service.isOidcEnabled());

        return service;
    }

    private OidcService getServiceWithOidcSupportAndCustomExpiration(final int duration, final TimeUnit units) throws Exception {
        final OidcIdentityProvider provider = mock(OidcIdentityProvider.class);
        when(provider.isOidcEnabled()).thenReturn(true);
        when(provider.exchangeAuthorizationCode(any())).then(invocation -> UUID.randomUUID().toString());

        final OidcService service = new OidcService(provider, duration, units);
        assertTrue(service.isOidcEnabled());

        return service;
    }

    private AuthorizationCodeGrant getAuthorizationCodeGrant() {
        return new AuthorizationCodeGrant(new AuthorizationCode("code"), URI.create("http://localhost:8080/nifi"));
    }
}