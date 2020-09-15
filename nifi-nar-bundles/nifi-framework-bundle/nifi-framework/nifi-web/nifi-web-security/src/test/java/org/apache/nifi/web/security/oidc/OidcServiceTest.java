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
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.id.State;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OidcServiceTest {

    public static final String TEST_REQUEST_IDENTIFIER = "test-request-identifier";
    public static final String TEST_STATE = "test-state";

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledCreateState() {
        final OidcService service = getServiceWithNoOidcSupport();
        service.createState(TEST_REQUEST_IDENTIFIER);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateStateMultipleInvocations() {
        final OidcService service = getServiceWithOidcSupport();
        service.createState(TEST_REQUEST_IDENTIFIER);
        service.createState(TEST_REQUEST_IDENTIFIER);
    }

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledValidateState() {
        final OidcService service = getServiceWithNoOidcSupport();
        service.isStateValid(TEST_REQUEST_IDENTIFIER, new State(TEST_STATE));
    }

    @Test
    public void testOidcUnknownState() {
        final OidcService service = getServiceWithOidcSupport();
        assertFalse(service.isStateValid(TEST_REQUEST_IDENTIFIER, new State(TEST_STATE)));
    }

    @Test
    public void testValidateState() {
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
    public void testStoreJwtMultipleInvocation() {
        final OidcService service = getServiceWithOidcSupport();

        final String TEST_JWT1 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6Ik5pRmkgT0" +
                "lEQyBVbml0IFRlc3RlciIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNTE2MzM5MDIyLCJpc3MiOiJuaWZpX3VuaXRfdGVzdF9hd" +
                "XRoyZyJ9.b4NIl0RONKdVLOH0D1eObdwAEX8qX-ExqB8KuKSZFLw";

        final String TEST_JWT2 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI5ODc2NTQzMjEwIiwibmFtZSI6Ik5pRm" +
                "kgT0lEQyBVbml0IFRlc3RlciIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNTE2MzM5MDIyLCJpc3MiOiJuaWZpX3VuaXRfdGVzdF" +
                "9hdXRob3JpdHkiLCJhdWQiOiJhbGwiLCJ1c2VybmFtZSI6Im9pZGNfdGVzdCIsImVtYWlsIjoib2lkY190ZXN0QG5pZmk" +
                "uYXBhY2hlLm9yZyJ9.nlYhplDLXeGAwW62rJ_ZnEaG7nxEB4TbaJNK-_pC4WQ";

        service.storeJwt(TEST_REQUEST_IDENTIFIER, TEST_JWT1);
        service.storeJwt(TEST_REQUEST_IDENTIFIER, TEST_JWT2);
    }

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledExchangeCodeForLoginAuthenticationToken() throws Exception {
        final OidcService service = getServiceWithNoOidcSupport();
        service.exchangeAuthorizationCodeForLoginAuthenticationToken(getAuthorizationGrant());
    }

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledExchangeCodeForAccessToken() throws Exception {
        final OidcService service = getServiceWithNoOidcSupport();
        service.exchangeAuthorizationCodeForAccessToken(getAuthorizationGrant());
    }

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledExchangeCodeForIdToken() throws IOException {
        final OidcService service = getServiceWithNoOidcSupport();
        service.exchangeAuthorizationCodeForIdToken(getAuthorizationGrant());
    }

    @Test(expected = IllegalStateException.class)
    public void testOidcNotEnabledGetJwt() {
        final OidcService service = getServiceWithNoOidcSupport();
        service.getJwt(TEST_REQUEST_IDENTIFIER);
    }

    private OidcService getServiceWithNoOidcSupport() {
        final OidcIdentityProvider provider = mock(OidcIdentityProvider.class);
        when(provider.isOidcEnabled()).thenReturn(false);

        final OidcService service = new OidcService(provider);
        assertFalse(service.isOidcEnabled());

        return service;
    }

    private OidcService getServiceWithOidcSupport() {
        final OidcIdentityProvider provider = mock(OidcIdentityProvider.class);
        when(provider.isOidcEnabled()).thenReturn(true);

        final OidcService service = new OidcService(provider);
        assertTrue(service.isOidcEnabled());

        return service;
    }

    private OidcService getServiceWithOidcSupportAndCustomExpiration(final int duration, final TimeUnit units) throws Exception {
        final OidcIdentityProvider provider = mock(OidcIdentityProvider.class);
        when(provider.isOidcEnabled()).thenReturn(true);
        when(provider.exchangeAuthorizationCodeforLoginAuthenticationToken(any())).then(invocation -> UUID.randomUUID().toString());

        final OidcService service = new OidcService(provider, duration, units);
        assertTrue(service.isOidcEnabled());

        return service;
    }

    private AuthorizationGrant getAuthorizationGrant() {
        return new AuthorizationCodeGrant(new AuthorizationCode("code"), URI.create("http://localhost:8080/nifi"));
    }
}