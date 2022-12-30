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
package org.apache.nifi.web.api;

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ErrorObject;
import com.nimbusds.openid.connect.sdk.AuthenticationErrorResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import org.apache.nifi.admin.service.IdpUserGroupService;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.jwt.provider.StandardBearerTokenProvider;
import org.apache.nifi.web.security.oidc.OidcService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.idp.IdpType.OIDC;
import static org.apache.nifi.web.security.cookie.ApplicationCookieName.OIDC_REQUEST_IDENTIFIER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public class OIDCAccessResourceTest {

    final static String REQUEST_IDENTIFIER = "an-identifier";

    final static String OIDC_LOGIN_FAILURE_MESSAGE = "Unsuccessful login attempt.";

    @Test
    public void testOidcCallbackSuccess() throws Exception {
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        Cookie[] cookies = { new Cookie(OIDC_REQUEST_IDENTIFIER.getCookieName(), REQUEST_IDENTIFIER) };
        Mockito.when(mockRequest.getCookies()).thenReturn(cookies);
        OidcService oidcService = Mockito.mock(OidcService.class);
        MockOIDCAccessResource accessResource = new MockOIDCAccessResource(oidcService, true);
        IdpUserGroupService idpUserGroupService = Mockito.mock(IdpUserGroupService.class);
        accessResource.setIdpUserGroupService(idpUserGroupService);
        accessResource.oidcCallback(mockRequest, mockResponse);
        Mockito.verify(oidcService).storeJwt(any(String.class), any(String.class));
        Mockito.verify(idpUserGroupService).replaceUserGroups(MockOIDCAccessResource.IDENTITY, OIDC, Stream.of(MockOIDCAccessResource.ROLE).collect(Collectors.toSet()));
    }

    @Test
    public void testOidcCallbackFailure() throws Exception {
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        Cookie[] cookies = { new Cookie(OIDC_REQUEST_IDENTIFIER.getCookieName(), REQUEST_IDENTIFIER) };
        Mockito.when(mockRequest.getCookies()).thenReturn(cookies);
        OidcService oidcService = Mockito.mock(OidcService.class);
        MockOIDCAccessResource accessResource = new MockOIDCCallbackFailure(oidcService, false);
        accessResource.oidcCallback(mockRequest, mockResponse);
    }

    public class MockOIDCCallbackFailure extends MockOIDCAccessResource {

        public MockOIDCCallbackFailure(OidcService oidcService, Boolean requestShouldSucceed) throws IOException {
            super(oidcService, requestShouldSucceed);
        }

        @Override
        protected void forwardToLoginMessagePage(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse, final String message) throws Exception {
            assertEquals(OIDC_LOGIN_FAILURE_MESSAGE, message);
        }
    }

    public class MockOIDCAccessResource extends OIDCAccessResource {

        final static String BEARER_TOKEN = "bearer_token";
        final static String IDENTITY = "identity";
        final static String ROLE = "role";
        final static String AUTHORIZATION_CODE = "authorization_code";
        final static String CALLBACK_URL = "https://nifi.apache.org/nifi-api/access/oidc/callback";
        final static String RESOURCE_URI = "resource_uri";
        private Boolean requestShouldSucceed;

        public MockOIDCAccessResource(final OidcService oidcService, final Boolean requestShouldSucceed) throws IOException {
            this.requestShouldSucceed = requestShouldSucceed;
            final BearerTokenProvider bearerTokenProvider = Mockito.mock(StandardBearerTokenProvider.class);
            Mockito.when(bearerTokenProvider.getBearerToken(any(LoginAuthenticationToken.class))).thenReturn(BEARER_TOKEN);
            setOidcService(oidcService);
            setBearerTokenProvider(bearerTokenProvider);
            final LoginAuthenticationToken token = Mockito.mock(LoginAuthenticationToken.class);
            Mockito.when(token.getName()).thenReturn(IDENTITY);
            Mockito.when(token.getAuthorities()).thenReturn(Stream.of(new SimpleGrantedAuthority(ROLE)).collect(Collectors.toSet()));
            Mockito.when(oidcService.exchangeAuthorizationCodeForLoginAuthenticationToken(any(AuthorizationGrant.class))).thenReturn(token);
        }

        @Override
        protected AuthenticationResponse parseOidcResponse(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, boolean isLogin) {
            if (requestShouldSucceed) {
                return getSuccessResponse();
            } else {
                return getErrorResponse();
            }
        }

        @Override
        protected void checkOidcState(HttpServletResponse httpServletResponse, final String oidcRequestIdentifier, AuthenticationSuccessResponse successfulOidcResponse, boolean isLogin)
                throws Exception {
            // do nothing
        }

        @Override
        protected String getOidcCallback() {
            return CALLBACK_URL;
        }

        @Override
        protected String generateResourceUri(final String... path) {
            return RESOURCE_URI;
        }

        @Override
        protected URI getCookieResourceUri() {
            return URI.create(RESOURCE_URI);
        }

        private AuthenticationResponse getSuccessResponse() {
            AuthenticationSuccessResponse successResponse = Mockito.mock(AuthenticationSuccessResponse.class);
            Mockito.when(successResponse.indicatesSuccess()).thenReturn(true);
            Mockito.when(successResponse.getAuthorizationCode()).thenReturn(new AuthorizationCode(AUTHORIZATION_CODE));
            return successResponse;
        }

        private AuthenticationResponse getErrorResponse() {
            AuthenticationErrorResponse errorResponse = Mockito.mock(AuthenticationErrorResponse.class);
            Mockito.when(errorResponse.indicatesSuccess()).thenReturn(false);
            Mockito.when(errorResponse.getErrorObject()).thenReturn(new ErrorObject("HTTP 500", "OIDC server error"));
            return errorResponse;
        }

    }
}