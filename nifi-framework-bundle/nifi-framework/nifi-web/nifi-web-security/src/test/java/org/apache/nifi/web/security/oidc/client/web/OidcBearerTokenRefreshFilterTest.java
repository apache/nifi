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
package org.apache.nifi.web.security.oidc.client.web;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.security.oauth2.client.endpoint.OAuth2AccessTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.OAuth2RefreshTokenGrantRequest;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizedClientRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.core.endpoint.OAuth2AccessTokenResponse;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OidcBearerTokenRefreshFilterTest {

    private static final String CLIENT_ID = "client-id";

    private static final String REDIRECT_URI = "http://localhost:8080";

    private static final String AUTHORIZATION_URI = "http://localhost/authorize";

    private static final String TOKEN_URI = "http://localhost/token";

    private static final String SUB_CLAIM = "sub";

    private static final String IDENTITY = "user-identity";

    private static final Duration REFRESH_WINDOW = Duration.ZERO;

    private static final String CURRENT_USER_URI = "/flow/current-user";

    private static final String BEARER_TOKEN = "bearer-token";

    private static final String BEARER_TOKEN_REFRESHED = "bearer-token-refreshed";

    private static final String ACCESS_TOKEN = "access-token";

    private static final String REFRESH_TOKEN = "refresh-token";

    private static final String ID_TOKEN = "id-token";

    private static final String EXP_CLAIM = "exp";

    private static final int INSTANT_OFFSET = 1;

    private static final String PROVIDER_GROUP = "Authorized";

    private static final Set<String> PROVIDER_GROUPS = Collections.singleton(PROVIDER_GROUP);

    @Mock
    BearerTokenProvider bearerTokenProvider;

    @Mock
    BearerTokenResolver bearerTokenResolver;

    @Mock
    JwtDecoder jwtDecoder;

    @Mock
    OAuth2AuthorizedClientRepository authorizedClientRepository;

    @Mock
    OAuth2AccessTokenResponseClient<OAuth2RefreshTokenGrantRequest> refreshTokenResponseClient;

    @Mock
    OidcAuthorizedClient authorizedClient;

    @Captor
    ArgumentCaptor<LoginAuthenticationToken> tokenArgumentCaptor;

    MockHttpServletRequest request;

    MockHttpServletResponse response;

    MockFilterChain filterChain;

    OidcBearerTokenRefreshFilter filter;

    @BeforeEach
    void setFilter() {
        filter = new OidcBearerTokenRefreshFilter(
                REFRESH_WINDOW,
                bearerTokenProvider,
                bearerTokenResolver,
                jwtDecoder,
                authorizedClientRepository,
                refreshTokenResponseClient
        );
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        filterChain = new MockFilterChain();
    }

    @AfterEach
    void clearContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void testDoFilterPathNotMatched() throws ServletException, IOException {
        filter.doFilter(request, response, filterChain);

        verifyNoInteractions(bearerTokenResolver);
    }

    @Test
    void testDoFilterBearerTokenNotFound() throws ServletException, IOException {
        request.setServletPath(CURRENT_USER_URI);

        filter.doFilter(request, response, filterChain);

        verify(bearerTokenResolver).resolve(eq(request));
    }

    @Test
    void testDoFilterBearerTokenFoundRefreshNotRequired() throws ServletException, IOException {
        request.setServletPath(CURRENT_USER_URI);

        when(bearerTokenResolver.resolve(eq(request))).thenReturn(BEARER_TOKEN);
        final Jwt jwt = getJwt(Instant.MAX);
        when(jwtDecoder.decode(eq(BEARER_TOKEN))).thenReturn(jwt);

        filter.doFilter(request, response, filterChain);

        verifyNoInteractions(authorizedClientRepository);
    }

    @Test
    void testDoFilterRefreshRequiredClientNotFound() throws ServletException, IOException {
        request.setServletPath(CURRENT_USER_URI);

        when(bearerTokenResolver.resolve(eq(request))).thenReturn(BEARER_TOKEN);
        final Jwt jwt = getJwt(Instant.now().minusSeconds(INSTANT_OFFSET));
        when(jwtDecoder.decode(eq(BEARER_TOKEN))).thenReturn(jwt);

        when(authorizedClientRepository.loadAuthorizedClient(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()), any(), eq(request))).thenReturn(null);

        filter.doFilter(request, response, filterChain);

        final Cookie cookie = response.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());
        assertNull(cookie);
    }

    @Test
    void testDoFilterRefreshRequiredRefreshTokenNotFound() throws ServletException, IOException {
        request.setServletPath(CURRENT_USER_URI);

        when(bearerTokenResolver.resolve(eq(request))).thenReturn(BEARER_TOKEN);
        final Jwt jwt = getJwt(Instant.now().minusSeconds(INSTANT_OFFSET));
        when(jwtDecoder.decode(eq(BEARER_TOKEN))).thenReturn(jwt);

        when(authorizedClientRepository.loadAuthorizedClient(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()), any(), eq(request))).thenReturn(authorizedClient);

        filter.doFilter(request, response, filterChain);

        final Cookie cookie = response.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());
        assertNull(cookie);
    }

    @Test
    void testDoFilterBearerTokenTokenRefreshed() throws ServletException, IOException {
        final NiFiUser user = new StandardNiFiUser.Builder().identity(IDENTITY).identityProviderGroups(PROVIDER_GROUPS).build();
        final NiFiUserDetails userDetails = new NiFiUserDetails(user);
        final NiFiAuthenticationToken authenticationToken = new NiFiAuthenticationToken(userDetails);
        final SecurityContext securityContext = new SecurityContextImpl(authenticationToken);
        SecurityContextHolder.setContext(securityContext);

        request.setServletPath(CURRENT_USER_URI);

        when(bearerTokenResolver.resolve(eq(request))).thenReturn(BEARER_TOKEN);
        final Instant expiration = Instant.now().minusSeconds(INSTANT_OFFSET);
        final Jwt jwt = getJwt(expiration);
        when(jwtDecoder.decode(eq(BEARER_TOKEN))).thenReturn(jwt);

        final Instant issued = expiration.minus(Duration.ofHours(INSTANT_OFFSET));
        final OAuth2AccessToken accessToken = new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER, ACCESS_TOKEN, issued, expiration);
        final OAuth2RefreshToken refreshToken = new OAuth2RefreshToken(REFRESH_TOKEN, issued);

        final Map<String, Object> claims = Collections.singletonMap(SUB_CLAIM, IDENTITY);
        final OidcIdToken idToken = new OidcIdToken(ID_TOKEN, issued, expiration, claims);

        when(authorizedClient.getAccessToken()).thenReturn(accessToken);
        when(authorizedClient.getRefreshToken()).thenReturn(refreshToken);
        when(authorizedClient.getIdToken()).thenReturn(idToken);
        when(authorizedClient.getClientRegistration()).thenReturn(getClientRegistration());
        when(authorizedClient.getPrincipalName()).thenReturn(IDENTITY);
        when(authorizedClientRepository.loadAuthorizedClient(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()), any(), eq(request))).thenReturn(authorizedClient);

        final OAuth2AccessTokenResponse tokenResponse = OAuth2AccessTokenResponse.withToken(ACCESS_TOKEN)
                .tokenType(OAuth2AccessToken.TokenType.BEARER)
                .expiresIn(INSTANT_OFFSET)
                .build();
        when(refreshTokenResponseClient.getTokenResponse(any())).thenReturn(tokenResponse);

        when(bearerTokenProvider.getBearerToken(tokenArgumentCaptor.capture())).thenReturn(BEARER_TOKEN_REFRESHED);

        filter.doFilter(request, response, filterChain);

        final Cookie cookie = response.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());
        assertNotNull(cookie);
        assertEquals(BEARER_TOKEN_REFRESHED, cookie.getValue());

        final LoginAuthenticationToken loginAuthenticationToken = tokenArgumentCaptor.getValue();
        final Iterator<GrantedAuthority> authorities = loginAuthenticationToken.getAuthorities().iterator();
        assertTrue(authorities.hasNext());
        final GrantedAuthority authority = authorities.next();
        assertEquals(PROVIDER_GROUP, authority.getAuthority());
    }

    private Jwt getJwt(final Instant expiration) {
        final Map<String, Object> claims = Collections.singletonMap(EXP_CLAIM, expiration);
        final Instant issued = expiration.minus(Duration.ofHours(INSTANT_OFFSET));
        return new Jwt(BEARER_TOKEN, issued, expiration, claims, claims);
    }

    ClientRegistration getClientRegistration() {
        return ClientRegistration.withRegistrationId(OidcRegistrationProperty.REGISTRATION_ID.getProperty())
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .clientId(CLIENT_ID)
                .redirectUri(REDIRECT_URI)
                .authorizationUri(AUTHORIZATION_URI)
                .tokenUri(TOKEN_URI)
                .userNameAttributeName(SUB_CLAIM)
                .build();
    }
}
