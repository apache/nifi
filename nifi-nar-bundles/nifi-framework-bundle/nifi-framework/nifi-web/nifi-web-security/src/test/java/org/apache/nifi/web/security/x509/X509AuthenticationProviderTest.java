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
package org.apache.nifi.web.security.x509;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Before;
import org.junit.Test;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class X509AuthenticationProviderTest {

    private static final String INVALID_CERTIFICATE = "invalid-certificate";
    private static final String IDENTITY_1 = "identity-1";
    private static final String ANONYMOUS = "";

    private static final String UNTRUSTED_PROXY = "untrusted-proxy";
    private static final String PROXY_1 = "proxy-1";
    private static final String PROXY_2 = "proxy-2";

    private static final String GT = ">";
    private static final String ESCAPED_GT = "\\\\>";
    private static final String LT = "<";
    private static final String ESCAPED_LT = "\\\\<";

    private X509AuthenticationProvider x509AuthenticationProvider;
    private X509IdentityProvider certificateIdentityProvider;
    private SubjectDnX509PrincipalExtractor extractor;
    private Authorizer authorizer;

    @Before
    public void setup() {
        extractor = new SubjectDnX509PrincipalExtractor();

        certificateIdentityProvider = mock(X509IdentityProvider.class);
        when(certificateIdentityProvider.authenticate(any(X509Certificate[].class))).then(invocation -> {
            final X509Certificate[] certChain = invocation.getArgumentAt(0, X509Certificate[].class);
            final String identity = extractor.extractPrincipal(certChain[0]).toString();

            if (INVALID_CERTIFICATE.equals(identity)) {
                throw new IllegalArgumentException();
            }

            return new AuthenticationResponse(identity, identity, TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS), "");
        });

        authorizer = mock(Authorizer.class);
        when(authorizer.authorize(any(AuthorizationRequest.class))).then(invocation -> {
            final AuthorizationRequest request = invocation.getArgumentAt(0, AuthorizationRequest.class);

            if (UNTRUSTED_PROXY.equals(request.getIdentity())) {
                return AuthorizationResult.denied();
            }

            return AuthorizationResult.approved();
        });

        x509AuthenticationProvider = new X509AuthenticationProvider(certificateIdentityProvider, authorizer, NiFiProperties.createBasicNiFiProperties(null, null));
    }

    @Test(expected = InvalidAuthenticationException.class)
    public void testInvalidCertificate() {
        x509AuthenticationProvider.authenticate(getX509Request("", INVALID_CERTIFICATE));
    }

    @Test
    public void testNoProxyChain() {
        final NiFiAuthenticationToken auth = (NiFiAuthenticationToken) x509AuthenticationProvider.authenticate(getX509Request("", IDENTITY_1));
        final NiFiUser user = ((NiFiUserDetails) auth.getDetails()).getNiFiUser();

        assertNotNull(user);
        assertEquals(IDENTITY_1, user.getIdentity());
        assertFalse(user.isAnonymous());
    }

    @Test(expected = UntrustedProxyException.class)
    public void testUntrustedProxy() {
        x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(IDENTITY_1), UNTRUSTED_PROXY));
    }

    @Test
    public void testOneProxy() {
        final NiFiAuthenticationToken auth = (NiFiAuthenticationToken) x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(IDENTITY_1), PROXY_1));
        final NiFiUser user = ((NiFiUserDetails) auth.getDetails()).getNiFiUser();

        assertNotNull(user);
        assertEquals(IDENTITY_1, user.getIdentity());
        assertFalse(user.isAnonymous());

        assertNotNull(user.getChain());
        assertEquals(PROXY_1, user.getChain().getIdentity());
        assertFalse(user.getChain().isAnonymous());
    }

    @Test
    public void testAnonymousWithOneProxy() {
        final NiFiAuthenticationToken auth = (NiFiAuthenticationToken) x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(ANONYMOUS), PROXY_1));
        final NiFiUser user = ((NiFiUserDetails) auth.getDetails()).getNiFiUser();

        assertNotNull(user);
        assertEquals(StandardNiFiUser.ANONYMOUS_IDENTITY, user.getIdentity());
        assertTrue(user.isAnonymous());

        assertNotNull(user.getChain());
        assertEquals(PROXY_1, user.getChain().getIdentity());
        assertFalse(user.getChain().isAnonymous());
    }

    @Test
    public void testTwoProxies() {
        final NiFiAuthenticationToken auth = (NiFiAuthenticationToken) x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(IDENTITY_1, PROXY_2), PROXY_1));
        final NiFiUser user = ((NiFiUserDetails) auth.getDetails()).getNiFiUser();

        assertNotNull(user);
        assertEquals(IDENTITY_1, user.getIdentity());
        assertFalse(user.isAnonymous());

        assertNotNull(user.getChain());
        assertEquals(PROXY_2, user.getChain().getIdentity());
        assertFalse(user.getChain().isAnonymous());

        assertNotNull(user.getChain().getChain());
        assertEquals(PROXY_1, user.getChain().getChain().getIdentity());
        assertFalse(user.getChain().getChain().isAnonymous());
    }

    @Test(expected = UntrustedProxyException.class)
    public void testUntrustedProxyInChain() {
        x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(IDENTITY_1, UNTRUSTED_PROXY), PROXY_1));
    }

    @Test
    public void testAnonymousProxyInChain() {
        final NiFiAuthenticationToken auth = (NiFiAuthenticationToken) x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(IDENTITY_1, ANONYMOUS), PROXY_1));
        final NiFiUser user = ((NiFiUserDetails) auth.getDetails()).getNiFiUser();

        assertNotNull(user);
        assertEquals(IDENTITY_1, user.getIdentity());
        assertFalse(user.isAnonymous());

        assertNotNull(user.getChain());
        assertEquals(StandardNiFiUser.ANONYMOUS_IDENTITY, user.getChain().getIdentity());
        assertTrue(user.getChain().isAnonymous());

        assertNotNull(user.getChain().getChain());
        assertEquals(PROXY_1, user.getChain().getChain().getIdentity());
        assertFalse(user.getChain().getChain().isAnonymous());
    }

    @Test
    public void testShouldCreateAnonymousUser() {
        // Arrange
        String identity = "someone";

        // Act
        NiFiUser user = X509AuthenticationProvider.createUser(identity, null, null, null, true);

        // Assert
        assert user != null;
        assert user instanceof StandardNiFiUser;
        assert user.getIdentity().equals(StandardNiFiUser.ANONYMOUS_IDENTITY);
        assert user.isAnonymous();
    }

    @Test
    public void testShouldCreateKnownUser() {
        // Arrange
        String identity = "someone";

        // Act
        NiFiUser user = X509AuthenticationProvider.createUser(identity, null, null, null, false);

        // Assert
        assert user != null;
        assert user instanceof StandardNiFiUser;
        assert user.getIdentity().equals(identity);
        assert !user.isAnonymous();
    }

    private String buildProxyChain(final String... identities) {
        List<String> elements = Arrays.asList(identities);
        return StringUtils.join(elements.stream().map(X509AuthenticationProviderTest::formatDn).collect(Collectors.toList()), "");
    }

    private static String formatDn(String rawDn) {
        return "<" + sanitizeDn(rawDn) + ">";
    }

    /**
     * If a user provides a DN with the sequence '><', they could escape the tokenization process and impersonate another user.
     * <p>
     * Example:
     * <p>
     * Provided DN: {@code jdoe><alopresto} -> {@code <jdoe><alopresto><proxy...>} would allow the user to impersonate jdoe
     *
     * @param rawDn the unsanitized DN
     * @return the sanitized DN
     */
    private static String sanitizeDn(String rawDn) {
        if (StringUtils.isEmpty(rawDn)) {
            return rawDn;
        } else {
            return rawDn.replaceAll(GT, ESCAPED_GT).replaceAll(LT, ESCAPED_LT);
        }
    }

    /**
     * Reconstitutes the original DN from the sanitized version passed in the proxy chain.
     * <p>
     * Example:
     * <p>
     * {@code alopresto\>\<proxy1} -> {@code alopresto><proxy1}
     *
     * @param sanitizedDn the sanitized DN
     * @return the original DN
     */
    private static String unsanitizeDn(String sanitizedDn) {
        if (StringUtils.isEmpty(sanitizedDn)) {
            return sanitizedDn;
        } else {
            return sanitizedDn.replaceAll(ESCAPED_GT, GT).replaceAll(ESCAPED_LT, LT);
        }
    }

    private X509AuthenticationRequestToken getX509Request(final String proxyChain, final String identity) {
        return new X509AuthenticationRequestToken(proxyChain, extractor, new X509Certificate[]{getX509Certificate(identity)}, "");
    }

    private X509Certificate getX509Certificate(final String identity) {
        final X509Certificate certificate = mock(X509Certificate.class);
        when(certificate.getSubjectDN()).then(invocation -> {
            final Principal principal = mock(Principal.class);
            when(principal.getName()).thenReturn(identity);
            return principal;
        });
        return certificate;
    }

}