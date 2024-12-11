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

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.security.auth.x500.X500Principal;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class X509AuthenticationProviderTest {

    private static final String INVALID_CERTIFICATE = "invalid-certificate";
    private static final String IDENTITY_1 = "CN=identity-1";
    private static final String ANONYMOUS = "";

    private static final String UNTRUSTED_PROXY = "CN=untrusted-proxy";
    private static final String PROXY_1 = "CN=proxy-1";
    private static final String PROXY_2 = "CN=proxy-2";


    private X509AuthenticationProvider x509AuthenticationProvider;
    private X509IdentityProvider certificateIdentityProvider;
    private SubjectDnX509PrincipalExtractor extractor;
    private Authorizer authorizer;

    @BeforeEach
    public void setup() {
        extractor = new SubjectDnX509PrincipalExtractor();

        certificateIdentityProvider = mock(X509IdentityProvider.class);
        when(certificateIdentityProvider.authenticate(any(X509Certificate[].class))).then(invocation -> {
            final X509Certificate[] certChain = invocation.getArgument(0);
            final String identity = extractor.extractPrincipal(certChain[0]).toString();

            if (INVALID_CERTIFICATE.equals(identity)) {
                throw new IllegalArgumentException();
            }

            return new AuthenticationResponse(identity, identity, TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS), "");
        });

        authorizer = mock(Authorizer.class);
        when(authorizer.authorize(any(AuthorizationRequest.class))).then(invocation -> {
            final AuthorizationRequest request = invocation.getArgument(0);

            if (UNTRUSTED_PROXY.equals(request.getIdentity())) {
                return AuthorizationResult.denied();
            }

            return AuthorizationResult.approved();
        });

        x509AuthenticationProvider = new X509AuthenticationProvider(certificateIdentityProvider, authorizer, NiFiProperties.createBasicNiFiProperties(null));
    }

    @Test
    public void testInvalidCertificate() {
        assertThrows(InvalidAuthenticationException.class, () -> x509AuthenticationProvider.authenticate(getX509Request("", INVALID_CERTIFICATE)));
    }

    @Test
    public void testNoProxyChain() {
        final NiFiAuthenticationToken auth = (NiFiAuthenticationToken) x509AuthenticationProvider.authenticate(getX509Request("", IDENTITY_1));
        final NiFiUser user = ((NiFiUserDetails) auth.getDetails()).getNiFiUser();

        assertNotNull(user);
        assertEquals(IDENTITY_1, user.getIdentity());
        assertFalse(user.isAnonymous());
    }

    @Test
    public void testUntrustedProxy() {
        assertThrows(UntrustedProxyException.class, () -> x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(IDENTITY_1), UNTRUSTED_PROXY)));
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
        // override the setting to enable anonymous authentication
        final Map<String, String> additionalProperties = new HashMap<>() {{
            put(NiFiProperties.SECURITY_ANONYMOUS_AUTHENTICATION, Boolean.TRUE.toString());
        }};
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, additionalProperties);
        x509AuthenticationProvider = new X509AuthenticationProvider(certificateIdentityProvider, authorizer, properties);

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
    public void testAnonymousWithOneProxyWhileAnonymousAuthenticationPrevented() {
        assertThrows(InvalidAuthenticationException.class, () -> x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(ANONYMOUS), PROXY_1)));
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

    @Test
    public void testUntrustedProxyInChain() {
        assertThrows(UntrustedProxyException.class, () -> x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(IDENTITY_1, UNTRUSTED_PROXY), PROXY_1)));
    }

    @Test
    public void testAnonymousProxyInChain() {
        // override the setting to enable anonymous authentication
        final Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(NiFiProperties.SECURITY_ANONYMOUS_AUTHENTICATION, Boolean.TRUE.toString());

        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, additionalProperties);
        x509AuthenticationProvider = new X509AuthenticationProvider(certificateIdentityProvider, authorizer, properties);

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
    public void testAnonymousProxyInChainWhileAnonymousAuthenticationPrevented() {
        assertThrows(InvalidAuthenticationException.class, () -> x509AuthenticationProvider.authenticate(getX509Request(buildProxyChain(IDENTITY_1, ANONYMOUS), PROXY_1)));
    }

    @Test
    public void testShouldCreateAnonymousUser() {
        // Arrange
        String identity = "someone";

        // Act
        NiFiUser user = X509AuthenticationProvider.createUser(identity, null, null, null, null, true);

        // Assert
        assertInstanceOf(StandardNiFiUser.class, user);
        assertEquals(StandardNiFiUser.ANONYMOUS_IDENTITY, user.getIdentity());
        assertTrue(user.isAnonymous());
    }

    @Test
    public void testShouldCreateKnownUser() {
        // Arrange
        String identity = "someone";

        // Act
        NiFiUser user = X509AuthenticationProvider.createUser(identity, null, null, null, null, false);

        // Assert
        assertInstanceOf(StandardNiFiUser.class, user);
        assertEquals(identity, user.getIdentity());
        assertFalse(user.isAnonymous());
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
            return rawDn.replaceAll(">", "\\\\>").replaceAll("<", "\\\\<");
        }
    }

    private X509AuthenticationRequestToken getX509Request(final String proxyChain, final String identity) {
        return getX509Request(proxyChain, null, identity);
    }

    private X509AuthenticationRequestToken getX509Request(final String proxyChain, final String proxiedEntityGroups, final String identity) {
        return new X509AuthenticationRequestToken(proxyChain, proxiedEntityGroups, extractor, new X509Certificate[]{getX509Certificate(identity)}, "");
    }

    private X509Certificate getX509Certificate(final String identity) {
        final X509Certificate certificate = mock(X509Certificate.class);
        when(certificate.getSubjectX500Principal()).then(invocation -> new X500Principal(identity));
        return certificate;
    }
}