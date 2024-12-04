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
package org.apache.nifi.web.security;

import org.apache.nifi.authorization.user.NiFiUser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ProxiedEntitiesUtilsTest {
    private static final String SAFE_USER_NAME_JOHN = "jdoe";
    private static final String SAFE_USER_DN_JOHN = "CN=" + SAFE_USER_NAME_JOHN + ", OU=Apache NiFi";
    private static final String SAFE_USER_NAME_PROXY_1 = "proxy1.nifi.apache.org";
    private static final String SAFE_USER_DN_PROXY_1 = "CN=" + SAFE_USER_NAME_PROXY_1 + ", OU=Apache NiFi";
    private static final String SAFE_USER_NAME_PROXY_2 = "proxy2.nifi.apache.org";
    private static final String SAFE_USER_DN_PROXY_2 = "CN=" + SAFE_USER_NAME_PROXY_2 + ", OU=Apache NiFi";
    private static final String MALICIOUS_USER_NAME_JOHN = SAFE_USER_NAME_JOHN + ", OU=Apache NiFi><CN=" + SAFE_USER_NAME_PROXY_1;
    private static final String MALICIOUS_USER_NAME_JOHN_ESCAPED = sanitizeDn(MALICIOUS_USER_NAME_JOHN);
    private static final String UNICODE_DN_1 = "CN=Алйс, OU=Apache NiFi";
    private static final String UNICODE_DN_1_ENCODED = "<" + base64Encode(UNICODE_DN_1) + ">";
    private static final String UNICODE_DN_2 = "CN=Боб, OU=Apache NiFi";
    private static final String UNICODE_DN_2_ENCODED = "<" + base64Encode(UNICODE_DN_2) + ">";
    private static final String ANONYMOUS_USER = "";
    private static final String ANONYMOUS_PROXIED_ENTITY_CHAIN = "<>";

    private static String sanitizeDn(String dn) {
        return dn.replaceAll(">", "\\\\>").replaceAll("<", "\\\\<");
    }

    private static String base64Encode(String dn) {
        return Base64.getEncoder().encodeToString(dn.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testShouldBuildProxyChain(@Mock NiFiUser proxy1, @Mock NiFiUser john) {
        when(proxy1.getIdentity()).thenReturn(SAFE_USER_NAME_PROXY_1);
        when(proxy1.getChain()).thenReturn(null);
        when(proxy1.isAnonymous()).thenReturn(false);
        when(john.getIdentity()).thenReturn(SAFE_USER_NAME_JOHN);
        when(john.getChain()).thenReturn(proxy1);
        when(john.isAnonymous()).thenReturn(false);

        assertEquals(formatDns(SAFE_USER_NAME_JOHN, SAFE_USER_NAME_PROXY_1), ProxiedEntitiesUtils.buildProxiedEntitiesChainString(john));
    }

    @Test
    public void testBuildProxyChainFromNullUserShouldBeAnonymous() {
        assertEquals(ANONYMOUS_PROXIED_ENTITY_CHAIN, ProxiedEntitiesUtils.buildProxiedEntitiesChainString(null));
    }

    @Test
    public void testBuildProxyChainFromAnonymousUserShouldBeAnonymous(@Mock NiFiUser proxy1, @Mock NiFiUser anonymous) {
        when(proxy1.getIdentity()).thenReturn(SAFE_USER_NAME_PROXY_1);
        when(proxy1.getChain()).thenReturn(null);
        when(proxy1.isAnonymous()).thenReturn(false);
        when(anonymous.getChain()).thenReturn(proxy1);
        when(anonymous.isAnonymous()).thenReturn(true);

        assertEquals(formatDns(ANONYMOUS_USER, SAFE_USER_NAME_PROXY_1), ProxiedEntitiesUtils.buildProxiedEntitiesChainString(anonymous));
    }

    @Test
    public void testBuildProxyChainShouldHandleUnicode(@Mock NiFiUser proxy1, @Mock NiFiUser john) {
        when(proxy1.getIdentity()).thenReturn(UNICODE_DN_1);
        when(proxy1.getChain()).thenReturn(null);
        when(proxy1.isAnonymous()).thenReturn(false);
        when(john.getIdentity()).thenReturn(SAFE_USER_NAME_JOHN);
        when(john.getChain()).thenReturn(proxy1);
        when(john.isAnonymous()).thenReturn(false);

        assertEquals(formatDns(SAFE_USER_NAME_JOHN, UNICODE_DN_1_ENCODED), ProxiedEntitiesUtils.buildProxiedEntitiesChainString(john));
    }

    @Test
    public void testBuildProxyChainShouldHandleMaliciousUser(@Mock NiFiUser proxy1, @Mock NiFiUser john) {
        when(proxy1.getIdentity()).thenReturn(SAFE_USER_NAME_PROXY_1);
        when(proxy1.getChain()).thenReturn(null);
        when(proxy1.isAnonymous()).thenReturn(false);
        when(john.getIdentity()).thenReturn(MALICIOUS_USER_NAME_JOHN);
        when(john.getChain()).thenReturn(proxy1);
        when(john.isAnonymous()).thenReturn(false);

        assertEquals(formatDns(MALICIOUS_USER_NAME_JOHN_ESCAPED, SAFE_USER_NAME_PROXY_1), ProxiedEntitiesUtils.buildProxiedEntitiesChainString(john));
    }

    @Test
    public void testShouldTokenizeProxiedEntitiesChainWithUserNames() {
        final List<String> names = Arrays.asList(SAFE_USER_NAME_JOHN, SAFE_USER_NAME_PROXY_1, SAFE_USER_NAME_PROXY_2);
        final String rawProxyChain = formatDns(names.toArray(new String[0]));

        assertEquals(names, ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(rawProxyChain));
    }

    @Test
    public void testShouldTokenizeAnonymous() {
        assertEquals(Collections.singletonList(ANONYMOUS_USER), ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(ANONYMOUS_PROXIED_ENTITY_CHAIN));
    }

    @Test
    public void testShouldTokenizeDoubleAnonymous() {
        assertEquals(Arrays.asList(ANONYMOUS_USER, ANONYMOUS_USER), ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(ANONYMOUS_PROXIED_ENTITY_CHAIN.repeat(2)));
    }

    @Test
    public void testShouldTokenizeNestedAnonymous() {
        final List<String> names = Arrays.asList(SAFE_USER_DN_PROXY_1, ANONYMOUS_USER, SAFE_USER_DN_PROXY_2);
        final String rawProxyChain = formatDns(names.toArray(new String[0]));

        assertEquals(names, ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(rawProxyChain));
    }

    @Test
    public void testShouldTokenizeProxiedEntitiesChainWithDNs() {
        final List<String> dns = Arrays.asList(SAFE_USER_DN_JOHN, SAFE_USER_DN_PROXY_1, SAFE_USER_DN_PROXY_2);
        final String rawProxyChain = formatDns(dns.toArray(new String[0]));

        assertEquals(dns, ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(rawProxyChain));
    }

    @Test
    public void testShouldTokenizeProxiedEntitiesChainWithAnonymousUser() {
        final List<String> names = Arrays.asList(ANONYMOUS_USER, SAFE_USER_NAME_PROXY_1, SAFE_USER_NAME_PROXY_2);
        final String rawProxyChain = formatDns(names.toArray(new String[0]));

        assertEquals(names, ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(rawProxyChain));
    }

    @Test
    public void testTokenizeProxiedEntitiesChainShouldHandleMaliciousUser() {
        final List<String> names = Arrays.asList(MALICIOUS_USER_NAME_JOHN, SAFE_USER_NAME_PROXY_1, SAFE_USER_NAME_PROXY_2);
        final String rawProxyChain = names.stream()
                .map(this::formatSanitizedDn)
                .collect(Collectors.joining());
        List<String> tokenizedNames = ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(rawProxyChain);

        assertEquals(names, tokenizedNames);
        assertFalse(tokenizedNames.contains(SAFE_USER_NAME_JOHN));
    }

    @Test
    public void testTokenizeProxiedEntitiesChainShouldDecodeNonAsciiValues() {
        List<String> tokenizedNames =
                ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(formatDns(SAFE_USER_NAME_JOHN, UNICODE_DN_1_ENCODED, UNICODE_DN_2_ENCODED));

        assertEquals(Arrays.asList(SAFE_USER_NAME_JOHN, UNICODE_DN_1, UNICODE_DN_2), tokenizedNames);
    }

    private String formatSanitizedDn(String dn) {
        return formatDn((sanitizeDn(dn)));
    }

    private String formatDn(String dn) {
        return formatDns(dn);
    }

    private String formatDns(String... dns) {
        return Arrays.stream(dns)
                .collect(Collectors.joining("><", "<", ">"));
    }
}
