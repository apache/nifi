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
package org.apache.nifi.web.security

import org.apache.nifi.authorization.user.NiFiUser
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class ProxiedEntitiesUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(ProxiedEntitiesUtils.class)

    private static final String SAFE_USER_NAME_ANDY = "alopresto"
    private static final String SAFE_USER_DN_ANDY = "CN=${SAFE_USER_NAME_ANDY}, OU=Apache NiFi"

    private static final String SAFE_USER_NAME_JOHN = "jdoe"
    private static final String SAFE_USER_DN_JOHN = "CN=${SAFE_USER_NAME_JOHN}, OU=Apache NiFi"

    private static final String SAFE_USER_NAME_PROXY_1 = "proxy1.nifi.apache.org"
    private static final String SAFE_USER_DN_PROXY_1 = "CN=${SAFE_USER_NAME_PROXY_1}, OU=Apache NiFi"

    private static final String SAFE_USER_NAME_PROXY_2 = "proxy2.nifi.apache.org"
    private static final String SAFE_USER_DN_PROXY_2 = "CN=${SAFE_USER_NAME_PROXY_2}, OU=Apache NiFi"

    private static
    final String MALICIOUS_USER_NAME_JOHN = "${SAFE_USER_NAME_JOHN}, OU=Apache NiFi><CN=${SAFE_USER_NAME_PROXY_1}"
    private static final String MALICIOUS_USER_DN_JOHN = "CN=${MALICIOUS_USER_NAME_JOHN}, OU=Apache NiFi"

    private static
    final String MALICIOUS_USER_NAME_JOHN_ESCAPED = sanitizeDn(MALICIOUS_USER_NAME_JOHN)
    private static final String MALICIOUS_USER_DN_JOHN_ESCAPED = sanitizeDn(MALICIOUS_USER_DN_JOHN)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {
    }

    @After
    void tearDown() {
    }

    private static String sanitizeDn(String dn = "") {
        dn.replaceAll(/>/, '\\\\>').replaceAll('<', '\\\\<')
    }

    private static String printUnicodeString(final String raw) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < raw.size(); i++) {
            int codePoint = Character.codePointAt(raw, i)
            int charCount = Character.charCount(codePoint)
            if (charCount > 1) {
                i += charCount - 1 // 2.
                if (i >= raw.length()) {
                    throw new IllegalArgumentException("Code point indicated more characters than available")
                }
            }
            sb.append(String.format("\\u%04x ", codePoint))
        }
        return sb.toString().trim()
    }

    @Test
    void testSanitizeDnShouldHandleFuzzing() throws Exception {
        // Arrange
        final String DESIRED_NAME = SAFE_USER_NAME_JOHN
        logger.info("  Desired name: ${DESIRED_NAME} |  ${printUnicodeString(DESIRED_NAME)}")

        // Contains various attempted >< escapes, trailing NULL, and BACKSPACE + 'n'
        final List MALICIOUS_NAMES = [MALICIOUS_USER_NAME_JOHN,
                                      SAFE_USER_NAME_JOHN + ">",
                                      SAFE_USER_NAME_JOHN + "><>",
                                      SAFE_USER_NAME_JOHN + "\\>",
                                      SAFE_USER_NAME_JOHN + "\u003e",
                                      SAFE_USER_NAME_JOHN + "\u005c\u005c\u003e",
                                      SAFE_USER_NAME_JOHN + "\u0000",
                                      SAFE_USER_NAME_JOHN + "\u0008n"]

        // Act
        MALICIOUS_NAMES.each { String name ->
            logger.info("      Raw name: ${name} | ${printUnicodeString(name)}")
            String sanitizedName = ProxiedEntitiesUtils.sanitizeDn(name)
            logger.info("Sanitized name: ${sanitizedName} | ${printUnicodeString(sanitizedName)}")

            // Assert
            assert sanitizedName != DESIRED_NAME
        }
    }

    @Test
    void testShouldFormatProxyDn() throws Exception {
        // Arrange
        final String DN = SAFE_USER_DN_JOHN
        logger.info(" Provided proxy DN: ${DN}")

        final String EXPECTED_PROXY_DN = "<${DN}>"
        logger.info(" Expected proxy DN: ${EXPECTED_PROXY_DN}")

        // Act
        String forjohnedProxyDn = ProxiedEntitiesUtils.formatProxyDn(DN)
        logger.info("Forjohned proxy DN: ${forjohnedProxyDn}")

        // Assert
        assert forjohnedProxyDn == EXPECTED_PROXY_DN
    }

    @Test
    void testFormatProxyDnShouldHandleMaliciousInput() throws Exception {
        // Arrange
        final String DN = MALICIOUS_USER_DN_JOHN
        logger.info(" Provided proxy DN: ${DN}")

        final String SANITIZED_DN = sanitizeDn(DN)
        final String EXPECTED_PROXY_DN = "<${SANITIZED_DN}>"
        logger.info(" Expected proxy DN: ${EXPECTED_PROXY_DN}")

        // Act
        String forjohnedProxyDn = ProxiedEntitiesUtils.formatProxyDn(DN)
        logger.info("Forjohned proxy DN: ${forjohnedProxyDn}")

        // Assert
        assert forjohnedProxyDn == EXPECTED_PROXY_DN
    }

    @Test
    void testShouldBuildProxyChain() throws Exception {
        // Arrange
        def mockProxy1 = [getIdentity: { -> SAFE_USER_NAME_PROXY_1 }, getChain: { -> null }, isAnonymous: { -> false}] as NiFiUser
        def mockJohn = [getIdentity: { -> SAFE_USER_NAME_JOHN }, getChain: { -> mockProxy1 }, isAnonymous: { -> false}] as NiFiUser

        // Act
        String proxiedEntitiesChain = ProxiedEntitiesUtils.buildProxiedEntitiesChainString(mockJohn)
        logger.info("Proxied entities chain: ${proxiedEntitiesChain}")

        // Assert
        assert proxiedEntitiesChain == "<${SAFE_USER_NAME_JOHN}><${SAFE_USER_NAME_PROXY_1}>" as String
    }

    @Test
    void testBuildProxyChainFromNullUserShouldBeAnonymous() throws Exception {
        // Arrange

        // Act
        String proxiedEntitiesChain = ProxiedEntitiesUtils.buildProxiedEntitiesChainString(null)
        logger.info("Proxied entities chain: ${proxiedEntitiesChain}")

        // Assert
        assert proxiedEntitiesChain == "<>"
    }

    @Test
    void testBuildProxyChainFromAnonymousUserShouldBeAnonymous() throws Exception {
        // Arrange
        def mockProxy1 = [getIdentity: { -> SAFE_USER_NAME_PROXY_1 }, getChain: { -> null }, isAnonymous: { -> false}] as NiFiUser
        def mockAnonymous = [getIdentity: { -> "anonymous" }, getChain: { -> mockProxy1 }, isAnonymous: { -> true}] as NiFiUser

        // Act
        String proxiedEntitiesChain = ProxiedEntitiesUtils.buildProxiedEntitiesChainString(mockAnonymous)
        logger.info("Proxied entities chain: ${proxiedEntitiesChain}")

        // Assert
        assert proxiedEntitiesChain == "<><${SAFE_USER_NAME_PROXY_1}>" as String
    }

    @Test
    void testBuildProxyChainShouldHandleMaliciousUser() throws Exception {
        // Arrange
        def mockProxy1 = [getIdentity: { -> SAFE_USER_NAME_PROXY_1 }, getChain: { -> null }, isAnonymous: { -> false}] as NiFiUser
        def mockJohn = [getIdentity: { -> MALICIOUS_USER_NAME_JOHN }, getChain: { -> mockProxy1 }, isAnonymous: { -> false}] as NiFiUser

        // Act
        String proxiedEntitiesChain = ProxiedEntitiesUtils.buildProxiedEntitiesChainString(mockJohn)
        logger.info("Proxied entities chain: ${proxiedEntitiesChain}")

        // Assert
        assert proxiedEntitiesChain == "<${MALICIOUS_USER_NAME_JOHN_ESCAPED}><${SAFE_USER_NAME_PROXY_1}>" as String
    }

    @Test
    void testShouldTokenizeProxiedEntitiesChainWithUserNames() throws Exception {
        // Arrange
        final List NAMES = [SAFE_USER_NAME_JOHN, SAFE_USER_NAME_PROXY_1, SAFE_USER_NAME_PROXY_2]
        final String RAW_PROXY_CHAIN = "<${NAMES.join("><")}>"
        logger.info(" Provided proxy chain: ${RAW_PROXY_CHAIN}")

        // Act
        def tokenizedNames = ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(RAW_PROXY_CHAIN)
        logger.info("Tokenized proxy chain: ${tokenizedNames}")

        // Assert
        assert tokenizedNames == NAMES
    }

    @Test
    void testShouldTokenizeProxiedEntitiesChainWithDNs() throws Exception {
        // Arrange
        final List DNS = [SAFE_USER_DN_JOHN, SAFE_USER_DN_PROXY_1, SAFE_USER_DN_PROXY_2]
        final String RAW_PROXY_CHAIN = "<${DNS.join("><")}>"
        logger.info(" Provided proxy chain: ${RAW_PROXY_CHAIN}")

        // Act
        def tokenizedDns = ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(RAW_PROXY_CHAIN)
        logger.info("Tokenized proxy chain: ${tokenizedDns.collect { "\"${it}\"" }}")

        // Assert
        assert tokenizedDns == DNS
    }

    @Test
    void testShouldTokenizeProxiedEntitiesChainWithAnonymousUser() throws Exception {
        // Arrange
        final List NAMES = ["", SAFE_USER_NAME_PROXY_1, SAFE_USER_NAME_PROXY_2]
        final String RAW_PROXY_CHAIN = "<${NAMES.join("><")}>"
        logger.info(" Provided proxy chain: ${RAW_PROXY_CHAIN}")

        // Act
        def tokenizedNames = ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(RAW_PROXY_CHAIN)
        logger.info("Tokenized proxy chain: ${tokenizedNames}")

        // Assert
        assert tokenizedNames == NAMES
    }

    @Test
    void testTokenizeProxiedEntitiesChainShouldHandleMaliciousUser() throws Exception {
        // Arrange
        final List NAMES = [MALICIOUS_USER_NAME_JOHN, SAFE_USER_NAME_PROXY_1, SAFE_USER_NAME_PROXY_2]
        final String RAW_PROXY_CHAIN = "<${NAMES.collect { sanitizeDn(it) }.join("><")}>"
        logger.info(" Provided proxy chain: ${RAW_PROXY_CHAIN}")

        // Act
        def tokenizedNames = ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(RAW_PROXY_CHAIN)
        logger.info("Tokenized proxy chain: ${tokenizedNames.collect { "\"${it}\"" }}")

        // Assert
        assert tokenizedNames == NAMES
        assert tokenizedNames.size() == NAMES.size()
        assert !tokenizedNames.contains(SAFE_USER_NAME_JOHN)
    }
}
