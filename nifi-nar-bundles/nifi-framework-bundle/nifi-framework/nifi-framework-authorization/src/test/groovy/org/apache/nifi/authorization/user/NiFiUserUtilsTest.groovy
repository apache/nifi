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
package org.apache.nifi.authorization.user

import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class NiFiUserUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(NiFiUserUtilsTest.class)

    private static final String SAFE_USER_NAME_ANDY = "alopresto"
    private static final String SAFE_USER_DN_ANDY = "CN=${SAFE_USER_NAME_ANDY}, OU=Apache NiFi"

    private static final String SAFE_USER_NAME_JOHN = "jdoe"
    private static final String SAFE_USER_DN_JOHN = "CN=${SAFE_USER_NAME_JOHN}, OU=Apache NiFi"

    private static final String SAFE_USER_NAME_PROXY_1 = "proxy1.nifi.apache.org"
    private static final String SAFE_USER_DN_PROXY_1 = "CN=${SAFE_USER_NAME_PROXY_1}, OU=Apache NiFi"

    private static final String SAFE_USER_NAME_PROXY_2 = "proxy2.nifi.apache.org"
    private static final String SAFE_USER_DN_PROXY_2 = "CN=${SAFE_USER_NAME_PROXY_2}, OU=Apache NiFi"

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

    @Test
    void testShouldBuildProxyChain() throws Exception {
        // Arrange
        def mockProxy1 = [getIdentity: { -> SAFE_USER_NAME_PROXY_1}, getChain: { -> null}, isAnonymous: { -> false}] as NiFiUser
        def mockJohn = [getIdentity: { -> SAFE_USER_NAME_JOHN}, getChain: { -> mockProxy1}, isAnonymous: { -> false}] as NiFiUser

        // Act
        List<String> proxiedEntitiesChain = NiFiUserUtils.buildProxiedEntitiesChain(mockJohn)
        logger.info("Proxied entities chain: ${proxiedEntitiesChain}")

        // Assert
        assert proxiedEntitiesChain == [SAFE_USER_NAME_JOHN, SAFE_USER_NAME_PROXY_1]
    }

    @Test
    void testShouldBuildProxyChainFromSingleUser() throws Exception {
        // Arrange
        def mockJohn = [getIdentity: { -> SAFE_USER_NAME_JOHN}, getChain: { -> null}, isAnonymous: { -> false}] as NiFiUser

        // Act
        List<String> proxiedEntitiesChain = NiFiUserUtils.buildProxiedEntitiesChain(mockJohn)
        logger.info("Proxied entities chain: ${proxiedEntitiesChain}")

        // Assert
        assert proxiedEntitiesChain == [SAFE_USER_NAME_JOHN]
    }

    @Test
    void testShouldBuildProxyChainFromAnonymousUser() throws Exception {
        // Arrange
        def mockProxy1 = [getIdentity: { -> SAFE_USER_NAME_PROXY_1}, getChain: { -> null}, isAnonymous: { -> false}] as NiFiUser
        def mockAnonymous = [getIdentity: { -> "anonymous"}, getChain: { -> mockProxy1}, isAnonymous: { -> true}] as NiFiUser

        // Act
        List<String> proxiedEntitiesChain = NiFiUserUtils.buildProxiedEntitiesChain(mockAnonymous)
        logger.info("Proxied entities chain: ${proxiedEntitiesChain}")

        // Assert
        assert proxiedEntitiesChain == ["", SAFE_USER_NAME_PROXY_1]
    }

    @Test
    void testBuildProxyChainFromNullUserShouldBeEmpty() throws Exception {
        // Arrange

        // Act
        List<String> proxiedEntitiesChain = NiFiUserUtils.buildProxiedEntitiesChain(null)
        logger.info("Proxied entities chain: ${proxiedEntitiesChain}")

        // Assert
        assert proxiedEntitiesChain == []
    }
}
