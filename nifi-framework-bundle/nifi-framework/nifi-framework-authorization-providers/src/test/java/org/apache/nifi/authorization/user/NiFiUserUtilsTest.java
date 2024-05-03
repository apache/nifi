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
package org.apache.nifi.authorization.user;

import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NiFiUserUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(NiFiUserUtilsTest.class);

    private static final String SAFE_USER_NAME_JOHN = "jdoe";
    private static final String SAFE_USER_NAME_PROXY_1 = "proxy1.nifi.apache.org";

    @Test
    public void testShouldBuildProxyChain() {
        final NiFiUser mockProxy1 = createMockNiFiUser(SAFE_USER_NAME_PROXY_1, null, false);
        final NiFiUser mockJohn = createMockNiFiUser(SAFE_USER_NAME_JOHN, mockProxy1, false);

        final List<String> proxiedEntitiesChain = NiFiUserUtils.buildProxiedEntitiesChain(mockJohn);
        logger.info(String.format("Proxied entities chain: %s", proxiedEntitiesChain));

        assertEquals(proxiedEntitiesChain, Arrays.asList(SAFE_USER_NAME_JOHN, SAFE_USER_NAME_PROXY_1));
    }

    @Test
    public void testShouldBuildProxyChainFromSingleUser() {
        final NiFiUser mockJohn = createMockNiFiUser(SAFE_USER_NAME_JOHN, null, false);

        final List<String> proxiedEntitiesChain = NiFiUserUtils.buildProxiedEntitiesChain(mockJohn);

        assertEquals(proxiedEntitiesChain, Arrays.asList(SAFE_USER_NAME_JOHN));
    }

    @Test
    public void testShouldBuildProxyChainFromAnonymousUser() throws Exception {
        final NiFiUser mockProxy1 = createMockNiFiUser(SAFE_USER_NAME_PROXY_1, null, false);
        final NiFiUser mockAnonymous = createMockNiFiUser("anonymous", mockProxy1, true);

        final List<String> proxiedEntitiesChain = NiFiUserUtils.buildProxiedEntitiesChain(mockAnonymous);
        logger.info(String.format("Proxied entities chain: %s", proxiedEntitiesChain));

        assertEquals(proxiedEntitiesChain, Arrays.asList("", SAFE_USER_NAME_PROXY_1));
    }

    @Test
    public void testBuildProxyChainFromNullUserShouldBeEmpty() {
        final List<String> proxiedEntitiesChain = NiFiUserUtils.buildProxiedEntitiesChain(null);
        logger.info(String.format("Proxied entities chain: %s", proxiedEntitiesChain));

        assertEquals(proxiedEntitiesChain, Collections.EMPTY_LIST);
    }

    private NiFiUser createMockNiFiUser(final String identity, final NiFiUser chain, final boolean isAnonymous) {
        final NiFiUser mockUser = mock(NiFiUser.class);
        when(mockUser.getIdentity()).thenReturn(identity);
        when(mockUser.getChain()).thenReturn(chain);
        when(mockUser.isAnonymous()).thenReturn(isAnonymous);
        return mockUser;
    }
}
