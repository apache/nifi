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
package org.apache.nifi.registry.client.impl.request;

import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.security.util.ProxiedEntitiesUtils;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestProxiedEntityRequestConfig {

    @Test
    public void testSingleProxiedEntity() {
        final String proxiedEntity = "user1";
        final String expectedProxiedEntitiesChain = "<user1>";

        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);

        final Map<String,String> headers = requestConfig.getHeaders();
        assertNotNull(headers);
        assertEquals(1, headers.size());

        final String proxiedEntitiesChainHeaderValue = headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN);
        assertEquals(expectedProxiedEntitiesChain, proxiedEntitiesChainHeaderValue);
    }

    @Test
    public void testMultipleProxiedEntity() {
        final String proxiedEntity1 = "user1";
        final String proxiedEntity2 = "user2";
        final String proxiedEntity3 = "user3";
        final String expectedProxiedEntitiesChain = "<user1><user2><user3>";

        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(
                proxiedEntity1, proxiedEntity2, proxiedEntity3);

        final Map<String,String> headers = requestConfig.getHeaders();
        assertNotNull(headers);
        assertEquals(1, headers.size());

        final String proxiedEntitiesChainHeaderValue = headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN);
        assertEquals(expectedProxiedEntitiesChain, proxiedEntitiesChainHeaderValue);
    }
}
