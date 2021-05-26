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

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.security.util.ProxiedEntitiesUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of RequestConfig that produces headers for a request with proxied-entities.
 */
public class ProxiedEntityRequestConfig implements RequestConfig {

    private final String proxiedEntitiesChain;

    public ProxiedEntityRequestConfig(final String... proxiedEntities) {
        Validate.notNull(proxiedEntities);
        this.proxiedEntitiesChain = ProxiedEntitiesUtils.getProxiedEntitiesChain(proxiedEntities);
    }

    @Override
    public Map<String, String> getHeaders() {
        final Map<String,String> headers = new HashMap<>();
        if (proxiedEntitiesChain != null) {
            headers.put(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, proxiedEntitiesChain);
        }
        return headers;
    }

}