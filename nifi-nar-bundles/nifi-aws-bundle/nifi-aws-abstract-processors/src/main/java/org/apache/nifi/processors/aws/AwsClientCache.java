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
package org.apache.nifi.processors.aws;

import com.amazonaws.AmazonWebServiceClient;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.processor.ProcessContext;

public class AwsClientCache<ClientType extends AmazonWebServiceClient> {

    private static final int MAXIMUM_CACHE_SIZE = 10;

    private final Cache<AwsClientDetails, ClientType> clientCache = Caffeine.newBuilder()
            .maximumSize(MAXIMUM_CACHE_SIZE)
            .build();

    public ClientType getOrCreateClient(final ProcessContext context, final AwsClientDetails clientDetails, final AwsClientProvider<ClientType> provider) {
        return clientCache.get(clientDetails, ignored -> provider.createClient(context, clientDetails));
    }

    public void clearCache() {
        clientCache.invalidateAll();
        clientCache.cleanUp();
    }

}
