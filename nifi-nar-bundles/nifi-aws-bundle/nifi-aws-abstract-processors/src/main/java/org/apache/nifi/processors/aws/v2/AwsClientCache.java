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
package org.apache.nifi.processors.aws.v2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.processor.ProcessContext;
import software.amazon.awssdk.core.SdkClient;

public class AwsClientCache<T extends SdkClient> {

    private final Cache<AwsClientDetails, T> clientCache = Caffeine.newBuilder().build();

    public T getOrCreateClient(final ProcessContext context, final AwsClientDetails clientDetails, final AwsClientProvider<T> provider) {
        return clientCache.get(clientDetails, ignored -> provider.createClient(context));
    }

    public void clearCache() {
        clientCache.invalidateAll();
        clientCache.cleanUp();
    }

}
