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
package org.apache.nifi.flow.resource;

import org.apache.nifi.nar.NarProvider;
import org.apache.nifi.nar.NarProviderInitializationContext;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@Deprecated
public class NarProviderAdapter implements ExternalResourceProvider {
    private final NarProvider payload;

    public NarProviderAdapter(final NarProvider payload) {
        this.payload = payload;
    }

    @Override
    public void initialize(final ExternalResourceProviderInitializationContext context) {
        payload.initialize(new NarProviderInitializationContextAdapter(context));
    }

    @Override
    public Collection<ExternalResourceDescriptor> listResources() throws IOException {
        // NarProvider is not capable to return information about modification time, it always returns with actual time.
        // {@code ExternalResourceConflictResolutionStrategy} must be set accordingly.
        return payload.listNars().stream().map(f -> new ImmutableExternalResourceDescriptor(f, System.currentTimeMillis())).collect(Collectors.toList());
    }

    @Override
    public InputStream fetchExternalResource(final ExternalResourceDescriptor descriptor) throws IOException {
        return payload.fetchNarContents(descriptor.getLocation());
    }

    private static class NarProviderInitializationContextAdapter implements NarProviderInitializationContext {
        private final ExternalResourceProviderInitializationContext payload;

        private NarProviderInitializationContextAdapter(final ExternalResourceProviderInitializationContext payload) {
            this.payload = payload;
        }

        @Override
        public Map<String, String> getProperties() {
            return payload.getProperties();
        }

        @Override
        public SSLContext getNiFiSSLContext() {
            return payload.getSSLContext();
        }
    }
}
