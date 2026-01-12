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
package org.apache.nifi.web.connector.authorization;

import org.apache.nifi.components.connector.components.ControllerServiceReferenceHierarchy;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceScope;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper around {@link ProcessGroupLifecycle} that enforces authorization before delegating
 * to the underlying implementation. All lifecycle operations require WRITE authorization.
 */
public class AuthorizingProcessGroupLifecycle implements ProcessGroupLifecycle {

    private final ProcessGroupLifecycle delegate;
    private final ConnectorAuthorizationContext authContext;

    public AuthorizingProcessGroupLifecycle(final ProcessGroupLifecycle delegate, final ConnectorAuthorizationContext authContext) {
        this.delegate = delegate;
        this.authContext = authContext;
    }

    @Override
    public CompletableFuture<Void> enableControllerServices(final ControllerServiceReferenceScope scope, final ControllerServiceReferenceHierarchy hierarchy) {
        authContext.authorizeWrite();
        return delegate.enableControllerServices(scope, hierarchy);
    }

    @Override
    public CompletableFuture<Void> enableControllerServices(final Collection<String> serviceIdentifiers) {
        authContext.authorizeWrite();
        return delegate.enableControllerServices(serviceIdentifiers);
    }

    @Override
    public CompletableFuture<Void> disableControllerServices(final ControllerServiceReferenceHierarchy hierarchy) {
        authContext.authorizeWrite();
        return delegate.disableControllerServices(hierarchy);
    }

    @Override
    public CompletableFuture<Void> disableControllerServices(final Collection<String> serviceIdentifiers) {
        authContext.authorizeWrite();
        return delegate.disableControllerServices(serviceIdentifiers);
    }

    @Override
    public CompletableFuture<Void> startProcessors() {
        authContext.authorizeWrite();
        return delegate.startProcessors();
    }

    @Override
    public CompletableFuture<Void> start(final ControllerServiceReferenceScope serviceReferenceScope) {
        authContext.authorizeWrite();
        return delegate.start(serviceReferenceScope);
    }

    @Override
    public CompletableFuture<Void> stop() {
        authContext.authorizeWrite();
        return delegate.stop();
    }

    @Override
    public CompletableFuture<Void> stopProcessors() {
        authContext.authorizeWrite();
        return delegate.stopProcessors();
    }

    @Override
    public int getActiveThreadCount() {
        authContext.authorizeRead();
        return delegate.getActiveThreadCount();
    }
}

