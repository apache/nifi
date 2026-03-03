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

import org.apache.nifi.components.connector.components.ComponentHierarchyScope;
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
    public CompletableFuture<Void> enableControllerServices(final ControllerServiceReferenceScope referenceScope, final ComponentHierarchyScope hierarchyScope) {
        authContext.authorizeWrite();
        return delegate.enableControllerServices(referenceScope, hierarchyScope);
    }

    @Override
    public CompletableFuture<Void> enableControllerServices(final Collection<String> serviceIdentifiers) {
        authContext.authorizeWrite();
        return delegate.enableControllerServices(serviceIdentifiers);
    }

    @Override
    public CompletableFuture<Void> disableControllerServices(final ComponentHierarchyScope scope) {
        authContext.authorizeWrite();
        return delegate.disableControllerServices(scope);
    }

    @Override
    public CompletableFuture<Void> disableControllerServices(final Collection<String> serviceIdentifiers) {
        authContext.authorizeWrite();
        return delegate.disableControllerServices(serviceIdentifiers);
    }

    @Override
    public CompletableFuture<Void> startProcessors(final ComponentHierarchyScope scope) {
        authContext.authorizeWrite();
        return delegate.startProcessors(scope);
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
    public CompletableFuture<Void> stopProcessors(final ComponentHierarchyScope scope) {
        authContext.authorizeWrite();
        return delegate.stopProcessors(scope);
    }

    @Override
    public CompletableFuture<Void> startPorts(final ComponentHierarchyScope scope) {
        authContext.authorizeWrite();
        return delegate.startPorts(scope);
    }

    @Override
    public CompletableFuture<Void> stopPorts(final ComponentHierarchyScope scope) {
        authContext.authorizeWrite();
        return delegate.stopPorts(scope);
    }

    @Override
    public CompletableFuture<Void> startRemoteProcessGroups(final ComponentHierarchyScope scope) {
        authContext.authorizeWrite();
        return delegate.startRemoteProcessGroups(scope);
    }

    @Override
    public CompletableFuture<Void> stopRemoteProcessGroups(final ComponentHierarchyScope scope) {
        authContext.authorizeWrite();
        return delegate.stopRemoteProcessGroups(scope);
    }

    @Override
    public CompletableFuture<Void> startStatelessGroups(final ComponentHierarchyScope scope) {
        authContext.authorizeWrite();
        return delegate.startStatelessGroups(scope);
    }

    @Override
    public CompletableFuture<Void> stopStatelessGroups(final ComponentHierarchyScope scope) {
        authContext.authorizeWrite();
        return delegate.stopStatelessGroups(scope);
    }

    @Override
    public int getActiveThreadCount() {
        authContext.authorizeRead();
        return delegate.getActiveThreadCount();
    }
}
