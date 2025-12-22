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

import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceHierarchy;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceScope;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * A wrapper around {@link ProcessGroupFacade} that enforces authorization before delegating
 * to the underlying implementation.
 */
public class AuthorizingProcessGroupFacade implements ProcessGroupFacade {

    private final ProcessGroupFacade delegate;
    private final ConnectorAuthorizationContext authContext;

    public AuthorizingProcessGroupFacade(final ProcessGroupFacade delegate, final ConnectorAuthorizationContext authContext) {
        this.delegate = delegate;
        this.authContext = authContext;
    }

    @Override
    public VersionedProcessGroup getDefinition() {
        authContext.authorizeRead();
        return delegate.getDefinition();
    }

    @Override
    public ProcessorFacade getProcessor(final String id) {
        authContext.authorizeRead();
        final ProcessorFacade processor = delegate.getProcessor(id);
        return processor == null ? null : new AuthorizingProcessorFacade(processor, authContext);
    }

    @Override
    public Set<ProcessorFacade> getProcessors() {
        authContext.authorizeRead();
        return delegate.getProcessors().stream()
                .map(p -> new AuthorizingProcessorFacade(p, authContext))
                .collect(Collectors.toSet());
    }

    @Override
    public ControllerServiceFacade getControllerService(final String id) {
        authContext.authorizeRead();
        final ControllerServiceFacade service = delegate.getControllerService(id);
        return service == null ? null : new AuthorizingControllerServiceFacade(service, authContext);
    }

    @Override
    public Set<ControllerServiceFacade> getControllerServices() {
        authContext.authorizeRead();
        return delegate.getControllerServices().stream()
                .map(s -> new AuthorizingControllerServiceFacade(s, authContext))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<ControllerServiceFacade> getControllerServices(final ControllerServiceReferenceScope referenceScope, final ControllerServiceReferenceHierarchy hierarchy) {
        authContext.authorizeRead();
        return delegate.getControllerServices(referenceScope, hierarchy).stream()
                .map(s -> new AuthorizingControllerServiceFacade(s, authContext))
                .collect(Collectors.toSet());
    }

    @Override
    public ConnectionFacade getConnection(final String id) {
        authContext.authorizeRead();
        final ConnectionFacade connection = delegate.getConnection(id);
        return connection == null ? null : new AuthorizingConnectionFacade(connection, authContext);
    }

    @Override
    public Set<ConnectionFacade> getConnections() {
        authContext.authorizeRead();
        return delegate.getConnections().stream()
                .map(c -> new AuthorizingConnectionFacade(c, authContext))
                .collect(Collectors.toSet());
    }

    @Override
    public ProcessGroupFacade getProcessGroup(final String id) {
        authContext.authorizeRead();
        final ProcessGroupFacade group = delegate.getProcessGroup(id);
        return group == null ? null : new AuthorizingProcessGroupFacade(group, authContext);
    }

    @Override
    public Set<ProcessGroupFacade> getProcessGroups() {
        authContext.authorizeRead();
        return delegate.getProcessGroups().stream()
                .map(g -> new AuthorizingProcessGroupFacade(g, authContext))
                .collect(Collectors.toSet());
    }

    @Override
    public QueueSize getQueueSize() {
        authContext.authorizeRead();
        return delegate.getQueueSize();
    }

    @Override
    public boolean isFlowEmpty() {
        authContext.authorizeRead();
        return delegate.isFlowEmpty();
    }

    @Override
    public StatelessGroupLifecycle getStatelessLifecycle() {
        authContext.authorizeRead();
        return new AuthorizingStatelessGroupLifecycle(delegate.getStatelessLifecycle(), authContext);
    }

    @Override
    public ProcessGroupLifecycle getLifecycle() {
        authContext.authorizeRead();
        return new AuthorizingProcessGroupLifecycle(delegate.getLifecycle(), authContext);
    }
}

