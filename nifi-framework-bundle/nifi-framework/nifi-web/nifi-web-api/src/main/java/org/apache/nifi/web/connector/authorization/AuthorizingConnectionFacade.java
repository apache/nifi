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

import org.apache.nifi.components.connector.DropFlowFileSummary;
import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flowfile.FlowFile;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A wrapper around {@link ConnectionFacade} that enforces authorization before delegating
 * to the underlying implementation.
 */
public class AuthorizingConnectionFacade implements ConnectionFacade {

    private final ConnectionFacade delegate;
    private final ConnectorAuthorizationContext authContext;

    public AuthorizingConnectionFacade(final ConnectionFacade delegate, final ConnectorAuthorizationContext authContext) {
        this.delegate = delegate;
        this.authContext = authContext;
    }

    @Override
    public VersionedConnection getDefinition() {
        authContext.authorizeRead();
        return delegate.getDefinition();
    }

    @Override
    public QueueSize getQueueSize() {
        authContext.authorizeRead();
        return delegate.getQueueSize();
    }

    @Override
    public void purge() {
        authContext.authorizeWrite();
        delegate.purge();
    }

    @Override
    public DropFlowFileSummary dropFlowFiles(final Predicate<FlowFile> predicate) throws IOException {
        authContext.authorizeWrite();
        return delegate.dropFlowFiles(predicate);
    }

    @Override
    public String toString() {
        return "AuthorizingConnectionFacade[delegate=" + delegate + "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AuthorizingConnectionFacade that = (AuthorizingConnectionFacade) o;
        return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(delegate);
    }
}

