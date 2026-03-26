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

import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;

/**
 * A wrapper around {@link FlowContext} that enforces authorization before delegating
 * to the underlying implementation.
 */
public class AuthorizingFlowContext implements FlowContext {

    private final FlowContext delegate;
    private final ConnectorAuthorizationContext authContext;

    public AuthorizingFlowContext(final FlowContext delegate, final ConnectorAuthorizationContext authContext) {
        this.delegate = delegate;
        this.authContext = authContext;
    }

    @Override
    public ProcessGroupFacade getRootGroup() {
        authContext.authorizeRead();
        return new AuthorizingProcessGroupFacade(delegate.getRootGroup(), authContext);
    }

    @Override
    public ParameterContextFacade getParameterContext() {
        authContext.authorizeRead();
        return new AuthorizingParameterContextFacade(delegate.getParameterContext(), authContext);
    }

    @Override
    public ConnectorConfigurationContext getConfigurationContext() {
        authContext.authorizeRead();
        return delegate.getConfigurationContext();
    }

    @Override
    public FlowContextType getType() {
        authContext.authorizeRead();
        return delegate.getType();
    }

    @Override
    public Bundle getBundle() {
        authContext.authorizeRead();
        return delegate.getBundle();
    }
}

