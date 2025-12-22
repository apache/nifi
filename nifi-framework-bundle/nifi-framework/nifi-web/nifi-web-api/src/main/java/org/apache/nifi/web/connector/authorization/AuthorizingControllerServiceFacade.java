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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ControllerServiceLifecycle;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameterContext;

import java.util.List;
import java.util.Map;

/**
 * A wrapper around {@link ControllerServiceFacade} that enforces authorization before delegating
 * to the underlying implementation.
 */
public class AuthorizingControllerServiceFacade implements ControllerServiceFacade {

    private final ControllerServiceFacade delegate;
    private final ConnectorAuthorizationContext authContext;

    public AuthorizingControllerServiceFacade(final ControllerServiceFacade delegate, final ConnectorAuthorizationContext authContext) {
        this.delegate = delegate;
        this.authContext = authContext;
    }

    @Override
    public VersionedControllerService getDefinition() {
        authContext.authorizeRead();
        return delegate.getDefinition();
    }

    @Override
    public ControllerServiceLifecycle getLifecycle() {
        authContext.authorizeRead();
        return new AuthorizingControllerServiceLifecycle(delegate.getLifecycle(), authContext);
    }

    @Override
    public List<ValidationResult> validate() {
        authContext.authorizeRead();
        return delegate.validate();
    }

    @Override
    public List<ValidationResult> validate(final Map<String, String> propertyValues) {
        authContext.authorizeRead();
        return delegate.validate(propertyValues);
    }

    @Override
    public List<ConfigVerificationResult> verify(final Map<String, String> propertyValues, final Map<String, String> variables) {
        authContext.authorizeRead();
        return delegate.verify(propertyValues, variables);
    }

    @Override
    public List<ConfigVerificationResult> verify(final Map<String, String> propertyValues, final VersionedParameterContext parameterContext, final Map<String, String> variables) {
        authContext.authorizeRead();
        return delegate.verify(propertyValues, parameterContext, variables);
    }

    @Override
    public List<ConfigVerificationResult> verify(final VersionedExternalFlow versionedExternalFlow, final Map<String, String> variables) {
        authContext.authorizeRead();
        return delegate.verify(versionedExternalFlow, variables);
    }

    @Override
    public Object invokeConnectorMethod(final String methodName, final Map<String, Object> arguments) throws InvocationFailedException {
        authContext.authorizeWrite();
        return delegate.invokeConnectorMethod(methodName, arguments);
    }

    @Override
    public <T> T invokeConnectorMethod(final String methodName, final Map<String, Object> arguments, final Class<T> returnType) throws InvocationFailedException {
        authContext.authorizeWrite();
        return delegate.invokeConnectorMethod(methodName, arguments, returnType);
    }
}

