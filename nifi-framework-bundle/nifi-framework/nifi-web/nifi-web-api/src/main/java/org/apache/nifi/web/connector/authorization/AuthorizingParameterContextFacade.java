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

import org.apache.nifi.asset.Asset;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ParameterValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * A wrapper around {@link ParameterContextFacade} that enforces authorization before delegating
 * to the underlying implementation.
 */
public class AuthorizingParameterContextFacade implements ParameterContextFacade {

    private final ParameterContextFacade delegate;
    private final ConnectorAuthorizationContext authContext;

    public AuthorizingParameterContextFacade(final ParameterContextFacade delegate, final ConnectorAuthorizationContext authContext) {
        this.delegate = delegate;
        this.authContext = authContext;
    }

    @Override
    public void updateParameters(final Collection<ParameterValue> parameterValues) {
        authContext.authorizeWrite();
        delegate.updateParameters(parameterValues);
    }

    @Override
    public String getValue(final String parameterName) {
        authContext.authorizeRead();
        return delegate.getValue(parameterName);
    }

    @Override
    public Set<String> getDefinedParameterNames() {
        authContext.authorizeRead();
        return delegate.getDefinedParameterNames();
    }

    @Override
    public boolean isSensitive(final String parameterName) {
        authContext.authorizeRead();
        return delegate.isSensitive(parameterName);
    }

    @Override
    public Asset createAsset(final InputStream inputStream) throws IOException {
        authContext.authorizeWrite();
        return delegate.createAsset(inputStream);
    }

    @Override
    public String toString() {
        return "AuthorizingParameterContextFacade[delegate=" + delegate + "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AuthorizingParameterContextFacade that = (AuthorizingParameterContextFacade) o;
        return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(delegate);
    }
}

