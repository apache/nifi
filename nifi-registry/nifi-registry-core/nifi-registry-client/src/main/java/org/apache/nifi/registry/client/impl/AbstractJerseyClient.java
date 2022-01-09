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
package org.apache.nifi.registry.client.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Base class for the client operations to share exception handling.
 *
 * Sub-classes should always execute a request from getRequestBuilder(target) to ensure proper headers are sent.
 */
public class AbstractJerseyClient {

    private static final RequestConfig EMPTY_REQUEST_CONFIG = () ->  Collections.emptyMap();

    private final RequestConfig requestConfig;

    public AbstractJerseyClient(final RequestConfig requestConfig) {
        this.requestConfig = (requestConfig == null ? EMPTY_REQUEST_CONFIG : requestConfig);
    }

    protected RequestConfig getRequestConfig() {
        return this.requestConfig;
    }
    /**
     * Adds query parameters for the given RevisionInfo if populated.
     *
     * @param target the WebTarget
     * @param revision the RevisionInfo
     * @return the target with query params added
     */
    protected WebTarget addRevisionQueryParams(WebTarget target, RevisionInfo revision) {
        if (revision == null) {
            return target;
        }

        WebTarget localTarget = target;

        final Long version = revision.getVersion();
        if (version != null) {
            localTarget = localTarget.queryParam("version", version.longValue());
        }

        final String clientId = revision.getClientId();
        if (!StringUtils.isBlank(clientId)) {
            localTarget = localTarget.queryParam("clientId", clientId);
        }
        return localTarget;
    }

    /**
     * Creates a new Invocation.Builder for the given WebTarget with the headers added to the builder.
     *
     * @param webTarget the target for the request
     * @return the builder for the target with the headers added
     */
    protected Invocation.Builder getRequestBuilder(final WebTarget webTarget) {
        final Invocation.Builder requestBuilder = webTarget.request();

        final Map<String,String> headers = requestConfig.getHeaders();
        headers.entrySet().stream().forEach(e -> requestBuilder.header(e.getKey(), e.getValue()));

        return requestBuilder;
    }

    /**
     * Executes the given action and returns the result.
     *
     * @param action the action to execute
     * @param errorMessage the message to use if a NiFiRegistryException is thrown
     * @param <T> the return type of the action
     * @return the result of the action
     * @throws NiFiRegistryException if any exception other than IOException is encountered
     * @throws IOException if an I/O error occurs communicating with the registry
     */
    protected <T> T executeAction(final String errorMessage, final NiFiRegistryAction<T> action) throws NiFiRegistryException, IOException {
        try {
            return action.execute();
        } catch (final Exception e) {
            final Throwable ioeCause = getIOExceptionCause(e);

            if (ioeCause == null) {
                final StringBuilder errorMessageBuilder = new StringBuilder(errorMessage);

                // see if we have a WebApplicationException, and if so add the response body to the error message
                if (e instanceof WebApplicationException) {
                    final Response response = ((WebApplicationException) e).getResponse();
                    final String responseBody = response.readEntity(String.class);
                    errorMessageBuilder.append(": ").append(responseBody);
                }

                throw new NiFiRegistryException(errorMessageBuilder.toString(), e);
            } else {
                throw (IOException) ioeCause;
            }
        }
    }


    /**
     * An action to execute with the given return type.
     *
     * @param <T> the return type of the action
     */
    protected interface NiFiRegistryAction<T> {

        T execute();

    }

    /**
     * @param e an exception that was encountered interacting with the registry
     * @return the IOException that caused this exception, or null if the an IOException did not cause this exception
     */
    protected Throwable getIOExceptionCause(final Throwable e) {
        if (e == null) {
            return null;
        }

        if (e instanceof IOException) {
            return e;
        }

        return getIOExceptionCause(e.getCause());
    }

}
