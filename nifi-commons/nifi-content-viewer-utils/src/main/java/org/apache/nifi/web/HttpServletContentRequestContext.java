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
package org.apache.nifi.web;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.UriBuilder;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;

/**
 * An implementation of the ConfigurationRequestContext that retrieves configuration
 * from a HttpServletRequest instance.
 */
public class HttpServletContentRequestContext implements ContentRequestContext {

    private final String dataRef;
    private final String clusterNodeId;
    private final String clientId;

    private static final String PROXY_CONTEXT_PATH_HTTP_HEADER = "X-ProxyContextPath";
    private static final String FORWARDED_CONTEXT_HTTP_HEADER = "X-Forwarded-Context";
    private static final String FORWARDED_PREFIX_HTTP_HEADER = "X-Forwarded-Prefix";

    public HttpServletContentRequestContext(HttpServletRequest request) {
        final String ref = request.getParameter("ref");
        final UriBuilder refUriBuilder = UriBuilder.fromUri(ref);

        // base the data ref on the request parameter but ensure the scheme is based off the incoming request...
        // this is necessary for scenario's where the NiFi instance is behind a proxy running a different scheme
        refUriBuilder.scheme(request.getScheme());

        // If there is path context from a proxy, remove it since this request will be used inside the cluster
        final String proxyContextPath = getFirstHeaderValue(request, PROXY_CONTEXT_PATH_HTTP_HEADER, FORWARDED_CONTEXT_HTTP_HEADER, FORWARDED_PREFIX_HTTP_HEADER);
        if (StringUtils.isNotBlank(proxyContextPath)) {
            refUriBuilder.replacePath(StringUtils.substringAfter(UriBuilder.fromUri(ref).build().getPath(), proxyContextPath));
        }

        final URI refUri = refUriBuilder.build();

        final String query = refUri.getQuery();

        String rawClusterNodeId = null;
        if (query != null) {
            final String[] queryParameters = query.split("&");

            for (int i = 0; i < queryParameters.length; i++) {
                if (queryParameters[0].startsWith("clusterNodeId=")) {
                    rawClusterNodeId = StringUtils.substringAfterLast(queryParameters[0], "clusterNodeId=");
                }
            }
        }
        final String clusterNodeId = rawClusterNodeId;

        this.dataRef = refUri.toString();
        this.clusterNodeId = clusterNodeId;
        this.clientId = request.getParameter("clientId");
    }

    /**
     * Returns the value for the first key discovered when inspecting the current request. Will
     * return null if there are no keys specified or if none of the specified keys are found.
     *
     * @param keys http header keys
     * @return the value for the first key found
     */
    private String getFirstHeaderValue(HttpServletRequest httpServletRequest, final String... keys) {
        if (keys == null) {
            return null;
        }

        for (final String key : keys) {
            final String value = httpServletRequest.getHeader(key);

            // if we found an entry for this key, return the value
            if (value != null) {
                return value;
            }
        }

        // unable to find any matching keys
        return null;
    }

    @Override
    public String getDataUri() {
        return this.dataRef;
    }

    @Override
    public String getClusterNodeId() {
        return this.clusterNodeId;
    }

    @Override
    public String getClientId() {
        return this.clientId;
    }
}
