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
package org.apache.nifi.web.security.util;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard implementation of Resource URI Resolver replaces REST API path with a configurable path
 */
public class StandardResourceUriResolver implements ResourceUriResolver {
    private static final Pattern REST_API_PATTERN = Pattern.compile("/nifi-api.*");

    private static final String ROOT_PATH = "/";

    private final String replacementPath;

    /**
     * Default constructor with root path for replacement path
     */
    public StandardResourceUriResolver() {
        this(ROOT_PATH);
    }

    /**
     * Standard Resource URI Resolver with configurable replacement path
     *
     * @param replacementPath Replacement path used to replace REST API path found in Request URL
     */
    public StandardResourceUriResolver(final String replacementPath) {
        this.replacementPath = Objects.requireNonNull(replacementPath, "Replacement path required");
    }

    /**
     * Get Resource URI
     *
     * @param request HTTP Servlet Request
     * @return Resource URI
     */
    @Override
    public URI getResourceUri(final HttpServletRequest request) {
        Objects.requireNonNull(request, "Request required");
        final StringBuffer requestUrl = request.getRequestURL();
        final Matcher requestUrlMatcher = REST_API_PATTERN.matcher(requestUrl);

        if (requestUrlMatcher.find()) {
            final String rootUri = requestUrlMatcher.replaceFirst(replacementPath);
            return URI.create(rootUri);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported Request URL [%s]", requestUrl));
        }
    }
}
