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
package org.apache.nifi.web.server.handler;

import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.rewrite.handler.RedirectPatternRule;
import org.eclipse.jetty.rewrite.handler.Rule;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Context Path extension of Redirect Pattern Rule supporting context paths provided in request headers
 */
public class ContextPathRedirectPatternRule extends RedirectPatternRule {

    private static final String EMPTY_PATH = "";

    private static final String ROOT_PATH = "/";

    private final List<String> allowedContextPaths;

    /**
     * Context Path Redirect Pattern Rule with supported context paths
     *
     * @param pattern Path pattern to be matched
     * @param location Location for redirect URI
     * @param allowedContextPaths Context Path values allowed in request headers
     */
    public ContextPathRedirectPatternRule(final String pattern, final String location, final List<String> allowedContextPaths) {
        super(pattern, location);
        this.allowedContextPaths = Objects.requireNonNull(allowedContextPaths, "Allowed Context Paths required");
    }

    @Override
    public Rule.Handler apply(Rule.Handler input) throws IOException {
        return new Rule.Handler(input) {
            protected boolean handle(Response response, Callback callback) {
                final String redirectUri = getRedirectUri(input);
                response.setStatus(ContextPathRedirectPatternRule.this.getStatusCode());
                response.getHeaders().put(HttpHeader.LOCATION, redirectUri);
                callback.succeeded();
                return true;
            }
        };
    }

    private String getRedirectUri(final Rule.Handler inputHandler) {
        final HttpFields requestHeaders = inputHandler.getHeaders();
        final String contextPath = getContextPath(requestHeaders);
        final String location = getLocation();
        final String contextPathLocation = contextPath + location;
        return Response.toRedirectURI(inputHandler, contextPathLocation);
    }

    private String getContextPath(final HttpFields requestHeaders) {
        final String path;

        final String headerPath = getFirstHeader(requestHeaders, ProxyHeader.PROXY_CONTEXT_PATH, ProxyHeader.FORWARDED_CONTEXT, ProxyHeader.FORWARDED_PREFIX);
        if (headerPath == null) {
            path = EMPTY_PATH;
        } else if (ROOT_PATH.equals(headerPath)) {
            path = ROOT_PATH;
        } else {
            if (allowedContextPaths.contains(headerPath)) {
                path = headerPath;
            } else {
                throw new IllegalArgumentException("Request Header Context Path not allowed based on properties [nifi.web.proxy.context.path]");
            }
        }

        return path;
    }

    private String getFirstHeader(final HttpFields requestFields, final ProxyHeader... proxyHeaders) {
        return Arrays.stream(proxyHeaders)
                .map(ProxyHeader::getHeader)
                .map(requestFields::get)
                .filter(Objects::nonNull)
                .filter(Predicate.not(String::isBlank))
                .findFirst()
                .orElse(null);
    }
}
