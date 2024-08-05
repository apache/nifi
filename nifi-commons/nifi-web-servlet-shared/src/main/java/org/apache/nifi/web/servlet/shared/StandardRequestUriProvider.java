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
package org.apache.nifi.web.servlet.shared;

import jakarta.servlet.http.HttpServletRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard implementation of Request URI Provider with allowed hosts and context paths
 */
public class StandardRequestUriProvider implements RequestUriProvider {
    private static final Pattern HOST_PATTERN = Pattern.compile("^([^:]+):?([1-9][0-9]{2,4})?$");

    private static final Pattern HOST_PORT_REQUIRED_PATTERN = Pattern.compile("^[^:]+:([1-9][0-9]{2,4})$");

    private static final Pattern SCHEME_PATTERN = Pattern.compile("^https?$");

    private static final int FIRST_GROUP = 1;

    private static final int MINIMUM_PORT_NUMBER = 100;

    private static final int MAXIMUM_PORT_NUMBER = 65535;

    private static final String EMPTY_PATH = "";

    private static final String ROOT_PATH = "/";

    private final List<String> allowedContextPaths;

    public StandardRequestUriProvider(final List<String> allowedContextPaths) {
        this.allowedContextPaths = Objects.requireNonNull(allowedContextPaths);
    }

    /**
     * Get Request URI from HTTP Servlet Request containing optional headers and validated against allowed context paths
     *
     * @param request HTTP Servlet Request
     * @return Request URI
     */
    @Override
    public URI getRequestUri(final HttpServletRequest request) {
        Objects.requireNonNull(request, "HTTP Servlet Request required");

        final String scheme = getScheme(request);
        final String host = getHost(request);
        final int port = getPort(request);
        final String path = getPath(request);

        try {
            return new URI(scheme, null, host, port, path, null, null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Request URI construction failed", e);
        }
    }

    private String getScheme(final HttpServletRequest request) {
        final String scheme;

        final String requestScheme = request.getScheme();
        final String headerScheme = getFirstHeader(request, ProxyHeader.PROXY_SCHEME, ProxyHeader.FORWARDED_PROTO);
        if (headerScheme == null) {
            scheme = requestScheme;
        } else {
            final Matcher matcher = SCHEME_PATTERN.matcher(headerScheme);
            if (matcher.matches()) {
                scheme = headerScheme;
            } else {
                scheme = requestScheme;
            }
        }

        return scheme;
    }

    private String getHost(final HttpServletRequest request) {
        final String host;

        final String serverName = request.getServerName();
        final String headerHost = getFirstHeader(request, ProxyHeader.PROXY_HOST, ProxyHeader.FORWARDED_HOST, ProxyHeader.HOST);
        if (headerHost == null) {
            host = serverName;
        } else {
            final Matcher matcher = HOST_PATTERN.matcher(headerHost);
            if (matcher.matches()) {
                host = matcher.group(FIRST_GROUP);
            } else {
                host = serverName;
            }
        }

        return host;
    }

    private int getPort(final HttpServletRequest request) {
        final int port;

        final int serverPort = request.getServerPort();
        final String headerHost = getFirstHeader(request, ProxyHeader.PROXY_HOST, ProxyHeader.FORWARDED_HOST);
        if (headerHost == null) {
            port = getProxyPort(request);
        } else {
            final Matcher matcher = HOST_PORT_REQUIRED_PATTERN.matcher(headerHost);
            if (matcher.matches()) {
                final String headerPort = matcher.group(FIRST_GROUP);
                port = getParsedPort(headerPort, serverPort);
            } else {
                port = getProxyPort(request);
            }
        }

        return port;
    }

    private int getProxyPort(final HttpServletRequest request) {
        final int port;

        final int serverPort = request.getServerPort();
        final String headerPort = getFirstHeader(request, ProxyHeader.PROXY_PORT, ProxyHeader.FORWARDED_PORT);
        if (headerPort == null) {
            port = serverPort;
        } else {
            port = getParsedPort(headerPort, serverPort);
        }

        return port;
    }

    private int getParsedPort(final String headerPort, final int serverPort) {
        int port;

        try {
            final int parsedPort = Integer.parseInt(headerPort);
            if (parsedPort < MINIMUM_PORT_NUMBER || parsedPort > MAXIMUM_PORT_NUMBER) {
                port = serverPort;
            } else {
                port = parsedPort;
            }
        } catch (final Exception e) {
            port = serverPort;
        }

        return port;
    }

    private String getPath(final HttpServletRequest request) {
        final String path;

        final String headerPath = getFirstHeader(request, ProxyHeader.PROXY_CONTEXT_PATH, ProxyHeader.FORWARDED_CONTEXT, ProxyHeader.FORWARDED_PREFIX);
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

    private String getFirstHeader(final HttpServletRequest request, final ProxyHeader... proxyHeaders) {
        return Arrays.stream(proxyHeaders)
                .map(ProxyHeader::getHeader)
                .map(request::getHeader)
                .filter(Objects::nonNull)
                .filter(Predicate.not(String::isBlank))
                .findFirst()
                .orElse(null);
    }
}
