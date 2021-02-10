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
package org.apache.nifi.web.security.saml.impl.http;

import org.apache.nifi.web.util.WebUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 * Extension of HttpServletRequestWrapper that respects proxied/forwarded header values for scheme, host, port, and context path.
 * <p>
 * If NiFi generates a SAML request using proxied values so that the IDP redirects back through the proxy, then this is needed
 * so that when Open SAML checks the Destination in the SAML response, it will match with the values here.
 * <p>
 * This class is based on SAMLContextProviderLB from spring-security-saml.
 */
public class ProxyAwareHttpServletRequestWrapper extends HttpServletRequestWrapper {

    private final String scheme;
    private final String serverName;
    private final int serverPort;
    private final String proxyContextPath;
    private final String contextPath;

    public ProxyAwareHttpServletRequestWrapper(final HttpServletRequest request) {
        super(request);
        this.scheme = WebUtils.determineProxiedScheme(request);
        this.serverName = WebUtils.determineProxiedHost(request);
        this.serverPort = Integer.valueOf(WebUtils.determineProxiedPort(request));

        final String tempProxyContextPath = WebUtils.normalizeContextPath(WebUtils.determineContextPath(request));
        this.proxyContextPath = tempProxyContextPath.equals("/") ? "" : tempProxyContextPath;

        this.contextPath = request.getContextPath();
    }

    @Override
    public String getContextPath() {
        return contextPath;
    }

    @Override
    public String getScheme() {
        return scheme;
    }

    @Override
    public String getServerName() {
        return serverName;
    }

    @Override
    public int getServerPort() {
        return serverPort;
    }

    @Override
    public String getRequestURI() {
        StringBuilder sb = new StringBuilder(contextPath);
        sb.append(getServletPath());
        return sb.toString();
    }

    @Override
    public StringBuffer getRequestURL() {
        StringBuffer sb = new StringBuffer();
        sb.append(scheme).append("://").append(serverName);
        sb.append(":").append(serverPort);
        sb.append(proxyContextPath);
        sb.append(contextPath);
        sb.append(getServletPath());
        if (getPathInfo() != null) sb.append(getPathInfo());
        return sb;
    }

    @Override
    public boolean isSecure() {
        return "https".equalsIgnoreCase(scheme);
    }

}
