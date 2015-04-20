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

import java.security.cert.X509Certificate;
import javax.servlet.http.HttpServletRequest;

/**
 * An implementation of the ConfigurationRequestContext that retrieves configuration
 * from a HttpServletRequest instance.
 */
public class HttpServletRequestContext implements NiFiWebRequestContext {

    private static final String ID_PARAM = "id";

    private final UiExtensionType extensionType;
    private final HttpServletRequest request;

    public HttpServletRequestContext(final UiExtensionType extensionType, final HttpServletRequest request) {
        this.extensionType = extensionType;
        this.request = request;
    }

    @Override
    public UiExtensionType getExtensionType() {
        return extensionType;
    }

    @Override
    public String getProxiedEntitiesChain() {
        String xProxiedEntitiesChain = request.getHeader("X-ProxiedEntitiesChain");
        final X509Certificate cert = extractClientCertificate(request);
        if (cert != null) {
            final String extractedPrincipal = extractPrincipal(cert);
            final String formattedPrincipal = formatProxyDn(extractedPrincipal);
            if (xProxiedEntitiesChain == null || xProxiedEntitiesChain.trim().isEmpty()) {
                xProxiedEntitiesChain = formattedPrincipal;
            } else {
                xProxiedEntitiesChain += formattedPrincipal;
            }
        }

        return xProxiedEntitiesChain;
    }

    /**
     * @return the protocol scheme of the HttpServletRequest instance.
     */
    @Override
    public String getScheme() {
        return request.getScheme();
    }

    /**
     * @return the ID retrieved from the request parameter with key
     * equal to "id".
     */
    @Override
    public String getId() {
        return request.getParameter(ID_PARAM);
    }

    /**
     * Utility methods that have been copied into this class to reduce the
     * dependency footprint of this artifact. These utility methods typically
     * live in web-utilities but that would pull in spring, jersey, jackson,
     * etc.
     */
    private X509Certificate extractClientCertificate(HttpServletRequest request) {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");

        if (certs != null && certs.length > 0) {
            return certs[0];
        }

        return null;
    }

    private String extractPrincipal(X509Certificate cert) {
        return cert.getSubjectDN().getName().trim();
    }

    private String formatProxyDn(String dn) {
        return "<" + dn + ">";
    }
}
