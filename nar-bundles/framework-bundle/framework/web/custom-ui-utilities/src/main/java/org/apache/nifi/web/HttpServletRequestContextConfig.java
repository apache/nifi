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
 * An implementation of the NiFiWebContextConfig that retrieves configuration
 * from a HttpServletRequest instance.
 */
public class HttpServletRequestContextConfig implements NiFiWebContextConfig {

    public static final String PROCESSOR_ID_PARAM = "processorId";

    public static final String CLIENT_ID_PARAM = "clientId";

    public static final String REVISION_PARAM = "revision";

    private final HttpServletRequest request;

    public HttpServletRequestContextConfig(final HttpServletRequest request) {
        this.request = request;
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
     * @return the processor ID retrieved from the request parameter with key
     * equal to "processorId".
     */
    @Override
    public String getProcessorId() {
        return request.getParameter(PROCESSOR_ID_PARAM);
    }

    /**
     * @return the revision retrieved from the request parameters with keys
     * equal to "clientId" and "revision".
     */
    @Override
    public Revision getRevision() {

        final String revisionParamVal = request.getParameter(REVISION_PARAM);
        Long revision;
        try {
            revision = Long.parseLong(revisionParamVal);
        } catch (final Exception ex) {
            revision = null;
        }

        final String clientId = request.getParameter(CLIENT_ID_PARAM);

        return new Revision(revision, clientId);
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
