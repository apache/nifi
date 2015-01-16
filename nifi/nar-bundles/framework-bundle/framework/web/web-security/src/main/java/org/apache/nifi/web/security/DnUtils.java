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
package org.apache.nifi.web.security;

import java.security.cert.X509Certificate;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import org.apache.nifi.web.security.x509.SubjectDnX509PrincipalExtractor;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.apache.commons.lang3.StringUtils;

/**
 *
 */
public class DnUtils {

    private static final Pattern proxyChainPattern = Pattern.compile("<(.*?)>");

    /**
     * Gets the X-ProxiedEntitiesChain from the specified request.
     *
     * @param request
     * @return
     */
    public static String getXProxiedEntitiesChain(final HttpServletRequest request) {
        String xProxiedEntitiesChain = request.getHeader("X-ProxiedEntitiesChain");
        final X509Certificate cert = new X509CertificateExtractor().extractClientCertificate(request);
        if (cert != null) {
            final SubjectDnX509PrincipalExtractor principalExtractor = new SubjectDnX509PrincipalExtractor();
            final String extractedPrincipal = principalExtractor.extractPrincipal(cert).toString();
            final String formattedPrincipal = formatProxyDn(extractedPrincipal);
            if (StringUtils.isBlank(xProxiedEntitiesChain)) {
                xProxiedEntitiesChain = formattedPrincipal;
            } else {
                xProxiedEntitiesChain += formattedPrincipal;
            }
        }

        return xProxiedEntitiesChain;
    }

    /**
     * Formats the specified DN to be set as a HTTP header using well known
     * conventions.
     *
     * @param dn
     * @return
     */
    public static String formatProxyDn(String dn) {
        return "<" + dn + ">";
    }

    /**
     * Tokenizes the specified proxy chain.
     *
     * @param rawProxyChain
     * @return
     */
    public static Deque<String> tokenizeProxyChain(String rawProxyChain) {
        final Deque<String> dnList = new ArrayDeque<>();

        // parse the proxy chain
        final Matcher rawProxyChainMatcher = proxyChainPattern.matcher(rawProxyChain);
        while (rawProxyChainMatcher.find()) {
            dnList.push(rawProxyChainMatcher.group(1));
        }

        return dnList;
    }
}
