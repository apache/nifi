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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.web.security.x509.SubjectDnX509PrincipalExtractor;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.user.NiFiUser;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

/**
 *
 */
public class ProxiedEntitiesUtils {

    public static final String PROXY_ENTITIES_CHAIN = "X-ProxiedEntitiesChain";
    public static final String PROXY_ENTITIES_ACCEPTED = "X-ProxiedEntitiesAccepted";
    public static final String PROXY_ENTITIES_DETAILS = "X-ProxiedEntitiesDetails";

    private static final Pattern proxyChainPattern = Pattern.compile("<(.*?)>");

    /**
     * @param request http request
     * @return the X-ProxiedEntitiesChain from the specified request
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
     * Builds the dn chain for the specified user.
     *
     * @param user The current user
     * @return The dn chain for that user
     */
    public static List<String> getXProxiedEntitiesChain(final NiFiUser user) {
        // calculate the dn chain
        final List<String> dnChain = new ArrayList<>();

        // build the dn chain
        NiFiUser chainedUser = user;
        do {
            // add the entry for this user
            dnChain.add(chainedUser.getDn());

            // go to the next user in the chain
            chainedUser = chainedUser.getChain();
        } while (chainedUser != null);

        return dnChain;
    }

    /**
     * Formats the specified DN to be set as a HTTP header using well known conventions.
     *
     * @param dn raw dn
     * @return the dn formatted as an HTTP header
     */
    public static String formatProxyDn(String dn) {
        return "<" + dn + ">";
    }

//    /**
//     * Tokenizes the specified proxy chain.
//     *
//     * @param rawProxyChain raw chain
//     * @return tokenized proxy chain
//     */
//    public static Deque<String> tokenizeProxyChain(String rawProxyChain) {
//        final Deque<String> dnList = new ArrayDeque<>();
//
//        // parse the proxy chain
//        final Matcher rawProxyChainMatcher = proxyChainPattern.matcher(rawProxyChain);
//        while (rawProxyChainMatcher.find()) {
//            dnList.push(rawProxyChainMatcher.group(1));
//        }
//
//        return dnList;
//    }
    public static List<String> buildProxyChain(final HttpServletRequest request, final String username) {
        String principal;
        if (username.startsWith("<") && username.endsWith(">")) {
            principal = username;
        } else {
            principal = formatProxyDn(username);
        }

        // look for a proxied user
        if (StringUtils.isNotBlank(request.getHeader(PROXY_ENTITIES_CHAIN))) {
            principal = request.getHeader(PROXY_ENTITIES_CHAIN) + principal;
        }

        // parse the proxy chain
        final List<String> proxyChain = new ArrayList<>();
        final Matcher rawProxyChainMatcher = proxyChainPattern.matcher(principal);
        while (rawProxyChainMatcher.find()) {
            proxyChain.add(rawProxyChainMatcher.group(1));
        }

        return proxyChain;
    }

    public static String extractProxiedEntitiesChain(final HttpServletRequest request, final String username) {
        String principal;
        if (username.startsWith("<") && username.endsWith(">")) {
            principal = username;
        } else {
            principal = formatProxyDn(username);
        }

        // look for a proxied user
        if (StringUtils.isNotBlank(request.getHeader(PROXY_ENTITIES_CHAIN))) {
            principal = request.getHeader(PROXY_ENTITIES_CHAIN) + principal;
        }
        return principal;
    }

    public static void successfulAuthorization(HttpServletRequest request, HttpServletResponse response, Authentication authResult) {
        if (StringUtils.isNotBlank(request.getHeader(PROXY_ENTITIES_CHAIN))) {
            response.setHeader(PROXY_ENTITIES_ACCEPTED, Boolean.TRUE.toString());
        }
    }

    public static void unsuccessfulAuthorization(HttpServletRequest request, HttpServletResponse response, AuthenticationException failed) {
        if (StringUtils.isNotBlank(request.getHeader(PROXY_ENTITIES_CHAIN))) {
            response.setHeader(PROXY_ENTITIES_DETAILS, failed.getMessage());
        }
    }
}
