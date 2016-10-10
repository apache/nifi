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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class ProxiedEntitiesUtils {

    public static final String PROXY_ENTITIES_CHAIN = "X-ProxiedEntitiesChain";
    public static final String PROXY_ENTITIES_ACCEPTED = "X-ProxiedEntitiesAccepted";
    public static final String PROXY_ENTITIES_DETAILS = "X-ProxiedEntitiesDetails";

    private static final Pattern proxyChainPattern = Pattern.compile("<(.*?)>");

    /**
     * Formats the specified DN to be set as a HTTP header using well known conventions.
     *
     * @param dn raw dn
     * @return the dn formatted as an HTTP header
     */
    public static String formatProxyDn(String dn) {
        return "<" + dn + ">";
    }

    /**
     * Tokenizes the specified proxy chain.
     *
     * @param rawProxyChain raw chain
     * @return tokenized proxy chain
     */
    public static List<String> tokenizeProxiedEntitiesChain(String rawProxyChain) {
        final List<String> proxyChain = new ArrayList<>();
        final Matcher rawProxyChainMatcher = proxyChainPattern.matcher(rawProxyChain);
        while (rawProxyChainMatcher.find()) {
            proxyChain.add(rawProxyChainMatcher.group(1));
        }

        return proxyChain;
    }

    /**
     * Builds the proxy chain for the specified user.
     *
     * @param user The current user
     * @return The proxy chain for that user in String form
     */
    public static String buildProxiedEntitiesChainString(final NiFiUser user) {
        // calculate the dn chain
        final List<String> proxyChain = NiFiUserUtils.buildProxiedEntitiesChain(user);
        return formatProxyDn(StringUtils.join(proxyChain, "><"));
    }

    /**
     * Builds the proxy chain from the specified request and user.
     *
     * @param request the request
     * @param username the username
     * @return the proxy chain in list form
     */
    public static List<String> buildProxiedEntitiesChain(final HttpServletRequest request, final String username) {
        final String chain = buildProxiedEntitiesChainString(request, username);
        return tokenizeProxiedEntitiesChain(chain);
    }

    /**
     * Builds the dn chain from the specified request and user.
     *
     * @param request the request
     * @param username the username
     * @return the dn chain in string form
     */
    public static String buildProxiedEntitiesChainString(final HttpServletRequest request, final String username) {
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
