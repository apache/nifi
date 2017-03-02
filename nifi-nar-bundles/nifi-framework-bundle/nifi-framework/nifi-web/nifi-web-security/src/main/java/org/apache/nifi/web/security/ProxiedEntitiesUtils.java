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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

/**
 *
 */
public class ProxiedEntitiesUtils {
    private static final Logger logger = LoggerFactory.getLogger(ProxiedEntitiesUtils.class);

    public static final String PROXY_ENTITIES_CHAIN = "X-ProxiedEntitiesChain";
    public static final String PROXY_ENTITIES_ACCEPTED = "X-ProxiedEntitiesAccepted";
    public static final String PROXY_ENTITIES_DETAILS = "X-ProxiedEntitiesDetails";

    private static final String GT = ">";
    private static final String ESCAPED_GT = "\\\\>";
    private static final String LT = "<";
    private static final String ESCAPED_LT = "\\\\<";

    private static final String ANONYMOUS_CHAIN = "<>";

    /**
     * Formats the specified DN to be set as a HTTP header using well known conventions.
     *
     * @param dn raw dn
     * @return the dn formatted as an HTTP header
     */
    public static String formatProxyDn(String dn) {
        return LT + sanitizeDn(dn) + GT;
    }

    /**
     * If a user provides a DN with the sequence '><', they could escape the tokenization process and impersonate another user.
     * <p>
     * Example:
     * <p>
     * Provided DN: {@code jdoe><alopresto} -> {@code <jdoe><alopresto><proxy...>} would allow the user to impersonate jdoe
     *
     * @param rawDn the unsanitized DN
     * @return the sanitized DN
     */
    private static String sanitizeDn(String rawDn) {
        if (StringUtils.isEmpty(rawDn)) {
            return rawDn;
        } else {
            String sanitizedDn = rawDn.replaceAll(GT, ESCAPED_GT).replaceAll(LT, ESCAPED_LT);
            if (!sanitizedDn.equals(rawDn)) {
                logger.warn("The provided DN [" + rawDn + "] contained dangerous characters that were escaped to [" + sanitizedDn + "]");
            }
            return sanitizedDn;
        }
    }

    /**
     * Reconstitutes the original DN from the sanitized version passed in the proxy chain.
     * <p>
     * Example:
     * <p>
     * {@code alopresto\>\<proxy1} -> {@code alopresto><proxy1}
     *
     * @param sanitizedDn the sanitized DN
     * @return the original DN
     */
    private static String unsanitizeDn(String sanitizedDn) {
        if (StringUtils.isEmpty(sanitizedDn)) {
            return sanitizedDn;
        } else {
            String unsanitizedDn = sanitizedDn.replaceAll(ESCAPED_GT, GT).replaceAll(ESCAPED_LT, LT);
            if (!unsanitizedDn.equals(sanitizedDn)) {
                logger.warn("The provided DN [" + sanitizedDn + "] had been escaped, and was reconstituted to the dangerous DN [" + unsanitizedDn + "]");
            }
            return unsanitizedDn;
        }
    }

    /**
     * Tokenizes the specified proxy chain.
     *
     * @param rawProxyChain raw chain
     * @return tokenized proxy chain
     */
    public static List<String> tokenizeProxiedEntitiesChain(String rawProxyChain) {
        final List<String> proxyChain = new ArrayList<>();
        if (!StringUtils.isEmpty(rawProxyChain)) {
            // Split the String on the >< token
            List<String> elements = Arrays.asList(StringUtils.splitByWholeSeparatorPreserveAllTokens(rawProxyChain, "><"));

            // Unsanitize each DN and collect back
            elements = elements.stream().map(ProxiedEntitiesUtils::unsanitizeDn).collect(Collectors.toList());

            // Remove the leading < from the first element
            elements.set(0, elements.get(0).replaceFirst(LT, ""));

            // Remove the trailing > from the last element
            int last = elements.size() - 1;
            String lastElement = elements.get(last);
            if (lastElement.endsWith(GT)) {
                elements.set(last, lastElement.substring(0, lastElement.length() - 1));
            }

            proxyChain.addAll(elements);
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
        List<String> proxyChain = NiFiUserUtils.buildProxiedEntitiesChain(user);
        if (proxyChain.isEmpty()) {
            return ANONYMOUS_CHAIN;
        }
        proxyChain = proxyChain.stream().map(ProxiedEntitiesUtils::formatProxyDn).collect(Collectors.toList());
        return StringUtils.join(proxyChain, "");
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
