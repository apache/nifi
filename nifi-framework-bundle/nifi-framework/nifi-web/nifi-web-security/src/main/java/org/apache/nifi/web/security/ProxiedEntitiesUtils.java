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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.security.proxied.entity.ProxiedEntityEncoder;
import org.apache.nifi.security.proxied.entity.StandardProxiedEntityEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.AuthenticationException;

public class ProxiedEntitiesUtils {
    private static final Logger logger = LoggerFactory.getLogger(ProxiedEntitiesUtils.class);

    public static final String PROXY_ENTITIES_CHAIN = "X-ProxiedEntitiesChain";
    public static final String PROXY_ENTITIES_ACCEPTED = "X-ProxiedEntitiesAccepted";
    public static final String PROXY_ENTITIES_DETAILS = "X-ProxiedEntitiesDetails";

    public static final String PROXY_ENTITY_GROUPS = "X-ProxiedEntityGroups";
    public static final String PROXY_ENTITY_GROUPS_EMPTY = "<>";

    private static final String GT = ">";
    private static final String ESCAPED_GT = "\\\\>";
    private static final String LT = "<";
    private static final String ESCAPED_LT = "\\\\<";

    private static final String ANONYMOUS_CHAIN = "<>";
    private static final String ANONYMOUS_IDENTITY = "";

    private static final ProxiedEntityEncoder proxiedEntityEncoder = StandardProxiedEntityEncoder.getInstance();

    /**
     * Tokenizes the specified proxy chain.
     *
     * @param rawProxyChain raw chain
     * @return tokenized proxy chain
     */
    public static List<String> tokenizeProxiedEntitiesChain(final String rawProxyChain) {
        final List<String> proxyChain = new ArrayList<>();
        if (!StringUtils.isEmpty(rawProxyChain)) {

            if (!isValidChainFormat(rawProxyChain)) {
                throw new IllegalArgumentException("Proxy chain format is not recognized and can not safely be converted to a list.");
            }

            if (rawProxyChain.equals(ANONYMOUS_CHAIN)) {
                proxyChain.add(ANONYMOUS_IDENTITY);
            } else {
                // Split the String on the `><` token, use substring to remove leading `<` and trailing `>`
                final String[] elements = StringUtils.splitByWholeSeparatorPreserveAllTokens(
                        rawProxyChain.substring(1, rawProxyChain.length() - 1), "><");
                // Unsanitize each DN and add it to the proxy chain list
                Arrays.stream(elements)
                        .map(ProxiedEntitiesUtils::unsanitizeDn)
                        .forEach(proxyChain::add);
            }
        }
        return proxyChain;
    }

    /**
     * Tokenizes the specified proxied entity groups which are formatted the same as a proxy chain.
     *
     * @param rawProxyEntityGroups the raw proxy entity groups
     * @return the set of group names, or empty set if none exist
     */
    public static Set<String> tokenizeProxiedEntityGroups(String rawProxyEntityGroups) {
        final List<String> elements = tokenizeProxiedEntitiesChain(rawProxyEntityGroups);
        if (elements.isEmpty()) {
            return Collections.emptySet();
        } else {
            return elements.stream().filter(e -> !StringUtils.isBlank(e)).collect(Collectors.toSet());
        }
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
        proxyChain = proxyChain.stream().map(proxiedEntityEncoder::getEncodedEntity).collect(Collectors.toList());
        return StringUtils.join(proxyChain, "");
    }

    /**
     * Builds the string representation for a set of groups that belong to a proxied entity.
     * The resulting string will be formatted similar to a proxied-entity chain.
     * Example:
     *   Groups set:    ("group1", "group2", "group3")
     *   Returns:       {@code "<group1><group2><group3> }
     *
     * @param groups the set of groups
     * @return the formatted group string, or null if there are no groups to proxy
     */
    public static String buildProxiedEntityGroupsString(final Set<String> groups) {
        if (groups == null || groups.isEmpty()) {
            return PROXY_ENTITY_GROUPS_EMPTY;
        }

        final List<String> formattedGroups = groups.stream().map(proxiedEntityEncoder::getEncodedEntity).collect(Collectors.toList());
        return StringUtils.join(formattedGroups, "");
    }

    /**
     * If a successfully authenticated request was made via a proxy, relevant proxy headers will be added to the response.
     *
     * @param request The proxied client request that was successfully authenticated.
     * @param response A servlet response to the client containing the successful authentication details.
     */
    public static void successfulAuthentication(final HttpServletRequest request, final HttpServletResponse response) {
        if (StringUtils.isNotBlank(request.getHeader(PROXY_ENTITIES_CHAIN))) {
            response.setHeader(PROXY_ENTITIES_ACCEPTED, Boolean.TRUE.toString());
        }
    }

    /**
     * If an unauthenticated request was made via a proxy, add proxy headers to explain why authentication failed.
     *
     * @param request The original client request that failed to be authenticated.
     * @param response Servlet response to the client containing the unsuccessful authentication attempt details.
     * @param failed The related exception thrown and explanation for the unsuccessful authentication attempt.
     */
    public static void unsuccessfulAuthentication(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final AuthenticationException failed) {
        if (StringUtils.isNotBlank(request.getHeader(PROXY_ENTITIES_CHAIN))) {
            response.setHeader(PROXY_ENTITIES_DETAILS, failed.getMessage());
        }
    }

    /**
     * Reconstitutes the original DN from the sanitized version passed in the proxy chain.
     * <p>
     * Example:
     * <p>
     * {@code alopresto\>\<proxy1} -> {@code alopresto><proxy1}
     * <p>
     * {@code <0JDQu9C50YE=>} -> {@code Алйс}
     *
     * @param sanitizedDn the sanitized DN
     * @return the original DN
     */
    private static String unsanitizeDn(final String sanitizedDn) {
        if (StringUtils.isEmpty(sanitizedDn)) {
            return sanitizedDn;
        } else {
            final String decodedDn;
            if (isBase64Encoded(sanitizedDn)) {
                decodedDn = base64Decode(sanitizedDn);
                logger.debug("The provided DN [{}] had been encoded, and was reconstituted to the original DN [{}]", sanitizedDn, decodedDn);
            } else {
                decodedDn = sanitizedDn;
            }
            final String unsanitizedDn = decodedDn.replaceAll(ESCAPED_GT, GT).replaceAll(ESCAPED_LT, LT);
            if (!unsanitizedDn.equals(decodedDn)) {
                logger.warn("The provided DN [{}] had been escaped, and was reconstituted to the dangerous DN [{}]", sanitizedDn, unsanitizedDn);
            }
            return unsanitizedDn;
        }
    }

    /**
     * Performs the reverse of Base64 encoding
     *
     * @param encodedValue the encoded value to decode.
     * @return The original, decoded string.
     */
    private static String base64Decode(final String encodedValue) {
        final String base64String = encodedValue.substring(1, encodedValue.length() - 1);
        return new String(Base64.getDecoder().decode(base64String), StandardCharsets.UTF_8);
    }

    /**
     * Check if a String is in the expected format and can be safely tokenized.
     *
     * @param rawProxiedEntitiesChain the value to check
     * @return true if the value is in the valid format to tokenize, false otherwise.
     */
    private static boolean isValidChainFormat(final String rawProxiedEntitiesChain) {
        return isWrappedInAngleBrackets(rawProxiedEntitiesChain);
    }

    /**
     * Check if a value has been encoded by Base64 encoding and therefore needs to be decoded.
     *
     * @param token the value to check
     * @return true if the value is encoded, false otherwise.
     */
    private static boolean isBase64Encoded(final String token) {
        return isWrappedInAngleBrackets(token);
    }

    /**
     * Check if a string is wrapped with  &lt;angle brackets&gt;.
     *
     * @param string the value to check
     * @return true if the value starts with &lt; and ends with &gt; - false otherwise
     */
    private static boolean isWrappedInAngleBrackets(final String string) {
        return string.startsWith(LT) && string.endsWith(GT);
    }
}
