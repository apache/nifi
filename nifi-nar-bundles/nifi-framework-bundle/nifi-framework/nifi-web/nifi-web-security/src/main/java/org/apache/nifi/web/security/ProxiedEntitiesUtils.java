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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
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

    /**
     * Formats a list of DN/usernames to be set as a HTTP header using well known conventions.
     *
     * @param proxiedEntities the raw identities (usernames and DNs) to be formatted as a chain
     * @return the value to use in the X-ProxiedEntitiesChain header
     */
    public static String getProxiedEntitiesChain(final String... proxiedEntities) {
        return getProxiedEntitiesChain(Arrays.asList(proxiedEntities));
    }

    /**
     * Formats a list of DN/usernames to be set as a HTTP header using well known conventions.
     *
     * @param proxiedEntities the raw identities (usernames and DNs) to be formatted as a chain
     * @return the value to use in the X-ProxiedEntitiesChain header
     */
    public static String getProxiedEntitiesChain(final List<String> proxiedEntities) {
        if (proxiedEntities == null) {
            return null;
        }

        final List<String> proxiedEntityChain = proxiedEntities.stream()
                .map(org.apache.nifi.registry.security.util.ProxiedEntitiesUtils::formatProxyDn)
                .collect(Collectors.toList());
        return StringUtils.join(proxiedEntityChain, "");
    }

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
        proxyChain = proxyChain.stream().map(ProxiedEntitiesUtils::formatProxyDn).collect(Collectors.toList());
        return StringUtils.join(proxyChain, "");
    }

    /**
     * Builds the string representation for a set of groups that belong to a proxied entity.
     *
     * The resulting string will be formatted similar to a proxied-entity chain.
     *
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

        final List<String> formattedGroups = groups.stream().map(ProxiedEntitiesUtils::formatProxyDn).collect(Collectors.toList());
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
     * Formats the specified DN to be set as a HTTP header using well known conventions.
     *
     * @param dn raw dn
     * @return the dn formatted as an HTTP header
     */
    public static String formatProxyDn(final String dn) {
        return LT + sanitizeDn(dn) + GT;
    }

    /**
     * Sanitizes a DN for safe and lossless transmission.
     *
     * Sanitization requires:
     * <ol>
     *   <li>Encoded so that it can be sent losslessly using US-ASCII (the character set of HTTP Header values)</li>
     *   <li>Resilient to a DN with the sequence '><' to attempt to escape the tokenization process and impersonate another user.</li>
     * </ol>
     *
     * <p>
     * Example:
     * <p>
     * Provided DN: {@code jdoe><alopresto} -> {@code <jdoe><alopresto><proxy...>} would allow the user to impersonate jdoe
     * <p>Алйс
     * Provided DN: {@code Алйс} -> {@code <Алйс>} cannot be encoded/decoded as ASCII
     *
     * @param rawDn the unsanitized DN
     * @return the sanitized DN
     */
    private static String sanitizeDn(final String rawDn) {
        if (StringUtils.isEmpty(rawDn)) {
            return rawDn;
        } else {

            // First, escape any GT [>] or LT [<] characters, which are not safe
            final String escapedDn = rawDn.replaceAll(GT, ESCAPED_GT).replaceAll(LT, ESCAPED_LT);
            if (!escapedDn.equals(rawDn)) {
                logger.warn("The provided DN [" + rawDn + "] contained dangerous characters that were escaped to [" + escapedDn + "]");
            }

            // Second, check for characters outside US-ASCII.
            // This is necessary because X509 Certs can contain international/Unicode characters,
            // but this value will be passed in an HTTP Header which must be US-ASCII.
            // If non-ascii characters are present, base64 encode the DN and wrap in <angled-brackets>,
            // to indicate to the receiving end that the value must be decoded.
            // Note: We could have decided to always base64 encode these values,
            //       not only to avoid the isPureAscii(...) check, but also as a
            //       method of sanitizing  GT [>] or LT [<] chars. However, there
            //       are advantages to encoding only when necessary, namely:
            //         1. Backwards compatibility
            //         2. Debugging this X-ProxiedEntitiesChain headers is easier unencoded.
            //       This algorithm can be revisited as part of the next major version change.
            if (isPureAscii(escapedDn)) {
                return escapedDn;
            } else {
                final String encodedDn = base64Encode(escapedDn);
                logger.debug("The provided DN [" + rawDn + "] contained non-ASCII characters and was encoded as [" + encodedDn + "]");
                return encodedDn;
            }
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
                logger.debug("The provided DN [" + sanitizedDn + "] had been encoded, and was reconstituted to the original DN [" + decodedDn + "]");
            } else {
                decodedDn = sanitizedDn;
            }
            final String unsanitizedDn = decodedDn.replaceAll(ESCAPED_GT, GT).replaceAll(ESCAPED_LT, LT);
            if (!unsanitizedDn.equals(decodedDn)) {
                logger.warn("The provided DN [" + sanitizedDn + "] had been escaped, and was reconstituted to the dangerous DN [" + unsanitizedDn + "]");
            }
            return unsanitizedDn;
        }
    }

    /**
     * Base64 encodes a DN and wraps it in angled brackets to indicate the value is base64 and not a raw DN.
     *
     * @param rawValue The value to encode
     * @return A string containing a wrapped, encoded value.
     */
    private static String base64Encode(final String rawValue) {
        final String base64String = Base64.getEncoder().encodeToString(rawValue.getBytes(StandardCharsets.UTF_8));
        final String wrappedEncodedValue = LT + base64String + GT;
        return wrappedEncodedValue;
    }

    /**
     * Performs the reverse of ${@link #base64Encode(String)}.
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
     * Check if a value has been encoded by ${@link #base64Encode(String)}, and therefore needs to be decoded.
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

    /**
     * Check if a string contains only pure ascii characters.
     *
     * @param stringWithUnknownCharacters - the string to check
     * @return true if string can be encoded as ascii. false otherwise.
     */
    private static boolean isPureAscii(final String stringWithUnknownCharacters) {
        return StandardCharsets.US_ASCII.newEncoder().canEncode(stringWithUnknownCharacters);
    }
}
