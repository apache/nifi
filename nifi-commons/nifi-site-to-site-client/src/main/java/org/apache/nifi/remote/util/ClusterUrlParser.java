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
package org.apache.nifi.remote.util;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Site-To-Site Cluster URL Parser
 */
public class ClusterUrlParser {
    /**
     * Parse the comma-separated URLs string for the remote NiFi instances.
     *
     * @return A set containing one or more URLs
     * @throws IllegalArgumentException when it fails to parse the URLs string,
     * URLs string contains multiple protocols (http and https mix),
     * or none of URL is specified.
     */
    public static Set<String> parseClusterUrls(final String clusterUrls) {
        final Set<String> urls = new LinkedHashSet<>();
        if (clusterUrls != null && !clusterUrls.isEmpty()) {
            Arrays.stream(clusterUrls.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .forEach(s -> {
                        validateUriString(s);
                        urls.add(resolveBaseUrl(s).intern());
                    });
        }

        if (urls.isEmpty()) {
            throw new IllegalArgumentException("Cluster URL was not specified.");
        }

        final Predicate<String> isHttps = url -> url.toLowerCase().startsWith("https:");
        if (urls.stream().anyMatch(isHttps) && urls.stream().anyMatch(isHttps.negate())) {
            throw new IllegalArgumentException("Different protocols are used in the cluster URLs " + clusterUrls);
        }

        return Collections.unmodifiableSet(urls);
    }

    static String resolveBaseUrl(final String clusterUrl) {
        Objects.requireNonNull(clusterUrl, "clusterUrl cannot be null.");
        final URI uri;
        try {
            uri = new URI(clusterUrl.trim());
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("The specified URL is malformed: " + clusterUrl);
        }

        return resolveBaseUrl(uri);
    }

    private static void validateUriString(String s) {
        // parse the uri
        final URI uri;
        try {
            uri = URI.create(s);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("The specified remote process group URL is malformed: " + s);
        }

        // validate each part of the uri
        if (uri.getScheme() == null || uri.getHost() == null) {
            throw new IllegalArgumentException("The specified remote process group URL is malformed: " + s);
        }

        if (!(uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException("The specified remote process group URL is invalid because it is not http or https: " + s);
        }
    }

    /**
     * Resolve NiFi API url with leniency. This method does following conversion on uri path:
     * <ul>
     * <li>/ to /nifi-api</li>
     * <li>/nifi to /nifi-api</li>
     * <li>/some/path/ to /some/path/nifi-api</li>
     * </ul>
     * @param clusterUrl url to be resolved
     * @return resolved url
     */
    private static String resolveBaseUrl(final URI clusterUrl) {

        if (clusterUrl.getScheme() == null || clusterUrl.getHost() == null) {
            throw new IllegalArgumentException("The specified URL is malformed: " + clusterUrl);
        }

        if (!(clusterUrl.getScheme().equalsIgnoreCase("http") || clusterUrl.getScheme().equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException("The specified URL is invalid because it is not http or https: " + clusterUrl);
        }


        String uriPath = clusterUrl.getPath().trim();

        if (StringUtils.isEmpty(uriPath) || uriPath.equals("/")) {
            uriPath = "/nifi";
        } else if (uriPath.endsWith("/")) {
            uriPath = uriPath.substring(0, uriPath.length() - 1);
        }

        final StringBuilder uriPathBuilder = new StringBuilder(uriPath);
        if (uriPath.endsWith("/nifi")) {
            uriPathBuilder.append("-api");
        } else if (!uriPath.endsWith("/nifi-api")) {
            uriPathBuilder.append("/nifi-api");
        }

        try {
            final URI uri = new URI(clusterUrl.getScheme(), null, clusterUrl.getHost(), clusterUrl.getPort(), uriPathBuilder.toString(), null, null);
            return uri.toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
