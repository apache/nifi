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
package org.apache.nifi.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Utility class providing java.net.URI utilities.
 * The regular expressions in this class used to capture the various components of a URI were adapted from
 * <a href="https://github.com/spring-projects/spring-framework/blob/main/spring-web/src/main/java/org/springframework/web/util/UriComponentsBuilder.java">UriComponentsBuilder</a>
 */
public class UriUtils {
    private static final String SCHEME_PATTERN = "([^:/?#]+):";
    private static final String USERINFO_PATTERN = "([^@\\[/?#]*)";
    private static final String HOST_IPV4_PATTERN = "[^\\[/?#:]*";
    private static final String HOST_IPV6_PATTERN = "\\[[\\p{XDigit}:.]*[%\\p{Alnum}]*]";
    private static final String HOST_PATTERN = "(" + HOST_IPV6_PATTERN + "|" + HOST_IPV4_PATTERN + ")";
    private static final String PORT_PATTERN = "(\\{[^}]+\\}?|[^/?#]*)";
    private static final String PATH_PATTERN = "([^?#]*)";
    private static final String QUERY_PATTERN = "([^#]*)";
    private static final String LAST_PATTERN = "(.*)";

    // Regex patterns that matches URIs. See RFC 3986, appendix B
    private static final Pattern URI_PATTERN = Pattern.compile(
            "^(" + SCHEME_PATTERN + ")?" + "(//(" + USERINFO_PATTERN + "@)?" + HOST_PATTERN + "(:" + PORT_PATTERN +
                    ")?" + ")?" + PATH_PATTERN + "(\\?" + QUERY_PATTERN + ")?" + "(#" + LAST_PATTERN + ")?");

    private UriUtils() {}

    /**
     * This method provides an alternative to the use of java.net.URI's single argument constructor and 'create' method.
     * The drawbacks of the java.net.URI's single argument constructor and 'create' method are:
     *   <ul>
     *      <li>They do not provide quoting in the path section for any character not in the unreserved, punct, escaped, or other categories,
     *          and not equal to the slash character ('/') or the commercial-at character ('{@literal @}').</li>
     *      <li>They do not provide quoting for any illegal characters found in the query and fragment sections.</li>
     *  </ul>
     *  On the other hand, java.net.URI's seven argument constructor provides these quoting capabilities. In order
     *  to take advantage of this constructor, this method parses the given string into the arguments needed
     *  thereby allowing for instantiating a java.net.URI with the quoting of all illegal characters.
     * @param uri String representing a URI.
     * @return Instance of java.net.URI
     * @throws URISyntaxException Thrown on parsing failures
     */
    public static URI create(String uri) throws URISyntaxException {
        final Matcher matcher = URI_PATTERN.matcher(uri);
        if (matcher.matches()) {
            final String scheme = matcher.group(2);
            final String userInfo = matcher.group(5);
            final String host = matcher.group(6);
            final String port = matcher.group(8);
            final String path = matcher.group(9);
            final String query = matcher.group(11);
            final String fragment = matcher.group(13);
            return new URI(scheme, userInfo, host, port != null ? Integer.parseInt(port) : -1, path, query, fragment);
        } else {
            throw new IllegalArgumentException(uri + " is not a valid URI");
        }
    }
}
