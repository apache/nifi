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
package org.apache.nifi.web.client.api;

import java.net.URI;

/**
 * HTTP URI Builder supports construction of a URI using component elements
 */
public interface HttpUriBuilder {
    /**
     * Build URI based on current component elements
     *
     * @return URI
     */
    URI build();

    /**
     * Set URI scheme as http or https
     *
     * @param scheme URI scheme
     * @return Builder
     */
    HttpUriBuilder scheme(String scheme);

    /**
     * Set URI host address
     *
     * @param host Host address
     * @return Builder
     */
    HttpUriBuilder host(String host);

    /**
     * Set URI port number
     *
     * @param port Port number
     * @return Builder
     */
    HttpUriBuilder port(int port);

    /**
     * Set path with segments encoded according to URL standard requirements
     *
     * @param encodedPath URL-encoded path
     * @return Builder
     */
    HttpUriBuilder encodedPath(String encodedPath);

    /**
     * Add path segment appending to current path
     *
     * @param pathSegment Path segment
     * @return Builder
     */
    HttpUriBuilder addPathSegment(String pathSegment);

    /**
     * Add query parameter using specified name and value
     *
     * @param name Query parameter name
     * @param value Query parameter value can be null
     * @return Builder
     */
    HttpUriBuilder addQueryParameter(String name, String value);
}
