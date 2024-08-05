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
package org.apache.nifi.web.servlet.shared;

import jakarta.servlet.http.HttpServletRequest;

import java.net.URI;

/**
 * Abstraction for resolving and returning an HTTP Request URI based on presented headers and configured properties
 */
public interface RequestUriProvider {
    /**
     * Get Request URI from HTTP Servlet Request containing optional headers
     *
     * @param request HTTP Servlet Request
     * @return Request URI
     */
    URI getRequestUri(HttpServletRequest request);
}
