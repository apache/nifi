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

package org.apache.nifi.web.servlet.filter;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;

import java.io.IOException;
import java.net.URI;

public class QueryStringToFragmentFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        final String queryString = httpServletRequest.getQueryString();

        if (queryString != null) {
            // Some NiFi front ends use hash based routing, so they don't need to know the baseHref. With hash based
            // routing query parameters are implemented within the URL fragment. Because of this any query parameters on the
            // original URL are not considered. This filter captures those and adds them to the fragment.
            final String contextPath = httpServletRequest.getContextPath() + "/";
            final RequestUriBuilder requestUriBuilder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest).path(contextPath).fragment("/?" + queryString);
            final URI redirectUri = requestUriBuilder.build();

            final HttpServletResponse httpServletResponse = (HttpServletResponse) response;
            httpServletResponse.sendRedirect(redirectUri.toString());
        } else {
            filterChain.doFilter(request, response);
        }
    }
}
