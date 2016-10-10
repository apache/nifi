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
package org.apache.nifi.web.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.authorization.user.NiFiUser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A filter to log requests.
 *
 */
public class RequestLogger implements Filter {

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(RequestLogger.class));

    @Override
    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain filterChain)
            throws IOException, ServletException {

        final HttpServletRequest request = (HttpServletRequest) req;

        // only log http requests has https requests are logged elsewhere
        if ("http".equalsIgnoreCase(request.getScheme())) {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();

            // get the user details for the log message
            String identity = "<no user found>";
            if (user != null) {
                identity = user.getIdentity();
            }

            // log the request attempt - response details will be logged later
            logger.info(String.format("Attempting request for (%s) %s %s (source ip: %s)", identity, request.getMethod(),
                    request.getRequestURL().toString(), request.getRemoteAddr()));
        }

        // continue the filter chain
        filterChain.doFilter(req, resp);
    }

    @Override
    public void init(final FilterConfig config) {
    }

    @Override
    public void destroy() {
    }

}
