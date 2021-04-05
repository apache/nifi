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
package org.apache.nifi.web.security.requests;

import org.apache.nifi.logging.NiFiLog;
import org.eclipse.jetty.servlets.DoSFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class NiFiDoSFilter extends DoSFilter {
    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(NiFiDoSFilter.class));

    @Override
    public void init(FilterConfig config) throws ServletException {
        // TODO: could customize what paths should be bypassed using the filter config and initializing the FilterPathUtil
        super.init(config);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        if (FilterPathUtil.isSubjectToFilter(httpRequest, NiFiDoSFilter.class.getSimpleName())) {
            logger.debug("DoS filter being applied to request on path {}", httpRequest.getRequestURI());
            super.doFilter(request, response, chain);
        } else {
            logger.debug("DoS filter not applied for request path {}", httpRequest.getRequestURI());
            chain.doFilter(request, response);
            return;
        }
    }
}
