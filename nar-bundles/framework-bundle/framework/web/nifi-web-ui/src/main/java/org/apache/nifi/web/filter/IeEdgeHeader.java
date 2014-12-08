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
import javax.servlet.http.HttpServletResponse;

/**
 * A filter to set a response header directing IE to use the most recent
 * document mode.
 *
 * @author unattributed
 */
public class IeEdgeHeader implements Filter {

    @Override
    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain filterChain)
            throws IOException, ServletException {

        // add the header
        final HttpServletResponse response = (HttpServletResponse) resp;
        response.addHeader("X-UA-Compatible", "IE=edge");

        // continue the chain
        filterChain.doFilter(req, resp);
    }

    @Override
    public void init(final FilterConfig config) {
    }

    @Override
    public void destroy() {
    }

}
