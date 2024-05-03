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
package org.apache.nifi.web.security.csrf;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.security.web.csrf.CsrfTokenRequestAttributeHandler;
import org.springframework.security.web.csrf.XorCsrfTokenRequestAttributeHandler;
import org.springframework.util.StringUtils;

import java.util.function.Supplier;

/**
 * Cross-Site Request Forgery Mitigation Token Handler implementation supporting resolution using Request Header
 */
public class StandardCsrfTokenRequestAttributeHandler extends CsrfTokenRequestAttributeHandler {
    private final XorCsrfTokenRequestAttributeHandler handler = new XorCsrfTokenRequestAttributeHandler();

    /**
     * Handle Request using standard Spring Security implementation
     *
     * @param request HTTP Servlet Request being handled
     * @param response HTTP Servlet Response being handled
     * @param csrfTokenSupplier Supplier for CSRF Token
     */
    @Override
    public void handle(final HttpServletRequest request, final HttpServletResponse response, final Supplier<CsrfToken> csrfTokenSupplier) {
        this.handler.handle(request, response, csrfTokenSupplier);
    }

    /**
     * Resolve CSRF Token Value from HTTP Request Header
     *
     * @param request HTTP Servlet Request being processed
     * @param csrfToken CSRF Token created from a CSRF Token Repository
     * @return Token Value from Request Header or null when not found
     */
    @Override
    public String resolveCsrfTokenValue(final HttpServletRequest request, final CsrfToken csrfToken) {
        final String headerTokenValue = request.getHeader(csrfToken.getHeaderName());

        final String resolvedToken;
        if (StringUtils.hasText(headerTokenValue)) {
            resolvedToken = super.resolveCsrfTokenValue(request, csrfToken);
        } else {
            resolvedToken = null;
        }

        return resolvedToken;
    }
}
