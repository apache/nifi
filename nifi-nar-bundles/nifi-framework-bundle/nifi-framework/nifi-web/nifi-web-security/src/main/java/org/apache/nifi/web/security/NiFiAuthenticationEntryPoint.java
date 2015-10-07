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

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.WebAttributes;

/**
 * This is our own implementation of
 * org.springframework.security.web.AuthenticationEntryPoint that allows us to
 * send the response to the client exactly how we want to and log the results.
 */
public class NiFiAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private static final Logger logger = LoggerFactory.getLogger(NiFiAuthenticationEntryPoint.class);

    /**
     * Always returns a 403 error code to the client.
     *
     * @param request request
     * @param response response
     * @param ae ae
     * @throws java.io.IOException ex
     * @throws javax.servlet.ServletException ex
     */
    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException ae) throws IOException, ServletException {
        // get the last exception - the exception that is being passed in is a generic no credentials found
        // exception because the authentication could not be found in the security context. the actual cause
        // of the problem is stored in the session as the authentication_exception
        Object authenticationException = request.getSession().getAttribute(WebAttributes.AUTHENTICATION_EXCEPTION);

        // log request result
        if (authenticationException instanceof AuthenticationException) {
            ae = (AuthenticationException) authenticationException;
            logger.info(String.format("Rejecting access to web api: %s", ae.getMessage()));
        }

        // set the response status
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        response.setContentType("text/plain");

        // write the response message
        PrintWriter out = response.getWriter();
        out.println("Access is denied.");
    }
}
