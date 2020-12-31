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
package org.apache.nifi.web.security.otp;

import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

/**
 * This filter is used to capture one time passwords (OTP) from requests made to download files through the browser.
 * It's required because when we initiate a download in the browser, it must be opened in a new tab. The new tab
 * cannot be initialized with authentication headers, so we must add a token as a query parameter instead. As
 * tokens in URL strings are visible in various places, this must only be used once - hence our OTP.
 */
public class OtpAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(OtpAuthenticationFilter.class);

    private static final Pattern PROVENANCE_DOWNLOAD_PATTERN =
        Pattern.compile("/provenance-events/([0-9]+)/content/((?:input)|(?:output))");
    private static final Pattern QUEUE_DOWNLOAD_PATTERN =
        Pattern.compile("/flowfile-queues/([a-f0-9\\-]{36})/flowfiles/([a-f0-9\\-]{36})/content");
    private static final Pattern TEMPLATE_DOWNLOAD_PATTERN =
        Pattern.compile("/templates/[a-f0-9\\-]{36}/download");
    private static final Pattern FLOW_DOWNLOAD_PATTERN =
        Pattern.compile("/process-groups/[a-f0-9\\-]{36}/download");

    protected static final String ACCESS_TOKEN = "access_token";

    @Override
    public Authentication attemptAuthentication(final HttpServletRequest request) {
        // only support otp login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // get the accessToken out of the query string
        final String accessToken = request.getParameter(ACCESS_TOKEN);

        // if there is no authorization header, we don't know the user
        if (accessToken == null) {
            return null;
        } else {
            if (request.getContextPath().equals("/nifi-api")) {
                if (isDownloadRequest(request.getPathInfo())) {
                    // handle download requests
                    return new OtpAuthenticationRequestToken(accessToken, true, request.getRemoteAddr());
                }
            } else {
                // handle requests to other context paths (other UI extensions)
                return new OtpAuthenticationRequestToken(accessToken, false, request.getRemoteAddr());
            }

            // the path is a support path for otp tokens
            return null;
        }
    }

    private boolean isDownloadRequest(final String pathInfo) {
        return PROVENANCE_DOWNLOAD_PATTERN.matcher(pathInfo).matches() || QUEUE_DOWNLOAD_PATTERN.matcher(pathInfo).matches()
                || TEMPLATE_DOWNLOAD_PATTERN.matcher(pathInfo).matches() || FLOW_DOWNLOAD_PATTERN.matcher(pathInfo).matches();
    }

}
