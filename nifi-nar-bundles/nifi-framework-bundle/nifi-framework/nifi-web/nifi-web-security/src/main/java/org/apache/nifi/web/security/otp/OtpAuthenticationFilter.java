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

import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.apache.nifi.web.security.token.NiFiAuthorizationRequestToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 */
public class OtpAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(OtpAuthenticationFilter.class);

    private static final Pattern PROVENANCE_DOWNLOAD_PATTERN =
        Pattern.compile("/controller/provenance/events/[0-9]+/content/(?:(?:output)|(?:input))");
    private static final Pattern QUEUE_DOWNLOAD_PATTERN =
        Pattern.compile("/controller/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/connections/[a-f0-9\\-]{36}/flowfiles/[a-f0-9\\-]{36}/content");
    private static final Pattern TEMPLATE_DOWNLOAD_PATTERN =
        Pattern.compile("/controller/templates/[a-f0-9\\-]{36}");

    protected static final String ACCESS_TOKEN = "access_token";

    private OtpService otpService;

    @Override
    public NiFiAuthorizationRequestToken attemptAuthentication(final HttpServletRequest request) {
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
            try {
                String identity = null;
                if (request.getContextPath().equals("/nifi-api")) {
                    if (isDownloadRequest(request.getPathInfo())) {
                        // handle download requests
                        identity = otpService.getAuthenticationFromDownloadToken(accessToken);
                    }
                } else {
                    // handle requests to other context paths (other UI extensions)
                    identity = otpService.getAuthenticationFromUiExtensionToken(accessToken);
                }

                // the path is a support path for otp tokens
                if (identity == null) {
                    return null;
                }

                return new NiFiAuthorizationRequestToken(Arrays.asList(identity));
            } catch (final OtpAuthenticationException oae) {
                throw new InvalidAuthenticationException(oae.getMessage(), oae);
            }
        }
    }

    private boolean isDownloadRequest(final String pathInfo) {
        return PROVENANCE_DOWNLOAD_PATTERN.matcher(pathInfo).matches() || QUEUE_DOWNLOAD_PATTERN.matcher(pathInfo).matches() || TEMPLATE_DOWNLOAD_PATTERN.matcher(pathInfo).matches();
    }

    public void setOtpService(OtpService otpService) {
        this.otpService = otpService;
    }

}
