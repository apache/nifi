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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

/**
 *
 */
public class OtpAuthenticationProvider implements AuthenticationProvider {

    private OtpService otpService;

    public OtpAuthenticationProvider(OtpService otpService) {
        this.otpService = otpService;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        final OtpAuthenticationRequestToken request = (OtpAuthenticationRequestToken) authentication;

        try {
            final String otpPrincipal;
            if (request.isDownloadToken()) {
                otpPrincipal = otpService.getAuthenticationFromDownloadToken(request.getToken());
            } else {
                otpPrincipal = otpService.getAuthenticationFromUiExtensionToken(request.getToken());
            }
            final NiFiUser user = new NiFiUser(otpPrincipal);
            return new NiFiAuthenticationToken(new NiFiUserDetails(user));
        } catch (OtpAuthenticationException e) {
            throw new InvalidAuthenticationException(e.getMessage(), e);
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return OtpAuthenticationRequestToken.class.isAssignableFrom(authentication);
    }
}
