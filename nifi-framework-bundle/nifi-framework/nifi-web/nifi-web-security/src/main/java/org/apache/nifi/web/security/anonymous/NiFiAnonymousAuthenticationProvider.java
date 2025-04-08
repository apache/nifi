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
package org.apache.nifi.web.security.anonymous;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiAuthenticationProvider;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

/**
 *
 */
public class NiFiAnonymousAuthenticationProvider extends NiFiAuthenticationProvider {

    final NiFiProperties properties;

    public NiFiAnonymousAuthenticationProvider(NiFiProperties nifiProperties, Authorizer authorizer) {
        super(nifiProperties, authorizer);
        this.properties = nifiProperties;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        final NiFiAnonymousAuthenticationRequestToken request = (NiFiAnonymousAuthenticationRequestToken) authentication;

        if (request.isSecureRequest() && !properties.isAnonymousAuthenticationAllowed()) {
            throw new InvalidAuthenticationException("Anonymous authentication has not been configured.");
        }

        return new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.populateAnonymousUser(null, request.getClientAddress())), null, request.getDetails());
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return NiFiAnonymousAuthenticationRequestToken.class.isAssignableFrom(authentication);
    }
}
