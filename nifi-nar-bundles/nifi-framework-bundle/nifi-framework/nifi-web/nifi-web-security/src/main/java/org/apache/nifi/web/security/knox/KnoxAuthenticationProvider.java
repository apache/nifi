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
package org.apache.nifi.web.security.knox;

import com.nimbusds.jose.JOSEException;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser.Builder;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiAuthenticationProvider;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import java.text.ParseException;

/**
 *
 */
public class KnoxAuthenticationProvider extends NiFiAuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(KnoxAuthenticationProvider.class);

    private final KnoxService knoxService;

    public KnoxAuthenticationProvider(KnoxService knoxService, NiFiProperties nifiProperties, Authorizer authorizer) {
        super(nifiProperties, authorizer);
        this.knoxService = knoxService;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        final KnoxAuthenticationRequestToken request = (KnoxAuthenticationRequestToken) authentication;

        try {
            final String jwtPrincipal = knoxService.getAuthenticationFromToken(request.getToken());
            final String mappedIdentity = mapIdentity(jwtPrincipal);
            final NiFiUser user = new Builder().identity(mappedIdentity).groups(getUserGroups(mappedIdentity)).clientAddress(request.getClientAddress()).build();
            return new NiFiAuthenticationToken(new NiFiUserDetails(user));
        } catch (ParseException | JOSEException e) {
            logger.info("Unable to validate the access token: " + e.getMessage(), e);
            throw new InvalidAuthenticationException("Unable to validate the access token.", e);
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return KnoxAuthenticationRequestToken.class.isAssignableFrom(authentication);
    }
}
