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

package org.apache.nifi.minifi.c2.security.authentication;

import org.apache.nifi.minifi.c2.api.security.authorization.AuthorityGranter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

public class X509AuthenticationProvider implements AuthenticationProvider {
    private static final Logger logger = LoggerFactory.getLogger(X509AuthenticationProvider.class);
    private final AuthorityGranter authorityGranter;

    public X509AuthenticationProvider(AuthorityGranter authorityGranter) {
        this.authorityGranter = authorityGranter;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        X509AuthenticationToken x509AuthenticationToken = (X509AuthenticationToken) authentication;
        if (logger.isDebugEnabled()) {
            logger.debug("Authenticating " + X509AuthenticationToken.class.getSimpleName() + " with principal " +  x509AuthenticationToken.getPrincipal());
        }
        return new C2AuthenticationToken(x509AuthenticationToken.getPrincipal(), x509AuthenticationToken.getCredentials(),
                authorityGranter.grantAuthorities(authentication));
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return X509AuthenticationToken.class.isAssignableFrom(authentication);
    }
}
