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
package org.apache.nifi.web.security.oidc.client.web.converter;

import org.springframework.core.convert.converter.Converter;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.client.authentication.OAuth2LoginAuthenticationToken;

/**
 * OAuth2 Authentication Result Converter returns an extended Authentication Token containing the OAuth2 Access Token
 */
public class AuthenticationResultConverter implements Converter<OAuth2LoginAuthenticationToken, OAuth2AuthenticationToken> {
    /**
     * Convert OAuth2 Login Authentication Token to OAuth2 Authentication Token containing the OAuth2 Access Token
     *
     * @param source OAuth2 Login Authentication Token
     * @return Standard extension of OAuth2 Authentication Token
     */
    @Override
    public OAuth2AuthenticationToken convert(final OAuth2LoginAuthenticationToken source) {
        return new StandardOAuth2AuthenticationToken(
                source.getPrincipal(),
                source.getAuthorities(),
                source.getClientRegistration().getRegistrationId(),
                source.getAccessToken()
        );
    }
}
