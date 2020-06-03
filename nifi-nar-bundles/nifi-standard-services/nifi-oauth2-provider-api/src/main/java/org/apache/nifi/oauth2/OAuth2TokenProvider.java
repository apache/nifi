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

package org.apache.nifi.oauth2;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.ssl.SSLContextService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Interface for defining a credential-providing controller service for oauth2 processes.
 */
public interface OAuth2TokenProvider extends ControllerService {
    PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("oauth2-ssl-context")
            .displayName("SSL Context")
            .addValidator(Validator.VALID)
            .identifiesControllerService(SSLContextService.class)
            .required(false)
            .build();
    PropertyDescriptor ACCESS_TOKEN_URL = new PropertyDescriptor.Builder()
            .name("oauth2-access-token-url")
            .displayName("Access Token Url")
            .description("The full endpoint of the URL where access tokens are handled.")
            .required(true)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            SSL_CONTEXT, ACCESS_TOKEN_URL
    ));

    AccessToken getAccessTokenByPassword(String clientId, String clientSecret, String username, String password) throws AccessTokenAcquisitionException;
    AccessToken getAccessTokenByClientCredentials(String clientId, String clientSecret) throws AccessTokenAcquisitionException;
    AccessToken refreshToken(AccessToken refreshThis) throws AccessTokenAcquisitionException;
}
