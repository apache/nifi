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
package org.apache.nifi.web.security.saml2;

import static org.apache.nifi.web.security.saml2.registration.Saml2RegistrationProperty.REGISTRATION_ID;

/**
 * Shared configuration for SAML URL Paths
 */
public enum SamlUrlPath {
    METADATA("/access/saml/metadata"),

    LOCAL_LOGOUT_REQUEST("/access/saml/local-logout/request"),

    LOGIN_RESPONSE(String.format("/access/saml/login/%s", REGISTRATION_ID.getProperty())),

    LOGIN_RESPONSE_REGISTRATION_ID("/access/saml/login/{registrationId}"),

    SINGLE_LOGOUT_REQUEST("/access/saml/single-logout/request"),

    SINGLE_LOGOUT_RESPONSE(String.format("/access/saml/single-logout/%s", REGISTRATION_ID.getProperty()));

    private final String path;

    SamlUrlPath(final String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
