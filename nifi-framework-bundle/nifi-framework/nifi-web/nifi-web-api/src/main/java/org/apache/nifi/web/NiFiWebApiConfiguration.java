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
package org.apache.nifi.web;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.EntityStoreAuditService;
import org.apache.nifi.framework.configuration.ApplicationPropertiesConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.configuration.AuthenticationConfiguration;
import org.apache.nifi.web.configuration.WebApplicationConfiguration;
import org.apache.nifi.web.security.configuration.WebSecurityConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Web Application Spring Configuration
 */
@ComponentScan(basePackageClasses = {
        ApplicationPropertiesConfiguration.class,
        WebSecurityConfiguration.class,
        WebApplicationConfiguration.class
})
@Configuration
public class NiFiWebApiConfiguration {
    private static final URI OAUTH2_AUTHORIZATION_URI = getPathUri("/nifi-api/oauth2/authorization/consumer");

    private static final URI OIDC_LOGOUT_URI = getPathUri("/nifi-api/access/oidc/logout");

    private static final URI SAML2_AUTHENTICATE_URI = getPathUri("/nifi-api/saml2/authenticate/consumer");

    private static final URI SAML_LOCAL_LOGOUT_URI = getPathUri("/nifi-api/access/saml/local-logout/request");

    private static final URI SAML_SINGLE_LOGOUT_URI = getPathUri("/nifi-api/access/saml/single-logout/request");

    private static final URI LOGIN_FORM_URI = getLoginFormUri();

    private static final URI LOGOUT_COMPLETE_URI = getPathUri("/nifi-api/access/logout/complete");

    private static final String UI_PATH = "/nf/";

    private static final String LOGIN_FRAGMENT = "/login";

    public NiFiWebApiConfiguration() {
        super();
    }

    /**
     * Audit Service implementation from nifi-administration
     *
     * @param properties NiFi Properties
     * @return Audit Service implementation using Persistent Entity Store
     */
    @Bean
    public AuditService auditService(final NiFiProperties properties) {
        final File databaseDirectory = properties.getDatabaseRepositoryPath().toFile();
        return new EntityStoreAuditService(databaseDirectory);
    }

    @Bean
    public AuthenticationConfiguration authenticationConfiguration(final NiFiProperties properties) {
        final URI loginUri;
        final URI logoutUri;
        final boolean externalLoginRequired;

        // HTTPS is required for authentication
        if (properties.isHTTPSConfigured()) {
            final String loginIdentityProvider = properties.getProperty(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER);
            if (properties.isOidcEnabled()) {
                externalLoginRequired = true;
                loginUri = OAUTH2_AUTHORIZATION_URI;
                logoutUri = OIDC_LOGOUT_URI;
            } else if (properties.isSamlEnabled()) {
                externalLoginRequired = true;
                loginUri = SAML2_AUTHENTICATE_URI;
                if (properties.isSamlSingleLogoutEnabled()) {
                    logoutUri = SAML_SINGLE_LOGOUT_URI;
                } else {
                    logoutUri = SAML_LOCAL_LOGOUT_URI;
                }
            } else if (StringUtils.hasText(loginIdentityProvider)) {
                externalLoginRequired = false;
                loginUri = LOGIN_FORM_URI;
                logoutUri = LOGOUT_COMPLETE_URI;
            } else {
                externalLoginRequired = false;
                loginUri = null;
                logoutUri = null;
            }
        } else {
            externalLoginRequired = false;
            loginUri = null;
            logoutUri = null;
        }

        final boolean loginSupported = loginUri != null;
        return new AuthenticationConfiguration(externalLoginRequired, loginSupported, loginUri, logoutUri);
    }

    private static URI getPathUri(final String path) {
        try {
            return new URI(null, null, path, null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Path URI construction failed", e);
        }
    }

    private static URI getLoginFormUri() {
        try {
            return new URI(null, null, UI_PATH, LOGIN_FRAGMENT);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Path Fragment URI construction failed", e);
        }
    }
}
