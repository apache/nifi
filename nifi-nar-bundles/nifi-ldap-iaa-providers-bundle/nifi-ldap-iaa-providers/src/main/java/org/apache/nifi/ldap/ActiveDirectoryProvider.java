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
package org.apache.nifi.ldap;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.springframework.security.ldap.authentication.AbstractLdapAuthenticationProvider;
import org.springframework.security.ldap.authentication.ad.ActiveDirectoryLdapAuthenticationProvider;

/**
 * Active Directory based implementation of a login identity provider.
 */
public class ActiveDirectoryProvider extends AbstractLdapProvider {

    @Override
    protected AbstractLdapAuthenticationProvider getLdapAuthenticationProvider(LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        final String domain = configurationContext.getProperty("Domain");
        if (StringUtils.isBlank(domain)) {
            throw new ProviderCreationException("The Active Directory Domain must be specified.");
        }

        final String url = configurationContext.getProperty("Url");
        if (StringUtils.isBlank(url)) {
            throw new ProviderCreationException("The Active Directory Url must be specified.");
        }

        final String rootDn = configurationContext.getProperty("Root DN");

        return new ActiveDirectoryLdapAuthenticationProvider(domain, url, StringUtils.isBlank(rootDn) ? null : rootDn);
    }
}
