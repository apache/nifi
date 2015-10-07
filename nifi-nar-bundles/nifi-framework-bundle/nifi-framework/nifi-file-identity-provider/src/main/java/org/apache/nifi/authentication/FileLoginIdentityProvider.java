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
package org.apache.nifi.authentication;

import java.io.IOException;
import java.util.List;
import org.apache.nifi.authentication.annotation.LoginIdentityProviderContext;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.ProviderDestructionException;
import org.apache.nifi.authorized.users.AuthorizedUsers;
import org.apache.nifi.authorized.users.AuthorizedUsers.HasUser;
import org.apache.nifi.user.generated.LoginUser;
import org.apache.nifi.user.generated.NiFiUser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;

/**
 *
 */
public class FileLoginIdentityProvider implements LoginIdentityProvider {

    private AuthorizedUsers authorizedUsers;
    private NiFiProperties properties;

    @Override
    public void initialize(LoginIdentityProviderInitializationContext initializationContext) throws ProviderCreationException {
    }

    @Override
    public void onConfigured(LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        final String usersFilePath = configurationContext.getProperty("Authenticated Users File");
        if (usersFilePath == null || usersFilePath.trim().isEmpty()) {
            throw new ProviderCreationException("The authorized users file must be specified.");
        }

        try {
            // initialize the authorized users
            authorizedUsers = AuthorizedUsers.getInstance(usersFilePath, properties);
        } catch (IOException | IllegalStateException e) {
            throw new ProviderCreationException(e);
        }
    }

    @Override
    public boolean supportsRegistration() {
        return false;
    }

    @Override
    public void register(LoginCredentials credentials) {
    }

    @Override
    public boolean authenticate(final LoginCredentials credentials) {
        if (StringUtils.isBlank(credentials.getUsername()) || StringUtils.isBlank(credentials.getPassword())) {
            return false;
        }

        return authorizedUsers.hasUser(new HasUser() {
            @Override
            public boolean hasUser(List<NiFiUser> users) {
                for (final NiFiUser user : users) {
                    // only consider LoginUsers
                    if (LoginUser.class.isAssignableFrom(user.getClass())) {
                        final LoginUser loginUser = (LoginUser) user;

                        // TODO - need to properly encrypt and hash password
                        final String loginUserPassword = loginUser.getPassword();
                        if (credentials.getUsername().equals(loginUser.getUsername()) && credentials.getPassword().equals(loginUserPassword)) {
                            return true;
                        }
                    }
                }
                return false;
            }
        });
    }

    @Override
    public void preDestruction() throws ProviderDestructionException {
    }

    @LoginIdentityProviderContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
