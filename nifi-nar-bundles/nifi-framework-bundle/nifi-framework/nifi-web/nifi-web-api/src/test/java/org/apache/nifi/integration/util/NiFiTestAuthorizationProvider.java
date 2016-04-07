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
package org.apache.nifi.integration.util;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.AuthorityProviderConfigurationContext;
import org.apache.nifi.authorization.AuthorityProviderInitializationContext;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.DownloadAuthorization;

/**
 *
 */
public class NiFiTestAuthorizationProvider implements AuthorityProvider {

    private final Map<String, Set<Authority>> users;

    /**
     * Creates a new FileAuthorizationProvider.
     */
    public NiFiTestAuthorizationProvider() {
        users = new HashMap<>();
        users.put("CN=localhost, OU=Apache NiFi, O=Apache, L=Santa Monica, ST=CA, C=US", EnumSet.of(Authority.ROLE_PROXY));
        users.put("CN=Lastname Firstname Middlename monitor, OU=Unknown, OU=Unknown, OU=Unknown, O=Unknown, C=Unknown", EnumSet.of(Authority.ROLE_MONITOR));
        users.put("CN=Lastname Firstname Middlename dfm, OU=Unknown, OU=Unknown, OU=Unknown, O=Unknown, C=Unknown", EnumSet.of(Authority.ROLE_DFM));
        users.put("CN=Lastname Firstname Middlename admin, OU=Unknown, OU=Unknown, OU=Unknown, O=Unknown, C=Unknown", EnumSet.of(Authority.ROLE_ADMIN));
        users.put("user@nifi", EnumSet.of(Authority.ROLE_DFM));
    }

    @Override
    public void initialize(AuthorityProviderInitializationContext initializationContext) throws ProviderCreationException {
    }

    @Override
    public void onConfigured(AuthorityProviderConfigurationContext configurationContext) throws ProviderCreationException {
    }

    @Override
    public void preDestruction() {
    }

    private void checkDn(String dn) throws UnknownIdentityException {
        if (!users.containsKey(dn)) {
            throw new UnknownIdentityException("Unknown user: " + dn);
        }
    }

    /**
     * Determines if the specified dn is known to this authority provider.
     *
     * @param dn dn
     * @return True if he dn is known, false otherwise
     */
    @Override
    public boolean doesDnExist(String dn) throws AuthorityAccessException {
        try {
            checkDn(dn);
            return true;
        } catch (UnknownIdentityException uie) {
            return false;
        }
    }

    /**
     * Loads the authorities for the specified user.
     *
     * @param dn dn
     * @return authorities
     * @throws UnknownIdentityException ex
     * @throws AuthorityAccessException ex
     */
    @Override
    public Set<Authority> getAuthorities(String dn) throws UnknownIdentityException, AuthorityAccessException {
        checkDn(dn);
        return new HashSet<>(users.get(dn));
    }

    /**
     * Sets the specified authorities to the specified user.
     *
     * @param dn dn
     * @param authorities authorities
     * @throws AuthorityAccessException ex
     */
    @Override
    public void setAuthorities(String dn, Set<Authority> authorities) throws UnknownIdentityException, AuthorityAccessException {
    }

    /**
     * Adds the specified user.
     *
     * @param dn dn
     * @param group group
     * @throws UnknownIdentityException ex
     * @throws AuthorityAccessException ex
     */
    @Override
    public void addUser(String dn, String group) throws AuthorityAccessException {
    }

    /**
     * Gets the users for the specified authority.
     *
     * @param authority authority
     * @return users
     * @throws AuthorityAccessException ex
     */
    @Override
    public Set<String> getUsers(Authority authority) throws AuthorityAccessException {
        Set<String> usersForAuthority = new HashSet<>();
        for (String dn : users.keySet()) {
            if (users.get(dn).contains(authority)) {
                usersForAuthority.add(dn);
            }
        }
        return usersForAuthority;
    }

    /**
     * Removes the specified user.
     *
     * @param dn dn
     * @throws UnknownIdentityException ex
     * @throws AuthorityAccessException ex
     */
    @Override
    public void revokeUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
    }

    @Override
    public String getGroupForUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
        return StringUtils.EMPTY;
    }

    @Override
    public void revokeGroup(String group) throws UnknownIdentityException, AuthorityAccessException {
    }

    @Override
    public void setUsersGroup(Set<String> dn, String group) throws UnknownIdentityException, AuthorityAccessException {
    }

    @Override
    public void ungroupUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
    }

    @Override
    public void ungroup(String group) throws UnknownIdentityException, AuthorityAccessException {
    }

    @Override
    public DownloadAuthorization authorizeDownload(List<String> dnChain, Map<String, String> attributes) throws UnknownIdentityException, AuthorityAccessException {
        return DownloadAuthorization.approved();
    }

}
