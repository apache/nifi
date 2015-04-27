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
package org.apache.nifi.authorization;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.IdentityAlreadyExistsException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.ProviderDestructionException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;

/**
 * This class allows clients to retrieve the authorities for a given DN.
 */
public interface AuthorityProvider {

    /**
     * @param dn of the user
     * @return whether the user with the specified DN is known to this authority
     * provider. It is not necessary for the user to have any authorities
     */
    boolean doesDnExist(String dn) throws AuthorityAccessException;

    /**
     * Get the authorities for the specified user. If the specified user exists
     * but does not have any authorities, an empty set should be returned.
     *
     * @param dn of the user to lookup
     * @return the authorities for the specified user. If the specified user
     * exists but does not have any authorities, an empty set should be returned
     * @throws UnknownIdentityException if identity is not known
     * @throws AuthorityAccessException if unable to access authorities
     */
    Set<Authority> getAuthorities(String dn) throws UnknownIdentityException, AuthorityAccessException;

    /**
     * Sets the specified authorities for the specified user.
     *
     * @param dn the specified user
     * @param authorities the new authorities for the user
     * @throws UnknownIdentityException if identity is not known
     * @throws AuthorityAccessException if unable to access authorities
     */
    void setAuthorities(String dn, Set<Authority> authorities) throws UnknownIdentityException, AuthorityAccessException;

    /**
     * Gets the users for the specified authority.
     *
     * @param authority for which to determine membership of
     * @return all users with the specified authority
     * @throws AuthorityAccessException if unable to access authorities
     */
    Set<String> getUsers(Authority authority) throws AuthorityAccessException;

    /**
     * Revokes the specified user. Its up to the implementor to determine the
     * semantics of revocation.
     *
     * @param dn the dn of the user
     * @throws UnknownIdentityException if the user is not known
     * @throws AuthorityAccessException if unable to access the authorities
     */
    void revokeUser(String dn) throws UnknownIdentityException, AuthorityAccessException;

    /**
     * Add the specified user.
     *
     * @param dn of the user
     * @param group Optional
     * @throws UnknownIdentityException if the user is not known
     * @throws AuthorityAccessException if unable to access the authorities
     */
    void addUser(String dn, String group) throws IdentityAlreadyExistsException, AuthorityAccessException;

    /**
     * Gets the group for the specified user. Return null if the user does not
     * belong to a group.
     *
     * @param dn the user
     * @return the group of the given user
     * @throws UnknownIdentityException if the user is not known
     * @throws AuthorityAccessException if unable to access the authorities
     */
    String getGroupForUser(String dn) throws UnknownIdentityException, AuthorityAccessException;

    /**
     * Revokes all users for a specified group. Its up to the implementor to
     * determine the semantics of revocation.
     *
     * @param group to revoke the users of
     * @throws UnknownIdentityException if the user is not known
     * @throws AuthorityAccessException if unable to access the authorities
     */
    void revokeGroup(String group) throws UnknownIdentityException, AuthorityAccessException;

    /**
     * Adds the specified users to the specified group.
     *
     * @param dn the set of users to add to the group
     * @param group to add users to
     * @throws UnknownIdentityException if the user is not known
     * @throws AuthorityAccessException if unable to access the authorities
     */
    void setUsersGroup(Set<String> dn, String group) throws UnknownIdentityException, AuthorityAccessException;

    /**
     * Ungroups the specified user.
     *
     * @param dn of the user
     * @throws UnknownIdentityException if the user is not known
     * @throws AuthorityAccessException if unable to access the authorities
     */
    void ungroupUser(String dn) throws UnknownIdentityException, AuthorityAccessException;

    /**
     * Ungroups the specified group. Since the semantics of revocation is up to
     * the implementor, this method should do nothing if the specified group
     * does not exist. If an admin revoked this group before calling ungroup, it
     * may or may not exist.
     *
     * @param group to ungroup
     * @throws AuthorityAccessException if unable to access the authorities
     */
    void ungroup(String group) throws AuthorityAccessException;

    /**
     * Determines whether the user in the specified dnChain should be able to
     * download the content for the flowfile with the specified attributes.
     *
     * The first dn in the chain is the end user that the request was issued on
     * behalf of. The subsequent dn's in the chain represent entities proxying
     * the user's request with the last being the proxy that sent the current
     * request.
     *
     * @param dnChain of the user
     * @param attributes of the flowfile being requested
     * @return the authorization result
     * @throws UnknownIdentityException if the user is not known
     * @throws AuthorityAccessException if unable to access the authorities
     */
    DownloadAuthorization authorizeDownload(List<String> dnChain, Map<String, String> attributes) throws UnknownIdentityException, AuthorityAccessException;

    /**
     * Called immediately after instance creation for implementers to perform
     * additional setup
     *
     * @param initializationContext in which to initialize
     */
    void initialize(AuthorityProviderInitializationContext initializationContext) throws ProviderCreationException;

    /**
     * Called to configure the AuthorityProvider.
     *
     * @param configurationContext at the time of configuration
     * @throws ProviderCreationException for any issues configuring the provider
     */
    void onConfigured(AuthorityProviderConfigurationContext configurationContext) throws ProviderCreationException;

    /**
     * Called immediately before instance destruction for implementers to
     * release resources.
     *
     * @throws ProviderDestructionException If pre-destruction fails.
     */
    void preDestruction() throws ProviderDestructionException;
}
