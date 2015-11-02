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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.authorization.annotation.AuthorityProviderContext;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.IdentityAlreadyExistsException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.user.generated.ObjectFactory;
import org.apache.nifi.user.generated.Role;
import org.apache.nifi.user.generated.User;
import org.apache.nifi.util.NiFiProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorized.users.AuthorizedUsers;
import org.apache.nifi.authorized.users.AuthorizedUsers.CreateUser;
import org.apache.nifi.authorized.users.AuthorizedUsers.FindUser;
import org.apache.nifi.authorized.users.AuthorizedUsers.FindUsers;
import org.apache.nifi.authorized.users.AuthorizedUsers.HasUser;
import org.apache.nifi.authorized.users.AuthorizedUsers.UpdateUser;
import org.apache.nifi.authorized.users.AuthorizedUsers.UpdateUsers;
import org.apache.nifi.user.generated.NiFiUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides identity checks and grants authorities.
 */
public class FileAuthorizationProvider implements AuthorityProvider {

    private static final Logger logger = LoggerFactory.getLogger(FileAuthorizationProvider.class);

    private NiFiProperties properties;
    private final Set<String> defaultAuthorities = new HashSet<>();

    private AuthorizedUsers authorizedUsers;

    @Override
    public void initialize(final AuthorityProviderInitializationContext initializationContext) throws ProviderCreationException {
    }

    @Override
    public void onConfigured(final AuthorityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        final String usersFilePath = configurationContext.getProperty("Authorized Users File");
        if (usersFilePath == null || usersFilePath.trim().isEmpty()) {
            throw new ProviderCreationException("The authorized users file must be specified.");
        }

        try {
            // initialize the authorized users
            authorizedUsers = AuthorizedUsers.getInstance(usersFilePath, properties);

            // attempt to load a default roles
            final String rawDefaultAuthorities = configurationContext.getProperty("Default User Roles");
            if (StringUtils.isNotBlank(rawDefaultAuthorities)) {
                final Set<String> invalidDefaultAuthorities = new HashSet<>();

                // validate the specified authorities
                final String[] rawDefaultAuthorityList = rawDefaultAuthorities.split(",");
                for (String rawAuthority : rawDefaultAuthorityList) {
                    rawAuthority = rawAuthority.trim();
                    final Authority authority = Authority.valueOfAuthority(rawAuthority);
                    if (authority == null) {
                        invalidDefaultAuthorities.add(rawAuthority);
                    } else {
                        defaultAuthorities.add(rawAuthority);
                    }
                }

                // report any unrecognized authorities
                if (!invalidDefaultAuthorities.isEmpty()) {
                    logger.warn(String.format("The following default role(s) '%s' were not recognized. Possible values: %s.",
                            StringUtils.join(invalidDefaultAuthorities, ", "), StringUtils.join(Authority.getRawAuthorities(), ", ")));
                }
            }
        } catch (IOException | IllegalStateException | ProviderCreationException e) {
            throw new ProviderCreationException(e);
        }
    }

    @Override
    public void preDestruction() {
    }

    private boolean hasDefaultRoles() {
        return !defaultAuthorities.isEmpty();
    }

    @Override
    public boolean doesDnExist(final String dn) throws AuthorityAccessException {
        if (hasDefaultRoles()) {
            return true;
        }

        return authorizedUsers.hasUser(new HasUserByIdentity(dn));
    }

    @Override
    public Set<Authority> getAuthorities(final String dn) throws UnknownIdentityException, AuthorityAccessException {
        final Set<Authority> authorities = EnumSet.noneOf(Authority.class);

        // get the user
        final NiFiUser user = authorizedUsers.getUser(new FindUser() {
            @Override
            public NiFiUser findUser(List<NiFiUser> users) {
                final FindUser byDn = new FindUserByIdentity(dn);
                NiFiUser user = byDn.findUser(users);

                // if the user is not found, add them and locate them
                if (user == null) {
                    if (hasDefaultRoles()) {
                        logger.debug(String.format("User identity not found: %s. Creating new user with default roles.", dn));

                        // create the user (which will automatically add any default authorities)
                        addUser(dn, null);

                        // find the user that was just added
                        user = byDn.findUser(users);
                    } else {
                        throw new UnknownIdentityException(String.format("User identity not found: %s.", dn));
                    }
                }

                return user;
            }
        });

        // create the authorities that this user has
        for (final Role role : user.getRole()) {
            authorities.add(Authority.valueOfAuthority(role.getName()));
        }

        return authorities;
    }

    @Override
    public void setAuthorities(final String dn, final Set<Authority> authorities) throws UnknownIdentityException, AuthorityAccessException {
        authorizedUsers.updateUser(new FindUserByIdentity(dn), new UpdateUser() {
            @Override
            public void updateUser(NiFiUser user) {
                // add the user authorities
                setUserAuthorities(user, authorities);
            }
        });
    }

    private void setUserAuthorities(final NiFiUser user, final Set<Authority> authorities) {
        // clear the existing rules
        user.getRole().clear();

        // set the new roles
        final ObjectFactory objFactory = new ObjectFactory();
        for (final Authority authority : authorities) {
            final Role role = objFactory.createRole();
            role.setName(authority.toString());

            // add the new role
            user.getRole().add(role);
        }
    }

    @Override
    public void addUser(final String dn, final String group) throws IdentityAlreadyExistsException, AuthorityAccessException {
        authorizedUsers.createUser(new CreateUser() {
            @Override
            public NiFiUser createUser() {
                // ensure the user doesn't already exist
                if (authorizedUsers.hasUser(new HasUserByIdentity(dn))) {
                    throw new IdentityAlreadyExistsException(String.format("User identity already exists: %s", dn));
                }

                // only support adding PreAuthenticatedUser's via this API - LoginUser's are added
                // via the LoginIdentityProvider
                final ObjectFactory objFactory = new ObjectFactory();
                final User newUser = objFactory.createUser();

                // set the user properties
                newUser.setDn(dn);
                newUser.setGroup(group);

                // add default roles if appropriate
                if (hasDefaultRoles()) {
                    for (final String authority : defaultAuthorities) {
                        Role role = objFactory.createRole();
                        role.setName(authority);

                        // add the role
                        newUser.getRole().add(role);
                    }
                }

                return newUser;
            }
        });
    }

    @Override
    public Set<String> getUsers(final Authority authority) throws AuthorityAccessException {
        final List<NiFiUser> matchingUsers = authorizedUsers.getUsers(new FindUsers() {
            @Override
            public List<NiFiUser> findUsers(List<NiFiUser> users) throws UnknownIdentityException {
                final List<NiFiUser> matchingUsers = new ArrayList<>();
                for (final NiFiUser user : users) {
                    for (final Role role : user.getRole()) {
                        if (role.getName().equals(authority.toString())) {
                            matchingUsers.add(user);
                        }
                    }
                }
                return matchingUsers;
            }
        });

        final Set<String> userSet = new HashSet<>();
        for (final NiFiUser user : matchingUsers) {
            userSet.add(authorizedUsers.getUserIdentity(user));
        }

        return userSet;
    }

    @Override
    public void revokeUser(final String dn) throws UnknownIdentityException, AuthorityAccessException {
        authorizedUsers.removeUser(new FindUserByIdentity(dn));
    }

    @Override
    public void setUsersGroup(final Set<String> dns, final String group) throws UnknownIdentityException, AuthorityAccessException {
        authorizedUsers.updateUsers(new FindUsersByIdentity(dns), new UpdateUsers() {
            @Override
            public void updateUsers(List<NiFiUser> users) {
                // update each user group
                for (final NiFiUser user : users) {
                    user.setGroup(group);
                }
            }
        });
    }

    @Override
    public void ungroupUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
        authorizedUsers.updateUser(new FindUserByIdentity(dn), new UpdateUser() {
            @Override
            public void updateUser(NiFiUser user) {
                // remove the users group
                user.setGroup(null);
            }
        });
    }

    @Override
    public void ungroup(final String group) throws AuthorityAccessException {
        authorizedUsers.updateUsers(new FindUsersByGroup(group), new UpdateUsers() {
            @Override
            public void updateUsers(List<NiFiUser> users) {
                // update each user group
                for (final NiFiUser user : users) {
                    user.setGroup(null);
                }
            }
        });
    }

    @Override
    public String getGroupForUser(final String dn) throws UnknownIdentityException, AuthorityAccessException {
        final NiFiUser user = authorizedUsers.getUser(new FindUserByIdentity(dn));
        return user.getGroup();
    }

    @Override
    public void revokeGroup(String group) throws UnknownIdentityException, AuthorityAccessException {
        authorizedUsers.removeUsers(new FindUsersByGroup(group));
    }

    /**
     * Grants access to download content regardless of FlowFile attributes.
     */
    @Override
    public DownloadAuthorization authorizeDownload(List<String> dnChain, Map<String, String> attributes) throws UnknownIdentityException, AuthorityAccessException {
        return DownloadAuthorization.approved();
    }

    @AuthorityProviderContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public class HasUserByIdentity implements HasUser {

        private final String identity;

        public HasUserByIdentity(String identity) {
            // ensure the identity was specified
            if (identity == null) {
                throw new UnknownIdentityException("User identity not specified.");
            }

            this.identity = identity;
        }

        @Override
        public boolean hasUser(List<NiFiUser> users) {
            // attempt to get the user and ensure it was located
            NiFiUser desiredUser = null;
            for (final NiFiUser user : users) {
                if (identity.equalsIgnoreCase(authorizedUsers.getUserIdentity(user))) {
                    desiredUser = user;
                    break;
                }
            }

            return desiredUser != null;
        }
    }

    public class FindUserByIdentity implements FindUser {

        private final String identity;

        public FindUserByIdentity(String identity) {
            // ensure the identity was specified
            if (identity == null) {
                throw new UnknownIdentityException("User identity not specified.");
            }

            this.identity = identity;
        }

        @Override
        public NiFiUser findUser(List<NiFiUser> users) {
            // attempt to get the user and ensure it was located
            NiFiUser desiredUser = null;
            for (final NiFiUser user : users) {
                if (identity.equalsIgnoreCase(authorizedUsers.getUserIdentity(user))) {
                    desiredUser = user;
                    break;
                }
            }

            if (desiredUser == null) {
                throw new UnknownIdentityException(String.format("User identity not found: %s.", identity));
            }

            return desiredUser;
        }
    }

    public static class FindUsersByGroup implements FindUsers {

        private final String group;

        public FindUsersByGroup(String group) {
            // ensure the group was specified
            if (group == null) {
                throw new UnknownIdentityException("User group not specified.");
            }

            this.group = group;
        }

        @Override
        public List<NiFiUser> findUsers(List<NiFiUser> users) throws UnknownIdentityException {
            // get all users with this group
            List<NiFiUser> userGroup = new ArrayList<>();
            for (final NiFiUser user : users) {
                if (group.equals(user.getGroup())) {
                    userGroup.add(user);
                }
            }

            // ensure the user group was located
            if (userGroup.isEmpty()) {
                throw new UnknownIdentityException(String.format("User group not found: %s.", group));
            }

            return userGroup;
        }
    }

    public class FindUsersByIdentity implements FindUsers {

        private final Set<String> identities;

        public FindUsersByIdentity(Set<String> identities) {
            // ensure the group was specified
            if (identities == null) {
                throw new UnknownIdentityException("User identities not specified.");
            }

            this.identities = identities;
        }

        @Override
        public List<NiFiUser> findUsers(List<NiFiUser> users) throws UnknownIdentityException {
            final Set<String> copy = new HashSet<>(identities);

            // get all users with this group
            List<NiFiUser> userList = new ArrayList<>();
            for (final NiFiUser user : users) {
                final String userIdentity = authorizedUsers.getUserIdentity(user);
                if (copy.contains(userIdentity)) {
                    copy.remove(userIdentity);
                    userList.add(user);
                }
            }

            if (!copy.isEmpty()) {
                throw new UnknownIdentityException("Unable to find users with identities: " + StringUtils.join(copy, ", "));
            }

            return userList;
        }
    }

}
