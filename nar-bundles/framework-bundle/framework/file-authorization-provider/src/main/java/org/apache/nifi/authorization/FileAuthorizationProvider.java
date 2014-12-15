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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.apache.nifi.authorization.annotation.AuthorityProviderContext;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.IdentityAlreadyExistsException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.file.FileUtils;
import org.apache.nifi.user.generated.ObjectFactory;
import org.apache.nifi.user.generated.Role;
import org.apache.nifi.user.generated.User;
import org.apache.nifi.user.generated.Users;
import org.apache.nifi.util.NiFiProperties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Provides identity checks and grants authorities.
 */
public class FileAuthorizationProvider implements AuthorityProvider {

    private static final Logger logger = LoggerFactory.getLogger(FileAuthorizationProvider.class);
    private static final String USERS_XSD = "/users.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.user.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, FileAuthorizationProvider.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private NiFiProperties properties;
    private File usersFile;
    private File restoreUsersFile;
    private Users users;
    private final Set<String> defaultAuthorities = new HashSet<>();

    @Override
    public void initialize(final AuthorityProviderInitializationContext initializationContext) throws ProviderCreationException {
    }

    @Override
    public void onConfigured(final AuthorityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        try {
            final String usersFilePath = configurationContext.getProperty("Authorized Users File");
            if (usersFilePath == null || usersFilePath.trim().isEmpty()) {
                throw new ProviderCreationException("The authorized users file must be specified.");
            }

            // the users file instance will never be null because a default is used
            usersFile = new File(usersFilePath);
            final File usersFileDirectory = usersFile.getParentFile();

            // the restore directory is optional and may be null
            final File restoreDirectory = properties.getRestoreDirectory();

            if (restoreDirectory != null) {

                // sanity check that restore directory is a directory, creating it if necessary
                FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

                // check that restore directory is not the same as the primary directory
                if (usersFileDirectory.getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
                    throw new ProviderCreationException(String.format("Authorized User's directory '%s' is the same as restore directory '%s' ",
                            usersFileDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
                }

                // the restore copy will have same file name, but reside in a different directory
                restoreUsersFile = new File(restoreDirectory, usersFile.getName());

                // sync the primary copy with the restore copy
                try {
                    FileUtils.syncWithRestore(usersFile, restoreUsersFile, logger);
                } catch (final IOException | IllegalStateException ioe) {
                    throw new ProviderCreationException(ioe);
                }

            }

            // load the users from the specified file
            if (usersFile.exists()) {
                // find the schema
                final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                final Schema schema = schemaFactory.newSchema(FileAuthorizationProvider.class.getResource(USERS_XSD));

                // attempt to unmarshal
                final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);
                final JAXBElement<Users> element = unmarshaller.unmarshal(new StreamSource(usersFile), Users.class);
                users = element.getValue();
            } else {
                final ObjectFactory objFactory = new ObjectFactory();
                users = objFactory.createUsers();
            }

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
        } catch (IOException | ProviderCreationException | SAXException | JAXBException e) {
            throw new ProviderCreationException(e);
        }

    }

    @Override
    public void preDestruction() {
    }

    /**
     * Determines if this provider has a default role.
     *
     * @return
     */
    private boolean hasDefaultRoles() {
        return !defaultAuthorities.isEmpty();
    }

    /**
     * Determines if the specified dn is known to this authority provider. When
     * this provider is configured to have default role(s), all dn are
     * considered to exist.
     *
     * @param dn
     * @return True if he dn is known, false otherwise
     */
    @Override
    public boolean doesDnExist(String dn) throws AuthorityAccessException {
        if (hasDefaultRoles()) {
            return true;
        }

        final User user = getUser(dn);
        return user != null;
    }

    /**
     * Loads the authorities for the specified user. If this provider is
     * configured for default user role(s) and a non existent dn is specified, a
     * new user will be automatically created with the default role(s).
     *
     * @param dn
     * @return
     * @throws UnknownIdentityException
     * @throws AuthorityAccessException
     */
    @Override
    public synchronized Set<Authority> getAuthorities(String dn) throws UnknownIdentityException, AuthorityAccessException {
        final Set<Authority> authorities = EnumSet.noneOf(Authority.class);

        // get the user 
        final User user = getUser(dn);

        // ensure the user was located
        if (user == null) {
            if (hasDefaultRoles()) {
                logger.debug(String.format("User DN not found: %s. Creating new user with default roles.", dn));

                // create the user (which will automatically add any default authorities)
                addUser(dn, null);

                // get the authorities for the newly created user
                authorities.addAll(getAuthorities(dn));
            } else {
                throw new UnknownIdentityException(String.format("User DN not found: %s.", dn));
            }
        } else {
            // create the authorities that this user has
            for (final Role role : user.getRole()) {
                authorities.add(Authority.valueOfAuthority(role.getName()));
            }
        }

        return authorities;
    }

    /**
     * Adds the specified authorities to the specified user. Regardless of
     * whether this provider is configured for a default user role, when a non
     * existent dn is specified, an UnknownIdentityException will be thrown.
     *
     * @param dn
     * @param authorities
     * @throws UnknownIdentityException
     * @throws AuthorityAccessException
     */
    @Override
    public synchronized void setAuthorities(String dn, Set<Authority> authorities) throws UnknownIdentityException, AuthorityAccessException {
        // get the user
        final User user = getUser(dn);

        // ensure the user was located
        if (user == null) {
            throw new UnknownIdentityException(String.format("User DN not found: %s.", dn));
        }

        // add the user authorities
        setUserAuthorities(user, authorities);

        try {
            // save the file
            save();
        } catch (Exception e) {
            throw new AuthorityAccessException(e.getMessage(), e);
        }
    }

    /**
     * Adds the specified authorities to the specified user.
     *
     * @param user
     * @param authorities
     */
    private void setUserAuthorities(final User user, final Set<Authority> authorities) {
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

    /**
     * Adds the specified user. If this provider is configured with default
     * role(s) they will be added to the new user.
     *
     * @param dn
     * @param group
     * @throws UnknownIdentityException
     * @throws AuthorityAccessException
     */
    @Override
    public synchronized void addUser(String dn, String group) throws IdentityAlreadyExistsException, AuthorityAccessException {
        final User user = getUser(dn);

        // ensure the user doesn't already exist
        if (user != null) {
            throw new IdentityAlreadyExistsException(String.format("User DN already exists: %s", dn));
        }

        // create the new user
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

        // add the user
        users.getUser().add(newUser);

        try {
            // save the file
            save();
        } catch (Exception e) {
            throw new AuthorityAccessException(e.getMessage(), e);
        }
    }

    /**
     * Gets the users for the specified authority.
     *
     * @param authority
     * @return
     * @throws AuthorityAccessException
     */
    @Override
    public synchronized Set<String> getUsers(Authority authority) throws AuthorityAccessException {
        final Set<String> userSet = new HashSet<>();
        for (final User user : users.getUser()) {
            for (final Role role : user.getRole()) {
                if (role.getName().equals(authority.toString())) {
                    userSet.add(user.getDn());
                }
            }
        }
        return userSet;
    }

    /**
     * Removes the specified user. Regardless of whether this provider is
     * configured for a default user role, when a non existent dn is specified,
     * an UnknownIdentityException will be thrown.
     *
     * @param dn
     * @throws UnknownIdentityException
     * @throws AuthorityAccessException
     */
    @Override
    public synchronized void revokeUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
        // get the user
        final User user = getUser(dn);

        // ensure the user was located
        if (user == null) {
            throw new UnknownIdentityException(String.format("User DN not found: %s.", dn));
        }

        // remove the specified user
        users.getUser().remove(user);

        try {
            // save the file
            save();
        } catch (Exception e) {
            throw new AuthorityAccessException(e.getMessage(), e);
        }
    }

    @Override
    public void setUsersGroup(Set<String> dns, String group) throws UnknownIdentityException, AuthorityAccessException {
        final Collection<User> groupedUsers = new HashSet<>();

        // get the specified users
        for (final String dn : dns) {
            // get the user
            final User user = getUser(dn);

            // ensure the user was located
            if (user == null) {
                throw new UnknownIdentityException(String.format("User DN not found: %s.", dn));
            }

            groupedUsers.add(user);
        }

        // update each user group
        for (final User user : groupedUsers) {
            user.setGroup(group);
        }

        try {
            // save the file
            save();
        } catch (Exception e) {
            throw new AuthorityAccessException(e.getMessage(), e);
        }
    }

    @Override
    public void ungroupUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
        // get the user
        final User user = getUser(dn);

        // ensure the user was located
        if (user == null) {
            throw new UnknownIdentityException(String.format("User DN not found: %s.", dn));
        }

        // remove the users group
        user.setGroup(null);

        try {
            // save the file
            save();
        } catch (Exception e) {
            throw new AuthorityAccessException(e.getMessage(), e);
        }
    }

    @Override
    public void ungroup(String group) throws AuthorityAccessException {
        // get the user group
        final Collection<User> userGroup = getUserGroup(group);

        // ensure the user group was located
        if (userGroup == null) {
            return;
        }

        // update each user group
        for (final User user : userGroup) {
            user.setGroup(null);
        }

        try {
            // save the file
            save();
        } catch (Exception e) {
            throw new AuthorityAccessException(e.getMessage(), e);
        }
    }

    @Override
    public String getGroupForUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
        // get the user
        final User user = getUser(dn);

        // ensure the user was located
        if (user == null) {
            throw new UnknownIdentityException(String.format("User DN not found: %s.", dn));
        }

        return user.getGroup();
    }

    @Override
    public void revokeGroup(String group) throws UnknownIdentityException, AuthorityAccessException {
        // get the user group
        final Collection<User> userGroup = getUserGroup(group);

        // ensure the user group was located
        if (userGroup == null) {
            throw new UnknownIdentityException(String.format("User group not found: %s.", group));
        }

        // remove each user in the group
        for (final User user : userGroup) {
            users.getUser().remove(user);
        }

        try {
            // save the file
            save();
        } catch (Exception e) {
            throw new AuthorityAccessException(e.getMessage(), e);
        }
    }

    /**
     * Locates the user with the specified DN.
     *
     * @param dn
     * @return
     */
    private User getUser(String dn) throws UnknownIdentityException {
        // ensure the DN was specified
        if (dn == null) {
            throw new UnknownIdentityException("User DN not specified.");
        }

        // attempt to get the user and ensure it was located
        User desiredUser = null;
        for (final User user : users.getUser()) {
            if (dn.equalsIgnoreCase(user.getDn())) {
                desiredUser = user;
                break;
            }
        }

        return desiredUser;
    }

    /**
     * Locates all users that are part of the specified group.
     *
     * @param group
     * @return
     * @throws UnknownIdentityException
     */
    private Collection<User> getUserGroup(String group) throws UnknownIdentityException {
        // ensure the DN was specified
        if (group == null) {
            throw new UnknownIdentityException("User group not specified.");
        }

        // get all users with this group
        Collection<User> userGroup = null;
        for (final User user : users.getUser()) {
            if (group.equals(user.getGroup())) {
                if (userGroup == null) {
                    userGroup = new HashSet<>();
                }
                userGroup.add(user);
            }
        }

        return userGroup;
    }

    /**
     * Saves the users file.
     *
     * @throws Exception
     */
    private void save() throws Exception {
        final Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        // save users to restore directory before primary directory
        if (restoreUsersFile != null) {
            marshaller.marshal(users, restoreUsersFile);
        }

        // save users to primary directory
        marshaller.marshal(users, usersFile);
    }

    @AuthorityProviderContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
