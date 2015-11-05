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
package org.apache.nifi.authorized.users;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.user.generated.LoginUser;
import org.apache.nifi.user.generated.NiFiUser;
import org.apache.nifi.user.generated.User;
import org.apache.nifi.user.generated.Users;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Access to the configured Authorized Users.
 */
public final class AuthorizedUsers {

    private static final Logger logger = LoggerFactory.getLogger(AuthorizedUsers.class);

    private static final String USERS_XSD = "/users.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.user.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    private static final Map<String, AuthorizedUsers> instances = new HashMap<>();

    private File usersFile;
    private File restoreFile;

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, AuthorizedUsers.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private AuthorizedUsers(final File usersFile, final NiFiProperties properties) throws IOException, IllegalStateException {
        this.usersFile = usersFile;

        // the restore directory is optional and may be null
        final File restoreDirectory = properties.getRestoreDirectory();
        if (restoreDirectory != null) {

            // sanity check that restore directory is a directory, creating it if necessary
            FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

            // check that restore directory is not the same as the primary directory
            final File usersFileDirectory = usersFile.getParentFile();
            if (usersFileDirectory.getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
                throw new IllegalStateException(String.format("Directory of users file '%s' is the same as restore directory '%s' ",
                        usersFileDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
            }

            // the restore copy will have same file name, but reside in a different directory
            restoreFile = new File(restoreDirectory, usersFile.getName());

            // sync the primary copy with the restore copy
            try {
                FileUtils.syncWithRestore(usersFile, restoreFile, logger);
            } catch (final IOException | IllegalStateException ioe) {
                throw new ProviderCreationException(ioe);
            }
        }
    }

    public static AuthorizedUsers getInstance(final String usersFilePath, final NiFiProperties properties) throws IOException, IllegalStateException {
        final File usersFile = new File(usersFilePath);

        // see if an authorizedUsers has already been created using this filename
        AuthorizedUsers authorizedUsers = instances.get(usersFile.getName());

        if (authorizedUsers == null) {
            // create a new authorizedUsers
            authorizedUsers = new AuthorizedUsers(usersFile, properties);

            // store it for later
            instances.put(usersFile.getName(), authorizedUsers);
        } else {
            // ensure the file paths are the same, the restore capability cannot support different files with the same name
            if (!authorizedUsers.usersFile.equals(usersFile)) {
                throw new IllegalStateException(String.format("A users file with this name has already been initialized. The name must be unique given the constraints of "
                        + "the restore directory. The paths in question are '%s' and '%s'", authorizedUsers.usersFile.getAbsolutePath(), usersFile.getAbsolutePath()));
            }
        }

        return authorizedUsers;
    }

    public String getUserIdentity(final NiFiUser user) {
        if (User.class.isAssignableFrom(user.getClass())) {
            return ((User) user).getDn();
        } else {
            return ((LoginUser) user).getUsername();
        }
    }

    public synchronized Users getUsers() {
        try {
            // ensure the directory exists and it can be created
            if (!usersFile.exists() && !usersFile.mkdirs()) {
                throw new IllegalStateException("The users file does not exist and could not be created.");
            }

            // find the schema
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            final Schema schema = schemaFactory.newSchema(AuthorizedUsers.class.getResource(USERS_XSD));

            // attempt to unmarshal
            final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
            unmarshaller.setSchema(schema);
            final JAXBElement<Users> element = unmarshaller.unmarshal(new StreamSource(usersFile), Users.class);
            return element.getValue();
        } catch (SAXException | JAXBException e) {
            throw new AuthorityAccessException(e.getMessage(), e);
        }
    }

    public synchronized boolean hasUser(final HasUser finder) {
        // load the users
        final Users users = getUsers();

        // combine the user lists
        final List<NiFiUser> nifiUsers = new ArrayList<>();
        nifiUsers.addAll(users.getUser());
        nifiUsers.addAll(users.getLoginUser());

        // find the desired user
        return finder.hasUser(nifiUsers);
    }

    public synchronized NiFiUser getUser(final FindUser finder) {
        // load the users
        final Users users = getUsers();

        // combine the user lists
        final List<NiFiUser> nifiUsers = new ArrayList<>();
        nifiUsers.addAll(users.getUser());
        nifiUsers.addAll(users.getLoginUser());

        // find the desired user
        return finder.findUser(nifiUsers);
    }

    public synchronized List<NiFiUser> getUsers(final FindUsers finder) {
        // load the users
        final Users users = getUsers();

        // combine the user lists
        final List<NiFiUser> nifiUsers = new ArrayList<>();
        nifiUsers.addAll(users.getUser());
        nifiUsers.addAll(users.getLoginUser());

        // find the desired user
        return finder.findUsers(nifiUsers);
    }

    public synchronized void createUser(final CreateUser creator) {
        // add the user
        final Users users = getUsers();

        // create the user
        final NiFiUser newUser = creator.createUser();
        if (User.class.isAssignableFrom(newUser.getClass())) {
            users.getUser().add((User) newUser);
        } else {
            users.getLoginUser().add((LoginUser) newUser);
        }

        // save the users
        saveUsers(users);
    }

    public synchronized void createOrUpdateUser(final FindUser finder, final CreateUser creator, final UpdateUser updater) {
        try {
            updateUser(finder, updater);
        } catch (final UnknownIdentityException uie) {
            createUser(creator);
        }
    }

    public synchronized void updateUser(final FindUser finder, final UpdateUser updater) {
        // update the user
        final Users users = getUsers();

        // combine the user lists
        final List<NiFiUser> nifiUsers = new ArrayList<>();
        nifiUsers.addAll(users.getUser());
        nifiUsers.addAll(users.getLoginUser());

        // find the user to update
        final NiFiUser user = finder.findUser(nifiUsers);

        // update the user
        updater.updateUser(user);

        // save the users
        saveUsers(users);
    }

    public synchronized void updateUsers(final FindUsers finder, final UpdateUsers updater) {
        // update the user
        final Users users = getUsers();

        // combine the user lists
        final List<NiFiUser> nifiUsers = new ArrayList<>();
        nifiUsers.addAll(users.getUser());
        nifiUsers.addAll(users.getLoginUser());

        final List<NiFiUser> userToUpdate = finder.findUsers(nifiUsers);

        // update the user
        updater.updateUsers(userToUpdate);

        // save the users
        saveUsers(users);
    }

    public synchronized Users removeUser(final FindUser finder) {
        // load the users
        final Users users = getUsers();

        // combine the user lists
        final List<NiFiUser> nifiUsers = new ArrayList<>();
        nifiUsers.addAll(users.getUser());
        nifiUsers.addAll(users.getLoginUser());

        // find the desired user
        final NiFiUser user = finder.findUser(nifiUsers);
        if (User.class.isAssignableFrom(user.getClass())) {
            users.getUser().remove((User) user);
        } else {
            users.getLoginUser().remove((LoginUser) user);
        }

        // save the users
        saveUsers(users);

        return users;
    }

    public synchronized Users removeUsers(final FindUsers finder) {
        // load the users
        final Users users = getUsers();

        // combine the user lists
        final List<NiFiUser> nifiUsers = new ArrayList<>();
        nifiUsers.addAll(users.getUser());
        nifiUsers.addAll(users.getLoginUser());

        // find the desired user
        final List<NiFiUser> usersToRemove = finder.findUsers(nifiUsers);
        for (final NiFiUser user : usersToRemove) {
            if (User.class.isAssignableFrom(user.getClass())) {
                users.getUser().remove((User) user);
            } else {
                users.getLoginUser().remove((LoginUser) user);
            }
        }

        // save the users
        saveUsers(users);

        return users;
    }

    private synchronized void saveUsers(final Users users) {
        try {
            final Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            // save users to restore directory before primary directory
            if (restoreFile != null) {
                marshaller.marshal(users, restoreFile);
            }

            // save users to primary directory
            marshaller.marshal(users, usersFile);
        } catch (JAXBException e) {
            throw new AuthorityAccessException(e.getMessage(), e);
        }
    }

    public static interface HasUser {

        /**
         * Determines if a user exists. Returns whether this user exists and will never through an UnknownIdentityException.
         *
         * @param users the users
         * @return whether the desired user exists
         */
        boolean hasUser(List<NiFiUser> users);
    }

    public static interface FindUser {

        /**
         * Finds the desired user. If the user cannot be found throws an UnknownIdentityException. Never returns null.
         *
         * @param users the users
         * @return the desired user
         * @throws UnknownIdentityException if the user cannot be found
         */
        NiFiUser findUser(List<NiFiUser> users) throws UnknownIdentityException;
    }

    public static interface FindUsers {

        /**
         * Finds the specified users.
         *
         * @param users the userss
         * @return the desired users
         * @throws UnknownIdentityException if the users cannot be found
         */
        List<NiFiUser> findUsers(List<NiFiUser> users) throws UnknownIdentityException;
    }

    public static interface CreateUser {

        /**
         * Creates the user to add.
         *
         * @return the users to add
         */
        NiFiUser createUser();
    }

    public static interface UpdateUser {

        /**
         * Updates the specified user.
         *
         * @param user the user
         */
        void updateUser(NiFiUser user);
    }

    public static interface UpdateUsers {

        /**
         * Updates the specified users.
         *
         * @param users the users to update
         */
        void updateUsers(List<NiFiUser> users);
    }
}
