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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.authorization.file.tenants.generated.Groups;
import org.apache.nifi.authorization.file.tenants.generated.Tenants;
import org.apache.nifi.authorization.file.tenants.generated.Users;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.security.xml.XmlUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class FileUserGroupProvider implements ConfigurableUserGroupProvider {

    private static final Logger logger = LoggerFactory.getLogger(FileUserGroupProvider.class);

    private static final String TENANTS_XSD = "/tenants.xsd";
    private static final String JAXB_TENANTS_PATH = "org.apache.nifi.authorization.file.tenants.generated";

    private static final String USERS_XSD = "/legacy-users.xsd";
    private static final String JAXB_USERS_PATH = "org.apache.nifi.user.generated";

    private static final JAXBContext JAXB_TENANTS_CONTEXT = initializeJaxbContext(JAXB_TENANTS_PATH);
    private static final JAXBContext JAXB_USERS_CONTEXT = initializeJaxbContext(JAXB_USERS_PATH);

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext(final String contextPath) {
        try {
            return JAXBContext.newInstance(contextPath, FileAuthorizer.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private static final DocumentBuilderFactory DOCUMENT_BUILDER_FACTORY = DocumentBuilderFactory.newInstance();
    private static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newInstance();

    private static final String USER_ELEMENT = "user";
    private static final String GROUP_USER_ELEMENT = "groupUser";
    private static final String GROUP_ELEMENT = "group";
    private static final String IDENTIFIER_ATTR = "identifier";
    private static final String IDENTITY_ATTR = "identity";
    private static final String NAME_ATTR = "name";

    static final String PROP_INITIAL_USER_IDENTITY_PREFIX = "Initial User Identity ";
    static final String PROP_TENANTS_FILE = "Users File";
    static final Pattern INITIAL_USER_IDENTITY_PATTERN = Pattern.compile(PROP_INITIAL_USER_IDENTITY_PREFIX + "\\S+");

    private Schema usersSchema;
    private Schema tenantsSchema;
    private NiFiProperties properties;
    private File tenantsFile;
    private File restoreTenantsFile;
    private String legacyAuthorizedUsersFile;
    private Set<String> initialUserIdentities;
    private List<IdentityMapping> identityMappings;

    private final AtomicReference<UserGroupHolder> userGroupHolder = new AtomicReference<>();

    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
        try {
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            tenantsSchema = schemaFactory.newSchema(FileAuthorizer.class.getResource(TENANTS_XSD));
            usersSchema = schemaFactory.newSchema(FileAuthorizer.class.getResource(USERS_XSD));
        } catch (Exception e) {
            throw new AuthorizerCreationException(e);
        }
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        try {
            final PropertyValue tenantsPath = configurationContext.getProperty(PROP_TENANTS_FILE);
            if (StringUtils.isBlank(tenantsPath.getValue())) {
                throw new AuthorizerCreationException("The users file must be specified.");
            }

            // get the tenants file and ensure it exists
            tenantsFile = new File(tenantsPath.getValue());
            if (!tenantsFile.exists()) {
                logger.info("Creating new users file at {}", new Object[] {tenantsFile.getAbsolutePath()});
                saveTenants(new Tenants());
            }

            final File tenantsFileDirectory = tenantsFile.getAbsoluteFile().getParentFile();

            // the restore directory is optional and may be null
            final File restoreDirectory = properties.getRestoreDirectory();
            if (restoreDirectory != null) {
                // sanity check that restore directory is a directory, creating it if necessary
                FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

                // check that restore directory is not the same as the user's directory
                if (tenantsFileDirectory.getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
                    throw new AuthorizerCreationException(String.format("Users file directory '%s' is the same as restore directory '%s' ",
                            tenantsFileDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
                }

                // the restore copy will have same file name, but reside in a different directory
                restoreTenantsFile = new File(restoreDirectory, tenantsFile.getName());

                try {
                    // sync the primary copy with the restore copy
                    FileUtils.syncWithRestore(tenantsFile, restoreTenantsFile, logger);
                } catch (final IOException | IllegalStateException ioe) {
                    throw new AuthorizerCreationException(ioe);
                }
            }

            // extract the identity mappings from nifi.properties if any are provided
            identityMappings = Collections.unmodifiableList(IdentityMappingUtil.getIdentityMappings(properties));

            // get the value of the legacy authorized users file
            final PropertyValue legacyAuthorizedUsersProp = configurationContext.getProperty(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE);
            legacyAuthorizedUsersFile = legacyAuthorizedUsersProp.isSet() ? legacyAuthorizedUsersProp.getValue() : null;

            // extract any node identities
            initialUserIdentities = new HashSet<>();
            for (Map.Entry<String,String> entry : configurationContext.getProperties().entrySet()) {
                Matcher matcher = INITIAL_USER_IDENTITY_PATTERN.matcher(entry.getKey());
                if (matcher.matches() && !StringUtils.isBlank(entry.getValue())) {
                    initialUserIdentities.add(IdentityMappingUtil.mapIdentity(entry.getValue(), identityMappings));
                }
            }

            load();

            // if we've copied the authorizations file to a restore directory synchronize it
            if (restoreTenantsFile != null) {
                FileUtils.copyFile(tenantsFile, restoreTenantsFile, false, false, logger);
            }

            logger.info(String.format("Users/Groups file loaded at %s", new Date().toString()));
        } catch (IOException | AuthorizerCreationException | JAXBException | IllegalStateException | SAXException e) {
            throw new AuthorizerCreationException(e);
        }
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return userGroupHolder.get().getAllUsers();
    }

    @Override
    public synchronized User addUser(User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final org.apache.nifi.authorization.file.tenants.generated.User jaxbUser = createJAXBUser(user);

        final UserGroupHolder holder = userGroupHolder.get();
        final Tenants tenants = holder.getTenants();
        tenants.getUsers().getUser().add(jaxbUser);

        saveAndRefreshHolder(tenants);

        return userGroupHolder.get().getUsersById().get(user.getIdentifier());
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }

        final UserGroupHolder holder = userGroupHolder.get();
        return holder.getUsersById().get(identifier);
    }

    @Override
    public synchronized User updateUser(User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final UserGroupHolder holder = userGroupHolder.get();
        final Tenants tenants = holder.getTenants();

        final List<org.apache.nifi.authorization.file.tenants.generated.User> users = tenants.getUsers().getUser();

        // fine the User that needs to be updated
        org.apache.nifi.authorization.file.tenants.generated.User updateUser = null;
        for (org.apache.nifi.authorization.file.tenants.generated.User jaxbUser : users) {
            if (user.getIdentifier().equals(jaxbUser.getIdentifier())) {
                updateUser = jaxbUser;
                break;
            }
        }

        // if user wasn't found return null, otherwise update the user and save changes
        if (updateUser == null) {
            return null;
        } else {
            updateUser.setIdentity(user.getIdentity());
            saveAndRefreshHolder(tenants);

            return userGroupHolder.get().getUsersById().get(user.getIdentifier());
        }
    }

    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        if (identity == null) {
            return null;
        }

        final UserGroupHolder holder = userGroupHolder.get();
        return holder.getUsersByIdentity().get(identity);
    }

    @Override
    public synchronized User deleteUser(User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final UserGroupHolder holder = userGroupHolder.get();
        final Tenants tenants = holder.getTenants();

        final List<org.apache.nifi.authorization.file.tenants.generated.User> users = tenants.getUsers().getUser();

        // for each group iterate over the user references and remove the user reference if it matches the user being deleted
        for (org.apache.nifi.authorization.file.tenants.generated.Group group : tenants.getGroups().getGroup()) {
            Iterator<org.apache.nifi.authorization.file.tenants.generated.Group.User> groupUserIter = group.getUser().iterator();
            while (groupUserIter.hasNext()) {
                org.apache.nifi.authorization.file.tenants.generated.Group.User groupUser = groupUserIter.next();
                if (groupUser.getIdentifier().equals(user.getIdentifier())) {
                    groupUserIter.remove();
                    break;
                }
            }
        }

        // remove the actual user if it exists
        boolean removedUser = false;
        Iterator<org.apache.nifi.authorization.file.tenants.generated.User> iter = users.iterator();
        while (iter.hasNext()) {
            org.apache.nifi.authorization.file.tenants.generated.User jaxbUser = iter.next();
            if (user.getIdentifier().equals(jaxbUser.getIdentifier())) {
                iter.remove();
                removedUser = true;
                break;
            }
        }

        if (removedUser) {
            saveAndRefreshHolder(tenants);
            return user;
        } else {
            return null;
        }
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return userGroupHolder.get().getAllGroups();
    }

    @Override
    public synchronized Group addGroup(Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        final UserGroupHolder holder = userGroupHolder.get();
        final Tenants tenants = holder.getTenants();

        // determine that all users in the group exist before doing anything, throw an exception if they don't
        checkGroupUsers(group, tenants.getUsers().getUser());

        // create a new JAXB Group based on the incoming Group
        final org.apache.nifi.authorization.file.tenants.generated.Group jaxbGroup = new org.apache.nifi.authorization.file.tenants.generated.Group();
        jaxbGroup.setIdentifier(group.getIdentifier());
        jaxbGroup.setName(group.getName());

        // add each user to the group
        for (String groupUser : group.getUsers()) {
            org.apache.nifi.authorization.file.tenants.generated.Group.User jaxbGroupUser = new org.apache.nifi.authorization.file.tenants.generated.Group.User();
            jaxbGroupUser.setIdentifier(groupUser);
            jaxbGroup.getUser().add(jaxbGroupUser);
        }

        tenants.getGroups().getGroup().add(jaxbGroup);
        saveAndRefreshHolder(tenants);

        return userGroupHolder.get().getGroupsById().get(group.getIdentifier());
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }
        return userGroupHolder.get().getGroupsById().get(identifier);
    }

    @Override
    public UserAndGroups getUserAndGroups(final String identity) throws AuthorizationAccessException {
        final UserGroupHolder holder = userGroupHolder.get();
        final User user = holder.getUser(identity);
        final Set<Group> groups = holder.getGroups(identity);

        return new UserAndGroups() {
            @Override
            public User getUser() {
                return user;
            }

            @Override
            public Set<Group> getGroups() {
                return groups;
            }
        };
    }

    @Override
    public synchronized Group updateGroup(Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        final UserGroupHolder holder = userGroupHolder.get();
        final Tenants tenants = holder.getTenants();

        // find the group that needs to be update
        org.apache.nifi.authorization.file.tenants.generated.Group updateGroup = null;
        for (org.apache.nifi.authorization.file.tenants.generated.Group jaxbGroup : tenants.getGroups().getGroup()) {
            if (jaxbGroup.getIdentifier().equals(group.getIdentifier())) {
                updateGroup = jaxbGroup;
                break;
            }
        }

        // if the group wasn't found return null, otherwise update the group and save changes
        if (updateGroup == null) {
            return null;
        }

        // reset the list of users and add each user to the group
        updateGroup.getUser().clear();
        for (String groupUser : group.getUsers()) {
            org.apache.nifi.authorization.file.tenants.generated.Group.User jaxbGroupUser = new org.apache.nifi.authorization.file.tenants.generated.Group.User();
            jaxbGroupUser.setIdentifier(groupUser);
            updateGroup.getUser().add(jaxbGroupUser);
        }

        updateGroup.setName(group.getName());
        saveAndRefreshHolder(tenants);

        return userGroupHolder.get().getGroupsById().get(group.getIdentifier());
    }

    @Override
    public synchronized Group deleteGroup(Group group) throws AuthorizationAccessException {
        final UserGroupHolder holder = userGroupHolder.get();
        final Tenants tenants = holder.getTenants();

        final List<org.apache.nifi.authorization.file.tenants.generated.Group> groups = tenants.getGroups().getGroup();

        // now remove the actual group from the top-level list of groups
        boolean removedGroup = false;
        Iterator<org.apache.nifi.authorization.file.tenants.generated.Group> iter = groups.iterator();
        while (iter.hasNext()) {
            org.apache.nifi.authorization.file.tenants.generated.Group jaxbGroup = iter.next();
            if (group.getIdentifier().equals(jaxbGroup.getIdentifier())) {
                iter.remove();
                removedGroup = true;
                break;
            }
        }

        if (removedGroup) {
            saveAndRefreshHolder(tenants);
            return group;
        } else {
            return null;
        }
    }

    UserGroupHolder getUserGroupHolder() {
        return userGroupHolder.get();
    }

    @AuthorizerContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public synchronized void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
        final UsersAndGroups usersAndGroups = parseUsersAndGroups(fingerprint);
        usersAndGroups.getUsers().forEach(user -> addUser(user));
        usersAndGroups.getGroups().forEach(group -> addGroup(group));
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException {
        try {
            // ensure we understand the proposed fingerprint
            parseUsersAndGroups(proposedFingerprint);
        } catch (final AuthorizationAccessException e) {
            throw new UninheritableAuthorizationsException("Unable to parse the proposed fingerprint: " + e);
        }

        final UserGroupHolder usersAndGroups = userGroupHolder.get();

        // ensure we are in a proper state to inherit the fingerprint
        if (!usersAndGroups.getAllUsers().isEmpty() || !usersAndGroups.getAllGroups().isEmpty()) {
            throw new UninheritableAuthorizationsException("Proposed fingerprint is not inheritable because the current users and groups is not empty.");
        }
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        final UserGroupHolder usersAndGroups = userGroupHolder.get();

        final List<User> users = new ArrayList<>(usersAndGroups.getAllUsers());
        Collections.sort(users, Comparator.comparing(User::getIdentifier));

        final List<Group> groups = new ArrayList<>(usersAndGroups.getAllGroups());
        Collections.sort(groups, Comparator.comparing(Group::getIdentifier));

        XMLStreamWriter writer = null;
        final StringWriter out = new StringWriter();
        try {
            writer = XML_OUTPUT_FACTORY.createXMLStreamWriter(out);
            writer.writeStartDocument();
            writer.writeStartElement("tenants");

            for (User user : users) {
                writeUser(writer, user);
            }
            for (Group group : groups) {
                writeGroup(writer, group);
            }

            writer.writeEndElement();
            writer.writeEndDocument();
            writer.flush();
        } catch (XMLStreamException e) {
            throw new AuthorizationAccessException("Unable to generate fingerprint", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (XMLStreamException e) {
                    // nothing to do here
                }
            }
        }

        return out.toString();
    }

    private UsersAndGroups parseUsersAndGroups(final String fingerprint) {
        final List<User> users = new ArrayList<>();
        final List<Group> groups = new ArrayList<>();

        final byte[] fingerprintBytes = fingerprint.getBytes(StandardCharsets.UTF_8);
        try (final ByteArrayInputStream in = new ByteArrayInputStream(fingerprintBytes)) {
            final DocumentBuilder docBuilder = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
            final Document document = docBuilder.parse(in);
            final Element rootElement = document.getDocumentElement();

            // parse all the users and add them to the current user group provider
            NodeList userNodes = rootElement.getElementsByTagName(USER_ELEMENT);
            for (int i=0; i < userNodes.getLength(); i++) {
                Node userNode = userNodes.item(i);
                users.add(parseUser((Element) userNode));
            }

            // parse all the groups and add them to the current user group provider
            NodeList groupNodes = rootElement.getElementsByTagName(GROUP_ELEMENT);
            for (int i=0; i < groupNodes.getLength(); i++) {
                Node groupNode = groupNodes.item(i);
                groups.add(parseGroup((Element) groupNode));
            }
        } catch (SAXException | ParserConfigurationException | IOException e) {
            throw new AuthorizationAccessException("Unable to parse fingerprint", e);
        }

        return new UsersAndGroups(users, groups);
    }

    private User parseUser(final Element element) {
        final User.Builder builder = new User.Builder()
                .identifier(element.getAttribute(IDENTIFIER_ATTR))
                .identity(element.getAttribute(IDENTITY_ATTR));

        return builder.build();
    }

    private Group parseGroup(final Element element) {
        final Group.Builder builder = new Group.Builder()
                .identifier(element.getAttribute(IDENTIFIER_ATTR))
                .name(element.getAttribute(NAME_ATTR));

        NodeList groupUsers = element.getElementsByTagName(GROUP_USER_ELEMENT);
        for (int i=0; i < groupUsers.getLength(); i++) {
            Element groupUserNode = (Element) groupUsers.item(i);
            builder.addUser(groupUserNode.getAttribute(IDENTIFIER_ATTR));
        }

        return builder.build();
    }

    private void writeUser(final XMLStreamWriter writer, final User user) throws XMLStreamException {
        writer.writeStartElement(USER_ELEMENT);
        writer.writeAttribute(IDENTIFIER_ATTR, user.getIdentifier());
        writer.writeAttribute(IDENTITY_ATTR, user.getIdentity());
        writer.writeEndElement();
    }

    private void writeGroup(final XMLStreamWriter writer, final Group group) throws XMLStreamException {
        List<String> users = new ArrayList<>(group.getUsers());
        Collections.sort(users);

        writer.writeStartElement(GROUP_ELEMENT);
        writer.writeAttribute(IDENTIFIER_ATTR, group.getIdentifier());
        writer.writeAttribute(NAME_ATTR, group.getName());

        for (String user : users) {
            writer.writeStartElement(GROUP_USER_ELEMENT);
            writer.writeAttribute(IDENTIFIER_ATTR, user);
            writer.writeEndElement();
        }

        writer.writeEndElement();
    }

    private org.apache.nifi.authorization.file.tenants.generated.User createJAXBUser(User user) {
        final org.apache.nifi.authorization.file.tenants.generated.User jaxbUser = new org.apache.nifi.authorization.file.tenants.generated.User();
        jaxbUser.setIdentifier(user.getIdentifier());
        jaxbUser.setIdentity(user.getIdentity());
        return jaxbUser;
    }

    private Set<org.apache.nifi.authorization.file.tenants.generated.User> checkGroupUsers(final Group group, final List<org.apache.nifi.authorization.file.tenants.generated.User> users) {
        final Set<org.apache.nifi.authorization.file.tenants.generated.User> jaxbUsers = new HashSet<>();
        for (String groupUser : group.getUsers()) {
            boolean found = false;
            for (org.apache.nifi.authorization.file.tenants.generated.User jaxbUser : users) {
                if (jaxbUser.getIdentifier().equals(groupUser)) {
                    jaxbUsers.add(jaxbUser);
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new IllegalStateException("Unable to add group because user " + groupUser + " does not exist");
            }
        }
        return jaxbUsers;
    }

    /**
     * Loads the authorizations file and populates the AuthorizationsHolder, only called during start-up.
     *
     * @throws JAXBException            Unable to reload the authorized users file
     * @throws IllegalStateException    Unable to sync file with restore
     * @throws SAXException             Unable to unmarshall tenants
     */
    private synchronized void load() throws JAXBException, IllegalStateException, SAXException {
        final Tenants tenants = unmarshallTenants();
        if (tenants.getUsers() == null) {
            tenants.setUsers(new Users());
        }
        if (tenants.getGroups() == null) {
            tenants.setGroups(new Groups());
        }

        final UserGroupHolder userGroupHolder = new UserGroupHolder(tenants);
        final boolean emptyTenants = userGroupHolder.getAllUsers().isEmpty() && userGroupHolder.getAllGroups().isEmpty();
        final boolean hasLegacyAuthorizedUsers = (legacyAuthorizedUsersFile != null && !StringUtils.isBlank(legacyAuthorizedUsersFile));

        if (emptyTenants) {
            if (hasLegacyAuthorizedUsers) {
                logger.info("Loading users from legacy model " + legacyAuthorizedUsersFile + " into new users file.");
                convertLegacyAuthorizedUsers(tenants);
            }

            populateInitialUsers(tenants);

            // save any changes that were made and repopulate the holder
            saveAndRefreshHolder(tenants);
        } else {
            this.userGroupHolder.set(userGroupHolder);
        }
    }

    private void saveTenants(final Tenants tenants) throws JAXBException {
        final Marshaller marshaller = JAXB_TENANTS_CONTEXT.createMarshaller();
        marshaller.setSchema(tenantsSchema);
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(tenants, tenantsFile);
    }

    private Tenants unmarshallTenants() throws JAXBException {
        final Unmarshaller unmarshaller = JAXB_TENANTS_CONTEXT.createUnmarshaller();
        unmarshaller.setSchema(tenantsSchema);

        try {
            final XMLStreamReader xsr = XmlUtils.createSafeReader(new StreamSource(tenantsFile));
            final JAXBElement<Tenants> element = unmarshaller.unmarshal(xsr, Tenants.class);
            return element.getValue();
        } catch (XMLStreamException e) {
            throw new JAXBException("Error unmarshalling tenants", e);
        }
    }

    private void populateInitialUsers(final Tenants tenants) {
        for (String initialUserIdentity : initialUserIdentities) {
            getOrCreateUser(tenants, initialUserIdentity);
        }
    }

    /**
     * Unmarshalls an existing authorized-users.xml and converts the object model to the new model.
     *
     * @param tenants the current Tenants instance users and groups will be added to
     * @throws AuthorizerCreationException if the legacy authorized users file that was provided does not exist
     * @throws JAXBException if the legacy authorized users file that was provided could not be unmarshalled
     */
    private void convertLegacyAuthorizedUsers(final Tenants tenants) throws AuthorizerCreationException, JAXBException {
        final File authorizedUsersFile = new File(legacyAuthorizedUsersFile);
        if (!authorizedUsersFile.exists()) {
            throw new AuthorizerCreationException("Legacy Authorized Users File '" + legacyAuthorizedUsersFile + "' does not exists");
        }

        XMLStreamReader xsr;
        try {
            xsr = XmlUtils.createSafeReader(new StreamSource(authorizedUsersFile));
        } catch (XMLStreamException e) {
            throw new AuthorizerCreationException("Error converting the legacy authorizers file", e);
        }

        final Unmarshaller unmarshaller = JAXB_USERS_CONTEXT.createUnmarshaller();
        unmarshaller.setSchema(usersSchema);

        final JAXBElement<org.apache.nifi.user.generated.Users> element = unmarshaller.unmarshal(
                xsr, org.apache.nifi.user.generated.Users.class);

        final org.apache.nifi.user.generated.Users users = element.getValue();
        if (users.getUser().isEmpty()) {
            logger.info("Legacy Authorized Users File contained no users, nothing to convert");
            return;
        }

        for (org.apache.nifi.user.generated.User legacyUser : users.getUser()) {
            // create the identifier of the new user based on the DN
            final String legacyUserDn = IdentityMappingUtil.mapIdentity(legacyUser.getDn(), identityMappings);
            org.apache.nifi.authorization.file.tenants.generated.User user = getOrCreateUser(tenants, legacyUserDn);

            // if there was a group name find or create the group and add the user to it
            org.apache.nifi.authorization.file.tenants.generated.Group group = getOrCreateGroup(tenants, legacyUser.getGroup());
            if (group != null) {
                org.apache.nifi.authorization.file.tenants.generated.Group.User groupUser = new org.apache.nifi.authorization.file.tenants.generated.Group.User();
                groupUser.setIdentifier(user.getIdentifier());
                group.getUser().add(groupUser);
            }
        }
    }

    /**
     * Finds the User with the given identity, or creates a new one and adds it to the Tenants.
     *
     * @param tenants the Tenants reference
     * @param userIdentity the user identity to find or create
     * @return the User from Tenants with the given identity, or a new instance that was added to Tenants
     */
    private org.apache.nifi.authorization.file.tenants.generated.User getOrCreateUser(final Tenants tenants, final String userIdentity) {
        if (StringUtils.isBlank(userIdentity)) {
            return null;
        }

        org.apache.nifi.authorization.file.tenants.generated.User foundUser = null;
        for (org.apache.nifi.authorization.file.tenants.generated.User user : tenants.getUsers().getUser()) {
            if (user.getIdentity().equals(userIdentity)) {
                foundUser = user;
                break;
            }
        }

        if (foundUser == null) {
            final String userIdentifier = IdentifierUtil.getIdentifier(userIdentity);
            foundUser = new org.apache.nifi.authorization.file.tenants.generated.User();
            foundUser.setIdentifier(userIdentifier);
            foundUser.setIdentity(userIdentity);
            tenants.getUsers().getUser().add(foundUser);
        }

        return foundUser;
    }

    /**
     * Finds the Group with the given name, or creates a new one and adds it to Tenants.
     *
     * @param tenants the Tenants reference
     * @param groupName the name of the group to look for
     * @return the Group from Tenants with the given name, or a new instance that was added to Tenants
     */
    private org.apache.nifi.authorization.file.tenants.generated.Group getOrCreateGroup(final Tenants tenants, final String groupName) {
        if (StringUtils.isBlank(groupName)) {
            return null;
        }

        org.apache.nifi.authorization.file.tenants.generated.Group foundGroup = null;
        for (org.apache.nifi.authorization.file.tenants.generated.Group group : tenants.getGroups().getGroup()) {
            if (group.getName().equals(groupName)) {
                foundGroup = group;
                break;
            }
        }

        if (foundGroup == null) {
            final String newGroupIdentifier = IdentifierUtil.getIdentifier(groupName);
            foundGroup = new org.apache.nifi.authorization.file.tenants.generated.Group();
            foundGroup.setIdentifier(newGroupIdentifier);
            foundGroup.setName(groupName);
            tenants.getGroups().getGroup().add(foundGroup);
        }

        return foundGroup;
    }

    /**
     * Saves the Authorizations instance by marshalling to a file, then re-populates the
     * in-memory data structures and sets the new holder.
     *
     * Synchronized to ensure only one thread writes the file at a time.
     *
     * @param tenants the tenants to save and populate from
     * @throws AuthorizationAccessException if an error occurs saving the authorizations
     */
    private synchronized void saveAndRefreshHolder(final Tenants tenants) throws AuthorizationAccessException {
        try {
            saveTenants(tenants);

            this.userGroupHolder.set(new UserGroupHolder(tenants));
        } catch (JAXBException e) {
            throw new AuthorizationAccessException("Unable to save Authorizations", e);
        }
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
    }

    private static class UsersAndGroups {
        final List<User> users;
        final List<Group> groups;

        public UsersAndGroups(List<User> users, List<Group> groups) {
            this.users = users;
            this.groups = groups;
        }

        public List<User> getUsers() {
            return users;
        }

        public List<Group> getGroups() {
            return groups;
        }
    }
}
