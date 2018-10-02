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
import org.apache.nifi.authorization.file.generated.Authorizations;
import org.apache.nifi.authorization.file.generated.Policies;
import org.apache.nifi.authorization.file.generated.Policy;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.security.xml.XmlUtils;
import org.apache.nifi.user.generated.Users;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.api.dto.PortDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class FileAccessPolicyProvider implements ConfigurableAccessPolicyProvider {

    private static final Logger logger = LoggerFactory.getLogger(FileAccessPolicyProvider.class);

    private static final String AUTHORIZATIONS_XSD = "/authorizations.xsd";
    private static final String JAXB_AUTHORIZATIONS_PATH = "org.apache.nifi.authorization.file.generated";

    private static final String USERS_XSD = "/legacy-users.xsd";
    private static final String JAXB_USERS_PATH = "org.apache.nifi.user.generated";

    private static final JAXBContext JAXB_AUTHORIZATIONS_CONTEXT = initializeJaxbContext(JAXB_AUTHORIZATIONS_PATH);
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

    private static final String POLICY_ELEMENT = "policy";
    private static final String POLICY_USER_ELEMENT = "policyUser";
    private static final String POLICY_GROUP_ELEMENT = "policyGroup";
    private static final String IDENTIFIER_ATTR = "identifier";
    private static final String RESOURCE_ATTR = "resource";
    private static final String ACTIONS_ATTR = "actions";

    static final String READ_CODE = "R";
    static final String WRITE_CODE = "W";

    static final String PROP_NODE_IDENTITY_PREFIX = "Node Identity ";
    static final String PROP_NODE_GROUP_NAME = "Node Group";
    static final String PROP_USER_GROUP_PROVIDER = "User Group Provider";
    static final String PROP_AUTHORIZATIONS_FILE = "Authorizations File";
    static final String PROP_INITIAL_ADMIN_IDENTITY = "Initial Admin Identity";
    static final Pattern NODE_IDENTITY_PATTERN = Pattern.compile(PROP_NODE_IDENTITY_PREFIX + "\\S+");

    private Schema usersSchema;
    private Schema authorizationsSchema;
    private NiFiProperties properties;
    private File authorizationsFile;
    private File restoreAuthorizationsFile;
    private String rootGroupId;
    private String initialAdminIdentity;
    private String legacyAuthorizedUsersFile;
    private Set<String> nodeIdentities;
    private String nodeGroupIdentifier;
    private List<PortDTO> ports = new ArrayList<>();
    private List<IdentityMapping> identityMappings;
    private List<IdentityMapping> groupMappings;

    private UserGroupProvider userGroupProvider;
    private UserGroupProviderLookup userGroupProviderLookup;
    private final AtomicReference<AuthorizationsHolder> authorizationsHolder = new AtomicReference<>();

    @Override
    public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {
        userGroupProviderLookup = initializationContext.getUserGroupProviderLookup();

        try {
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            authorizationsSchema = schemaFactory.newSchema(FileAuthorizer.class.getResource(AUTHORIZATIONS_XSD));
            usersSchema = schemaFactory.newSchema(FileAuthorizer.class.getResource(USERS_XSD));
        } catch (Exception e) {
            throw new AuthorizerCreationException(e);
        }
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        try {
            final PropertyValue userGroupProviderIdentifier = configurationContext.getProperty(PROP_USER_GROUP_PROVIDER);
            if (!userGroupProviderIdentifier.isSet()) {
                throw new AuthorizerCreationException("The user group provider must be specified.");
            }

            userGroupProvider = userGroupProviderLookup.getUserGroupProvider(userGroupProviderIdentifier.getValue());
            if (userGroupProvider == null) {
                throw new AuthorizerCreationException("Unable to locate user group provider with identifier " + userGroupProviderIdentifier.getValue());
            }

            final PropertyValue authorizationsPath = configurationContext.getProperty(PROP_AUTHORIZATIONS_FILE);
            if (StringUtils.isBlank(authorizationsPath.getValue())) {
                throw new AuthorizerCreationException("The authorizations file must be specified.");
            }

            // get the authorizations file and ensure it exists
            authorizationsFile = new File(authorizationsPath.getValue());
            if (!authorizationsFile.exists()) {
                logger.info("Creating new authorizations file at {}", new Object[] {authorizationsFile.getAbsolutePath()});
                saveAuthorizations(new Authorizations());
            }

            final File authorizationsFileDirectory = authorizationsFile.getAbsoluteFile().getParentFile();

            // the restore directory is optional and may be null
            final File restoreDirectory = properties.getRestoreDirectory();
            if (restoreDirectory != null) {
                // sanity check that restore directory is a directory, creating it if necessary
                FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

                // check that restore directory is not the same as the authorizations directory
                if (authorizationsFileDirectory.getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
                    throw new AuthorizerCreationException(String.format("Authorizations file directory '%s' is the same as restore directory '%s' ",
                            authorizationsFileDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
                }

                // the restore copy will have same file name, but reside in a different directory
                restoreAuthorizationsFile = new File(restoreDirectory, authorizationsFile.getName());

                try {
                    // sync the primary copy with the restore copy
                    FileUtils.syncWithRestore(authorizationsFile, restoreAuthorizationsFile, logger);
                } catch (final IOException | IllegalStateException ioe) {
                    throw new AuthorizerCreationException(ioe);
                }
            }

            // extract the identity mappings from nifi.properties if any are provided
            identityMappings = Collections.unmodifiableList(IdentityMappingUtil.getIdentityMappings(properties));
            groupMappings = Collections.unmodifiableList(IdentityMappingUtil.getGroupMappings(properties));

            // get the value of the initial admin identity
            final PropertyValue initialAdminIdentityProp = configurationContext.getProperty(PROP_INITIAL_ADMIN_IDENTITY);
            initialAdminIdentity = initialAdminIdentityProp.isSet() ? IdentityMappingUtil.mapIdentity(initialAdminIdentityProp.getValue(), identityMappings) : null;

            // get the value of the legacy authorized users file
            final PropertyValue legacyAuthorizedUsersProp = configurationContext.getProperty(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE);
            legacyAuthorizedUsersFile = legacyAuthorizedUsersProp.isSet() ? legacyAuthorizedUsersProp.getValue() : null;

            // extract any node identities
            nodeIdentities = new HashSet<>();
            for (Map.Entry<String,String> entry : configurationContext.getProperties().entrySet()) {
                Matcher matcher = NODE_IDENTITY_PATTERN.matcher(entry.getKey());
                if (matcher.matches() && !StringUtils.isBlank(entry.getValue())) {
                    final String mappedNodeIdentity = IdentityMappingUtil.mapIdentity(entry.getValue(), identityMappings);
                    nodeIdentities.add(mappedNodeIdentity);
                    logger.info("Added mapped node {} (raw node identity {})", new Object[]{mappedNodeIdentity, entry.getValue()});
                }
            }

            // read node group name
            PropertyValue nodeGroupNameProp = configurationContext.getProperty(PROP_NODE_GROUP_NAME);
            String nodeGroupName = (nodeGroupNameProp != null && nodeGroupNameProp.isSet()) ? nodeGroupNameProp.getValue() : null;

            // look up node group identifier using node group name
            nodeGroupIdentifier = null;

            if (nodeGroupName != null) {
                if (!StringUtils.isBlank(nodeGroupName)) {
                    logger.debug("Trying to load node group '{}' from the underlying userGroupProvider", nodeGroupName);
                    for (Group group : userGroupProvider.getGroups()) {
                        if (group.getName().equals(nodeGroupName)) {
                            nodeGroupIdentifier = group.getIdentifier();
                            break;
                        }
                    }

                    if (nodeGroupIdentifier == null) {
                        throw new AuthorizerCreationException(String.format(
                            "Authorizations node group '%s' could not be found", nodeGroupName));
                    }
                } else {
                    logger.debug("Empty node group name provided");
                }
            }

            // load the authorizations
            load();

            // if we've copied the authorizations file to a restore directory synchronize it
            if (restoreAuthorizationsFile != null) {
                FileUtils.copyFile(authorizationsFile, restoreAuthorizationsFile, false, false, logger);
            }

            logger.info(String.format("Authorizations file loaded at %s", new Date().toString()));
        } catch (IOException | AuthorizerCreationException | JAXBException | IllegalStateException | SAXException e) {
            throw new AuthorizerCreationException(e);
        }
    }

    @Override
    public UserGroupProvider getUserGroupProvider() {
        return userGroupProvider;
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return authorizationsHolder.get().getAllPolicies();
    }

    @Override
    public synchronized AccessPolicy addAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        // create the new JAXB Policy
        final Policy policy = createJAXBPolicy(accessPolicy);

        // add the new Policy to the top-level list of policies
        final AuthorizationsHolder holder = authorizationsHolder.get();
        final Authorizations authorizations = holder.getAuthorizations();
        authorizations.getPolicies().getPolicy().add(policy);

        saveAndRefreshHolder(authorizations);

        return authorizationsHolder.get().getPoliciesById().get(accessPolicy.getIdentifier());
    }

    @Override
    public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getPoliciesById().get(identifier);
    }

    @Override
    public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
        return authorizationsHolder.get().getAccessPolicy(resourceIdentifier, action);
    }

    @Override
    public synchronized AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        final Authorizations authorizations = holder.getAuthorizations();

        // try to find an existing Authorization that matches the policy id
        Policy updatePolicy = null;
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            if (policy.getIdentifier().equals(accessPolicy.getIdentifier())) {
                updatePolicy = policy;
                break;
            }
        }

        // no matching Policy so return null
        if (updatePolicy == null) {
            return null;
        }

        // update the Policy, save, reload, and return
        transferUsersAndGroups(accessPolicy, updatePolicy);
        saveAndRefreshHolder(authorizations);

        return this.authorizationsHolder.get().getPoliciesById().get(accessPolicy.getIdentifier());
    }

    @Override
    public synchronized AccessPolicy deleteAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        final Authorizations authorizations = holder.getAuthorizations();

        // find the matching Policy and remove it
        boolean deletedPolicy = false;
        Iterator<Policy> policyIter = authorizations.getPolicies().getPolicy().iterator();
        while (policyIter.hasNext()) {
            final Policy policy = policyIter.next();
            if (policy.getIdentifier().equals(accessPolicy.getIdentifier())) {
                policyIter.remove();
                deletedPolicy = true;
                break;
            }
        }

        // never found a matching Policy so return null
        if (!deletedPolicy) {
            return null;
        }

        saveAndRefreshHolder(authorizations);
        return accessPolicy;
    }

    AuthorizationsHolder getAuthorizationsHolder() {
        return authorizationsHolder.get();
    }

    @AuthorizerContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public synchronized void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
        parsePolicies(fingerprint).forEach(policy -> addAccessPolicy(policy));
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
        try {
            // ensure we can understand the proposed fingerprint
            parsePolicies(proposedFingerprint);
        } catch (final AuthorizationAccessException e) {
            throw new UninheritableAuthorizationsException("Unable to parse the proposed fingerprint: " + e);
        }

        // ensure we are in a proper state to inherit the fingerprint
        if (!getAccessPolicies().isEmpty()) {
            throw new UninheritableAuthorizationsException("Proposed fingerprint is not inheritable because the current access policies is not empty.");
        }
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        final List<AccessPolicy> policies = new ArrayList<>(getAccessPolicies());
        Collections.sort(policies, Comparator.comparing(AccessPolicy::getIdentifier));

        XMLStreamWriter writer = null;
        final StringWriter out = new StringWriter();
        try {
            writer = XML_OUTPUT_FACTORY.createXMLStreamWriter(out);
            writer.writeStartDocument();
            writer.writeStartElement("accessPolicies");

            for (AccessPolicy policy : policies) {
                writePolicy(writer, policy);
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

    private List<AccessPolicy> parsePolicies(final String fingerprint) {
        final List<AccessPolicy> policies = new ArrayList<>();

        final byte[] fingerprintBytes = fingerprint.getBytes(StandardCharsets.UTF_8);
        try (final ByteArrayInputStream in = new ByteArrayInputStream(fingerprintBytes)) {
            final DocumentBuilder docBuilder = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
            final Document document = docBuilder.parse(in);
            final Element rootElement = document.getDocumentElement();

            // parse all the policies and add them to the current access policy provider
            NodeList policyNodes = rootElement.getElementsByTagName(POLICY_ELEMENT);
            for (int i = 0; i < policyNodes.getLength(); i++) {
                Node policyNode = policyNodes.item(i);
                policies.add(parsePolicy((Element) policyNode));
            }
        } catch (SAXException | ParserConfigurationException | IOException e) {
            throw new AuthorizationAccessException("Unable to parse fingerprint", e);
        }

        return policies;
    }

    private AccessPolicy parsePolicy(final Element element) {
        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier(element.getAttribute(IDENTIFIER_ATTR))
                .resource(element.getAttribute(RESOURCE_ATTR));

        final String actions = element.getAttribute(ACTIONS_ATTR);
        if (actions.equals(RequestAction.READ.name())) {
            builder.action(RequestAction.READ);
        } else if (actions.equals(RequestAction.WRITE.name())) {
            builder.action(RequestAction.WRITE);
        } else {
            throw new IllegalStateException("Unknown Policy Action: " + actions);
        }

        NodeList policyUsers = element.getElementsByTagName(POLICY_USER_ELEMENT);
        for (int i=0; i < policyUsers.getLength(); i++) {
            Element policyUserNode = (Element) policyUsers.item(i);
            builder.addUser(policyUserNode.getAttribute(IDENTIFIER_ATTR));
        }

        NodeList policyGroups = element.getElementsByTagName(POLICY_GROUP_ELEMENT);
        for (int i=0; i < policyGroups.getLength(); i++) {
            Element policyGroupNode = (Element) policyGroups.item(i);
            builder.addGroup(policyGroupNode.getAttribute(IDENTIFIER_ATTR));
        }

        return builder.build();
    }

    private void writePolicy(final XMLStreamWriter writer, final AccessPolicy policy) throws XMLStreamException {
        // sort the users for the policy
        List<String> policyUsers = new ArrayList<>(policy.getUsers());
        Collections.sort(policyUsers);

        // sort the groups for this policy
        List<String> policyGroups = new ArrayList<>(policy.getGroups());
        Collections.sort(policyGroups);

        writer.writeStartElement(POLICY_ELEMENT);
        writer.writeAttribute(IDENTIFIER_ATTR, policy.getIdentifier());
        writer.writeAttribute(RESOURCE_ATTR, policy.getResource());
        writer.writeAttribute(ACTIONS_ATTR, policy.getAction().name());

        for (String policyUser : policyUsers) {
            writer.writeStartElement(POLICY_USER_ELEMENT);
            writer.writeAttribute(IDENTIFIER_ATTR, policyUser);
            writer.writeEndElement();
        }

        for (String policyGroup : policyGroups) {
            writer.writeStartElement(POLICY_GROUP_ELEMENT);
            writer.writeAttribute(IDENTIFIER_ATTR, policyGroup);
            writer.writeEndElement();
        }

        writer.writeEndElement();
    }

    /**
     * Loads the authorizations file and populates the AuthorizationsHolder, only called during start-up.
     *
     * @throws JAXBException            Unable to reload the authorized users file
     * @throws IOException              Unable to sync file with restore
     * @throws IllegalStateException    Unable to sync file with restore
     */
    private synchronized void load() throws JAXBException, IOException, IllegalStateException, SAXException {
        // attempt to unmarshal
        final Authorizations authorizations = unmarshallAuthorizations();
        if (authorizations.getPolicies() == null) {
            authorizations.setPolicies(new Policies());
        }

        final AuthorizationsHolder authorizationsHolder = new AuthorizationsHolder(authorizations);
        final boolean emptyAuthorizations = authorizationsHolder.getAllPolicies().isEmpty();
        final boolean hasInitialAdminIdentity = (initialAdminIdentity != null && !StringUtils.isBlank(initialAdminIdentity));
        final boolean hasLegacyAuthorizedUsers = (legacyAuthorizedUsersFile != null && !StringUtils.isBlank(legacyAuthorizedUsersFile));

        // if we are starting fresh then we might need to populate an initial admin or convert legacy users
        if (emptyAuthorizations) {
            parseFlow();

            if (hasInitialAdminIdentity && hasLegacyAuthorizedUsers) {
                throw new AuthorizerCreationException("Cannot provide an Initial Admin Identity and a Legacy Authorized Users File");
            } else if (hasInitialAdminIdentity) {
                logger.info("Populating authorizations for Initial Admin: " + initialAdminIdentity);
                populateInitialAdmin(authorizations);
            } else if (hasLegacyAuthorizedUsers) {
                logger.info("Converting " + legacyAuthorizedUsersFile + " to new authorizations model");
                convertLegacyAuthorizedUsers(authorizations);
            }

            populateNodes(authorizations);

            // save any changes that were made and repopulate the holder
            saveAndRefreshHolder(authorizations);
        } else {
            this.authorizationsHolder.set(authorizationsHolder);
        }
    }

    private void saveAuthorizations(final Authorizations authorizations) throws JAXBException {
        final Marshaller marshaller = JAXB_AUTHORIZATIONS_CONTEXT.createMarshaller();
        marshaller.setSchema(authorizationsSchema);
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(authorizations, authorizationsFile);
    }

    private Authorizations unmarshallAuthorizations() throws JAXBException {
        try {
            final XMLStreamReader xsr = XmlUtils.createSafeReader(new StreamSource(authorizationsFile));
            final Unmarshaller unmarshaller = JAXB_AUTHORIZATIONS_CONTEXT.createUnmarshaller();
            unmarshaller.setSchema(authorizationsSchema);

            final JAXBElement<Authorizations> element = unmarshaller.unmarshal(xsr, Authorizations.class);
            return element.getValue();
        } catch (XMLStreamException e) {
            logger.error("Encountered an error reading authorizations file: ", e);
            throw new JAXBException("Error reading authorizations file", e);
        }
    }

    /**
     * Try to parse the flow configuration file to extract the root group id and port information.
     *
     * @throws SAXException if an error occurs creating the schema
     */
    private void parseFlow() throws SAXException {
        final FlowParser flowParser = new FlowParser();
        final FlowInfo flowInfo = flowParser.parse(properties.getFlowConfigurationFile());

        if (flowInfo != null) {
            rootGroupId = flowInfo.getRootGroupId();
            ports = flowInfo.getPorts() == null ? new ArrayList<>() : flowInfo.getPorts();
        }
    }

    /**
     *  Creates the initial admin user and policies for access the flow and managing users and policies.
     */
    private void populateInitialAdmin(final Authorizations authorizations) {
        final User initialAdmin = userGroupProvider.getUserByIdentity(initialAdminIdentity);
        if (initialAdmin == null) {
            throw new AuthorizerCreationException("Unable to locate initial admin " + initialAdminIdentity + " to seed policies");
        }

        // grant the user read access to the /flow resource
        addUserToAccessPolicy(authorizations, ResourceType.Flow.getValue(), initialAdmin.getIdentifier(), READ_CODE);

        // grant the user read access to the root process group resource
        if (rootGroupId != null) {
            addUserToAccessPolicy(authorizations, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdmin.getIdentifier(), READ_CODE);
            addUserToAccessPolicy(authorizations, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdmin.getIdentifier(), WRITE_CODE);

            addUserToAccessPolicy(authorizations, ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdmin.getIdentifier(), READ_CODE);
            addUserToAccessPolicy(authorizations, ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, initialAdmin.getIdentifier(), WRITE_CODE);
        }

        // grant the user write to restricted components
        addUserToAccessPolicy(authorizations, ResourceType.RestrictedComponents.getValue(), initialAdmin.getIdentifier(), WRITE_CODE);

        // grant the user read/write access to the /tenants resource
        addUserToAccessPolicy(authorizations, ResourceType.Tenant.getValue(), initialAdmin.getIdentifier(), READ_CODE);
        addUserToAccessPolicy(authorizations, ResourceType.Tenant.getValue(), initialAdmin.getIdentifier(), WRITE_CODE);

        // grant the user read/write access to the /policies resource
        addUserToAccessPolicy(authorizations, ResourceType.Policy.getValue(), initialAdmin.getIdentifier(), READ_CODE);
        addUserToAccessPolicy(authorizations, ResourceType.Policy.getValue(), initialAdmin.getIdentifier(), WRITE_CODE);

        // grant the user read/write access to the /controller resource
        addUserToAccessPolicy(authorizations, ResourceType.Controller.getValue(), initialAdmin.getIdentifier(), READ_CODE);
        addUserToAccessPolicy(authorizations, ResourceType.Controller.getValue(), initialAdmin.getIdentifier(), WRITE_CODE);
    }

    /**
     * Creates a user for each node and gives the nodes write permission to /proxy.
     *
     * @param authorizations the overall authorizations
     */
    private void populateNodes(Authorizations authorizations) {
        // authorize static nodes
        for (String nodeIdentity : nodeIdentities) {
            final User node = userGroupProvider.getUserByIdentity(nodeIdentity);
            if (node == null) {
                throw new AuthorizerCreationException("Unable to locate node " + nodeIdentity + " to seed policies.");
            }
            logger.debug("Populating default authorizations for node '{}' ({})", node.getIdentity(), node.getIdentifier());
            // grant access to the proxy resource
            addUserToAccessPolicy(authorizations, ResourceType.Proxy.getValue(), node.getIdentifier(), WRITE_CODE);

            // grant the user read/write access data of the root group
            if (rootGroupId != null) {
                addUserToAccessPolicy(authorizations, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, node.getIdentifier(), READ_CODE);
                addUserToAccessPolicy(authorizations, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, node.getIdentifier(), WRITE_CODE);
            }
        }

        // authorize dynamic nodes (node group)
        if (nodeGroupIdentifier != null) {
            logger.debug("Populating default authorizations for group '{}' ({})", userGroupProvider.getGroup(nodeGroupIdentifier).getName(), nodeGroupIdentifier);
            addGroupToAccessPolicy(authorizations, ResourceType.Proxy.getValue(), nodeGroupIdentifier, WRITE_CODE);

            if (rootGroupId != null) {
                addGroupToAccessPolicy(authorizations, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, nodeGroupIdentifier, READ_CODE);
                addGroupToAccessPolicy(authorizations, ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, nodeGroupIdentifier, WRITE_CODE);
            }
        }
    }

    /**
     * Unmarshalls an existing authorized-users.xml and converts the object model to the new model.
     *
     * @param authorizations the current Authorizations instance that policies will be added to
     * @throws AuthorizerCreationException if the legacy authorized users file that was provided does not exist
     * @throws JAXBException if the legacy authorized users file that was provided could not be unmarshalled
     */
    private void convertLegacyAuthorizedUsers(final Authorizations authorizations) throws AuthorizerCreationException, JAXBException {
        final File authorizedUsersFile = new File(legacyAuthorizedUsersFile);
        if (!authorizedUsersFile.exists()) {
            throw new AuthorizerCreationException("Legacy Authorized Users File '" + legacyAuthorizedUsersFile + "' does not exists");
        }

        final Unmarshaller unmarshaller = JAXB_USERS_CONTEXT.createUnmarshaller();
        unmarshaller.setSchema(usersSchema);

        final XMLStreamReader xsr;
        try {
            xsr = XmlUtils.createSafeReader(new StreamSource(authorizedUsersFile));
        } catch (XMLStreamException e) {
            logger.error("Encountered an error reading authorized users file: ", e);
            throw new JAXBException("Error reading authorized users file", e);
        }
        final JAXBElement<Users> element = unmarshaller.unmarshal(
                xsr, org.apache.nifi.user.generated.Users.class);

        final org.apache.nifi.user.generated.Users users = element.getValue();
        if (users.getUser().isEmpty()) {
            logger.info("Legacy Authorized Users File contained no users, nothing to convert");
            return;
        }

        // get all the user DNs into a list
        List<String> userIdentities = new ArrayList<>();
        for (org.apache.nifi.user.generated.User legacyUser : users.getUser()) {
            userIdentities.add(IdentityMappingUtil.mapIdentity(legacyUser.getDn(), identityMappings));
        }

        // sort the list and pull out the first identity
        Collections.sort(userIdentities);
        final String seedIdentity = userIdentities.get(0);

        // create mapping from Role to access policies
        final Map<Role,Set<RoleAccessPolicy>> roleAccessPolicies = RoleAccessPolicy.getMappings(rootGroupId);

        final List<Policy> allPolicies = new ArrayList<>();

        for (org.apache.nifi.user.generated.User legacyUser : users.getUser()) {
            // create the identifier of the new user based on the DN
            final String legacyUserDn = IdentityMappingUtil.mapIdentity(legacyUser.getDn(), identityMappings);
            final User user = userGroupProvider.getUserByIdentity(legacyUserDn);
            if (user == null) {
                throw new AuthorizerCreationException("Unable to locate legacy user " + legacyUserDn + " to seed policies.");
            }

            // create policies based on the given role
            for (org.apache.nifi.user.generated.Role jaxbRole : legacyUser.getRole()) {
                Role role = Role.valueOf(jaxbRole.getName());
                Set<RoleAccessPolicy> policies = roleAccessPolicies.get(role);

                for (RoleAccessPolicy roleAccessPolicy : policies) {

                    // get the matching policy, or create a new one
                    Policy policy = getOrCreatePolicy(
                            allPolicies,
                            seedIdentity,
                            roleAccessPolicy.getResource(),
                            roleAccessPolicy.getAction());

                    // add the user to the policy if it doesn't exist
                    addUserToPolicy(user.getIdentifier(), policy);
                }
            }

        }

        // convert any access controls on ports to the appropriate policies
        for (PortDTO portDTO : ports) {
            final Resource resource;
            if (portDTO.getType() != null && portDTO.getType().equals("inputPort")) {
                resource = ResourceFactory.getDataTransferResource(ResourceFactory.getComponentResource(ResourceType.InputPort, portDTO.getId(), portDTO.getName()));
            } else {
                resource = ResourceFactory.getDataTransferResource(ResourceFactory.getComponentResource(ResourceType.OutputPort, portDTO.getId(), portDTO.getName()));
            }

            if (portDTO.getUserAccessControl() != null) {
                for (String userAccessControl : portDTO.getUserAccessControl()) {
                    // need to perform the identity mapping on the access control so it matches the identities in the User objects
                    final String mappedUserAccessControl = IdentityMappingUtil.mapIdentity(userAccessControl, identityMappings);
                    final User foundUser = userGroupProvider.getUserByIdentity(mappedUserAccessControl);

                    // couldn't find the user matching the access control so log a warning and skip
                    if (foundUser == null) {
                        logger.warn("Found port with user access control for {} but no user exists with this identity, skipping...",
                                new Object[] {mappedUserAccessControl});
                        continue;
                    }

                    // we found the user so create the appropriate policy and add the user to it
                    Policy policy = getOrCreatePolicy(
                            allPolicies,
                            seedIdentity,
                            resource.getIdentifier(),
                            WRITE_CODE);

                    addUserToPolicy(foundUser.getIdentifier(), policy);
                }
            }

            if (portDTO.getGroupAccessControl() != null) {
                for (String groupAccessControl : portDTO.getGroupAccessControl()) {
                    final String legacyGroupName = IdentityMappingUtil.mapIdentity(groupAccessControl, groupMappings);

                    // find a group where the name is the groupAccessControl
                    Group foundGroup = null;
                    for (Group group : userGroupProvider.getGroups()) {
                        if (group.getName().equals(legacyGroupName)) {
                            foundGroup = group;
                            break;
                        }
                    }

                    // couldn't find the group matching the access control so log a warning and skip
                    if (foundGroup == null) {
                        logger.warn("Found port with group access control for {} but no group exists with this name, skipping...",
                                new Object[] {legacyGroupName});
                        continue;
                    }

                    // we found the group so create the appropriate policy and add all the users to it
                    Policy policy = getOrCreatePolicy(
                            allPolicies,
                            seedIdentity,
                            resource.getIdentifier(),
                            WRITE_CODE);

                    addGroupToPolicy(IdentifierUtil.getIdentifier(legacyGroupName), policy);
                }
            }
        }

        authorizations.getPolicies().getPolicy().addAll(allPolicies);
    }

    /**
     * Creates and adds an access policy for the given resource, identity, and actions to the specified authorizations.
     *
     * @param authorizations the Authorizations instance to add the policy to
     * @param resource the resource for the policy
     * @param userIdentifier the identifier for the user to add to the policy
     * @param action the action for the policy
     */
    private void addUserToAccessPolicy(final Authorizations authorizations, final String resource, final String userIdentifier, final String action) {
        // first try to find an existing policy for the given resource and action
        Policy foundPolicy = null;
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            if (policy.getResource().equals(resource) && policy.getAction().equals(action)) {
                foundPolicy = policy;
                break;
            }
        }

        if (foundPolicy == null) {
            // if we didn't find an existing policy create a new one
            final String uuidSeed = resource + action;

            final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                    .identifierGenerateFromSeed(uuidSeed)
                    .resource(resource)
                    .addUser(userIdentifier);

            if (action.equals(READ_CODE)) {
                builder.action(RequestAction.READ);
            } else if (action.equals(WRITE_CODE)) {
                builder.action(RequestAction.WRITE);
            } else {
                throw new IllegalStateException("Unknown Policy Action: " + action);
            }

            final AccessPolicy accessPolicy = builder.build();
            final Policy jaxbPolicy = createJAXBPolicy(accessPolicy);
            authorizations.getPolicies().getPolicy().add(jaxbPolicy);
        } else {
            // otherwise add the user to the existing policy
            Policy.User policyUser = new Policy.User();
            policyUser.setIdentifier(userIdentifier);
            foundPolicy.getUser().add(policyUser);
        }
    }

    /**
     * Creates and adds an access policy for the given resource, group identity, and actions to the specified authorizations.
     *
     * @param authorizations the Authorizations instance to add the policy to
     * @param resource       the resource for the policy
     * @param groupIdentifier the identifier for the group to add to the policy
     * @param action         the action for the policy
     */
    private void addGroupToAccessPolicy(final Authorizations authorizations, final String resource, final String groupIdentifier, final String action) {
        // first try to find an existing policy for the given resource and action
        Policy foundPolicy = null;
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            if (policy.getResource().equals(resource) && policy.getAction().equals(action)) {
                foundPolicy = policy;
                break;
            }
        }

        if (foundPolicy == null) {
            // if we didn't find an existing policy create a new one
            final String uuidSeed = resource + action;

            final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                    .identifierGenerateFromSeed(uuidSeed)
                    .resource(resource)
                    .addGroup(groupIdentifier);

            if (action.equals(READ_CODE)) {
                builder.action(RequestAction.READ);
            } else if (action.equals(WRITE_CODE)) {
                builder.action(RequestAction.WRITE);
            } else {
                throw new IllegalStateException("Unknown Policy Action: " + action);
            }

            final AccessPolicy accessPolicy = builder.build();
            final Policy jaxbPolicy = createJAXBPolicy(accessPolicy);
            authorizations.getPolicies().getPolicy().add(jaxbPolicy);
        } else {
            // otherwise add the user to the existing policy
            Policy.Group policyGroup = new Policy.Group();
            policyGroup.setIdentifier(groupIdentifier);
            foundPolicy.getGroup().add(policyGroup);
        }
    }

    private Policy createJAXBPolicy(final AccessPolicy accessPolicy) {
        final Policy policy = new Policy();
        policy.setIdentifier(accessPolicy.getIdentifier());
        policy.setResource(accessPolicy.getResource());

        switch (accessPolicy.getAction()) {
            case READ:
                policy.setAction(READ_CODE);
                break;
            case WRITE:
                policy.setAction(WRITE_CODE);
                break;
            default:
                break;
        }

        transferUsersAndGroups(accessPolicy, policy);
        return policy;
    }

    /**
     * Sets the given Policy to the state of the provided AccessPolicy. Users and Groups will be cleared and
     * set to match the AccessPolicy, the resource and action will be set to match the AccessPolicy.
     *
     * Does not set the identifier.
     *
     * @param accessPolicy the AccessPolicy to transfer state from
     * @param policy the Policy to transfer state to
     */
    private void transferUsersAndGroups(AccessPolicy accessPolicy, Policy policy) {
        // add users to the policy
        policy.getUser().clear();
        for (String userIdentifier : accessPolicy.getUsers()) {
            Policy.User policyUser = new Policy.User();
            policyUser.setIdentifier(userIdentifier);
            policy.getUser().add(policyUser);
        }

        // add groups to the policy
        policy.getGroup().clear();
        for (String groupIdentifier : accessPolicy.getGroups()) {
            Policy.Group policyGroup = new Policy.Group();
            policyGroup.setIdentifier(groupIdentifier);
            policy.getGroup().add(policyGroup);
        }
    }

    /**
     * Adds the given user identifier to the policy if it doesn't already exist.
     *
     * @param userIdentifier a user identifier
     * @param policy a policy to add the user to
     */
    private void addUserToPolicy(final String userIdentifier, final Policy policy) {
        // determine if the user already exists in the policy
        boolean userExists = false;
        for (Policy.User policyUser : policy.getUser()) {
            if (policyUser.getIdentifier().equals(userIdentifier)) {
                userExists = true;
                break;
            }
        }

        // add the user to the policy if doesn't already exist
        if (!userExists) {
            Policy.User policyUser = new Policy.User();
            policyUser.setIdentifier(userIdentifier);
            policy.getUser().add(policyUser);
        }
    }

    /**
     * Adds the given group identifier to the policy if it doesn't already exist.
     *
     * @param groupIdentifier a group identifier
     * @param policy a policy to add the user to
     */
    private void addGroupToPolicy(final String groupIdentifier, final Policy policy) {
        // determine if the group already exists in the policy
        boolean groupExists = false;
        for (Policy.Group policyGroup : policy.getGroup()) {
            if (policyGroup.getIdentifier().equals(groupIdentifier)) {
                groupExists = true;
                break;
            }
        }

        // add the group to the policy if doesn't already exist
        if (!groupExists) {
            Policy.Group policyGroup = new Policy.Group();
            policyGroup.setIdentifier(groupIdentifier);
            policy.getGroup().add(policyGroup);
        }
    }

    /**
     * Finds the Policy matching the resource and action, or creates a new one and adds it to the list of policies.
     *
     * @param policies the policies to search through
     * @param seedIdentity the seedIdentity to use when creating identifiers for new policies
     * @param resource the resource for the policy
     * @param action the action string for the police (R or RW)
     * @return the matching policy or a new policy
     */
    private Policy getOrCreatePolicy(final List<Policy> policies, final String seedIdentity, final String resource, final String action) {
        Policy foundPolicy = null;

        // try to find a policy with the same resource and actions
        for (Policy policy : policies) {
            if (policy.getResource().equals(resource) && policy.getAction().equals(action)) {
                foundPolicy = policy;
                break;
            }
        }

        // if a matching policy wasn't found then create one
        if (foundPolicy == null) {
            final String uuidSeed = resource + action + seedIdentity;
            final String policyIdentifier = IdentifierUtil.getIdentifier(uuidSeed);

            foundPolicy = new Policy();
            foundPolicy.setIdentifier(policyIdentifier);
            foundPolicy.setResource(resource);
            foundPolicy.setAction(action);

            policies.add(foundPolicy);
        }

        return foundPolicy;
    }

    /**
     * Saves the Authorizations instance by marshalling to a file, then re-populates the
     * in-memory data structures and sets the new holder.
     *
     * Synchronized to ensure only one thread writes the file at a time.
     *
     * @param authorizations the authorizations to save and populate from
     * @throws AuthorizationAccessException if an error occurs saving the authorizations
     */
    private synchronized void saveAndRefreshHolder(final Authorizations authorizations) throws AuthorizationAccessException {
        try {
            saveAuthorizations(authorizations);

            this.authorizationsHolder.set(new AuthorizationsHolder(authorizations));
        } catch (JAXBException e) {
            throw new AuthorizationAccessException("Unable to save Authorizations", e);
        }
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
    }
}
