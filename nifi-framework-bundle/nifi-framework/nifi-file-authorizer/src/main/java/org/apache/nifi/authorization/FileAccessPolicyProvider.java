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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.authorization.file.generated.Authorizations;
import org.apache.nifi.authorization.file.generated.Policies;
import org.apache.nifi.authorization.file.generated.Policy;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.FlowInfo;
import org.apache.nifi.util.FlowParser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.apache.nifi.xml.processing.stream.StandardXMLStreamReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLStreamReaderProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileAccessPolicyProvider implements ConfigurableAccessPolicyProvider {

    private static final Logger logger = LoggerFactory.getLogger(FileAccessPolicyProvider.class);

    private static final String AUTHORIZATIONS_XSD = "/authorizations.xsd";
    private static final String JAXB_AUTHORIZATIONS_PATH = "org.apache.nifi.authorization.file.generated";

    private static final JAXBContext JAXB_AUTHORIZATIONS_CONTEXT = initializeJaxbContext();

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_AUTHORIZATIONS_PATH, FileAccessPolicyProvider.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

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

    private Schema authorizationsSchema;
    private NiFiProperties properties;
    private File authorizationsFile;
    private File restoreAuthorizationsFile;
    private String rootGroupId;
    private String initialAdminIdentity;
    private Set<String> nodeIdentities;
    private String nodeGroupIdentifier;

    private UserGroupProvider userGroupProvider;
    private UserGroupProviderLookup userGroupProviderLookup;
    private final AtomicReference<AuthorizationsHolder> authorizationsHolder = new AtomicReference<>();

    @Override
    public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {
        userGroupProviderLookup = initializationContext.getUserGroupProviderLookup();

        try {
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            authorizationsSchema = schemaFactory.newSchema(FileAccessPolicyProvider.class.getResource(AUTHORIZATIONS_XSD));
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
                logger.info("Creating new authorizations file at {}", authorizationsFile.getAbsolutePath());
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
            List<IdentityMapping> identityMappings = Collections.unmodifiableList(IdentityMappingUtil.getIdentityMappings(properties));

            // get the value of the initial admin identity
            final PropertyValue initialAdminIdentityProp = configurationContext.getProperty(PROP_INITIAL_ADMIN_IDENTITY);
            initialAdminIdentity = initialAdminIdentityProp.isSet() ? IdentityMappingUtil.mapIdentity(initialAdminIdentityProp.getValue(), identityMappings) : null;

            // extract any node identities
            nodeIdentities = new HashSet<>();
            for (Map.Entry<String, String> entry : configurationContext.getProperties().entrySet()) {
                Matcher matcher = NODE_IDENTITY_PATTERN.matcher(entry.getKey());
                if (matcher.matches() && !StringUtils.isBlank(entry.getValue())) {
                    final String mappedNodeIdentity = IdentityMappingUtil.mapIdentity(entry.getValue(), identityMappings);
                    nodeIdentities.add(mappedNodeIdentity);
                    logger.info("Added mapped node {} (raw node identity {})", mappedNodeIdentity, entry.getValue());
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

            logger.debug("Authorizations file loaded");
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
        addAccessPolicies(Collections.singletonList(accessPolicy));
        return authorizationsHolder.get().getPoliciesById().get(accessPolicy.getIdentifier());
    }

    private synchronized void addAccessPolicies(final List<AccessPolicy> accessPolicies) throws AuthorizationAccessException {
        if (accessPolicies == null) {
            throw new IllegalArgumentException("AccessPolicies cannot be null");
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        final Authorizations authorizations = holder.getAuthorizations();
        final List<Policy> policyList = authorizations.getPolicies().getPolicy();

        for (final AccessPolicy accessPolicy : accessPolicies) {
            // create the new JAXB Policy
            final Policy policy = createJAXBPolicy(accessPolicy);

            // add the new Policy to the top-level list of policies
            policyList.add(policy);
        }

        saveAndRefreshHolder(authorizations);
    }

    public synchronized void purgePolicies(final boolean save) {
        final AuthorizationsHolder holder = authorizationsHolder.get();
        final Authorizations authorizations = holder.getAuthorizations();
        final List<Policy> policyList = authorizations.getPolicies().getPolicy();

        policyList.clear();

        if (save) {
            saveAndRefreshHolder(authorizations);
        }
    }

    public void backupPolicies() throws JAXBException {
        final AuthorizationsHolder holder = authorizationsHolder.get();
        final Authorizations authorizations = holder.getAuthorizations();

        final String timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").format(OffsetDateTime.now());

        final File backupFile = new File(authorizationsFile.getParentFile(), authorizationsFile.getName() + "." + timestamp);
        logger.info("Writing backup of Policies to {}", backupFile.getAbsolutePath());
        saveAuthorizations(authorizations, backupFile);
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
        final List<AccessPolicy> accessPolicies = parsePolicies(fingerprint);
        inheritAccessPolicies(accessPolicies);
    }

    private synchronized void inheritAccessPolicies(final List<AccessPolicy> accessPolicies) {
        addAccessPolicies(accessPolicies);
    }

    @Override
    public synchronized void forciblyInheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        final List<AccessPolicy> accessPolicies = parsePolicies(fingerprint);

        if (isInheritable()) {
            logger.debug("Inheriting cluster's Access Policies");
            inheritAccessPolicies(accessPolicies);
        } else {
            logger.info("Cannot directly inherit cluster's Access Policies. Will create backup of existing policies and replace with proposed policies");

            try {
                backupPolicies();
            } catch (final JAXBException jaxb) {
                throw new AuthorizationAccessException("Failed to backup existing policies so will not inherit any policies", jaxb);
            }

            purgePolicies(false);
            addAccessPolicies(accessPolicies);
        }
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
        // ensure we are in a proper state to inherit the fingerprint
        if (!isInheritable()) {
            throw new UninheritableAuthorizationsException("Proposed fingerprint is not inheritable because the current access policies is not empty.");
        }
    }

    private boolean isInheritable() {
        return getAccessPolicies().isEmpty();
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        final List<AccessPolicy> policies = new ArrayList<>(getAccessPolicies());
        policies.sort(Comparator.comparing(AccessPolicy::getIdentifier));

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
                } catch (XMLStreamException ignored) {
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
            final StandardDocumentProvider documentProvider = new StandardDocumentProvider();
            final Document document = documentProvider.parse(in);
            final Element rootElement = document.getDocumentElement();

            // parse all the policies and add them to the current access policy provider
            NodeList policyNodes = rootElement.getElementsByTagName(POLICY_ELEMENT);
            for (int i = 0; i < policyNodes.getLength(); i++) {
                Node policyNode = policyNodes.item(i);
                policies.add(parsePolicy((Element) policyNode));
            }
        } catch (final ProcessingException | IOException e) {
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
        for (int i = 0; i < policyUsers.getLength(); i++) {
            Element policyUserNode = (Element) policyUsers.item(i);
            builder.addUser(policyUserNode.getAttribute(IDENTIFIER_ATTR));
        }

        NodeList policyGroups = element.getElementsByTagName(POLICY_GROUP_ELEMENT);
        for (int i = 0; i < policyGroups.getLength(); i++) {
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

        // if we are starting fresh then we might need to populate an initial admin or convert legacy users
        if (emptyAuthorizations) {
            parseFlow();

            if (hasInitialAdminIdentity) {
                logger.info("Populating authorizations for Initial Admin: {}", initialAdminIdentity);
                populateInitialAdmin(authorizations);
            }

            populateNodes(authorizations);

            // save any changes that were made and repopulate the holder
            saveAndRefreshHolder(authorizations);
        } else {
            this.authorizationsHolder.set(authorizationsHolder);
        }
    }

    private void saveAuthorizations(final Authorizations authorizations) throws JAXBException {
        saveAuthorizations(authorizations, authorizationsFile);
    }

    private void saveAuthorizations(final Authorizations authorizations, final File destinationFile) throws JAXBException {
        final Marshaller marshaller = JAXB_AUTHORIZATIONS_CONTEXT.createMarshaller();
        marshaller.setSchema(authorizationsSchema);
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(authorizations, destinationFile);
    }

    private Authorizations unmarshallAuthorizations() throws JAXBException {
        try {
            final XMLStreamReaderProvider provider = new StandardXMLStreamReaderProvider();
            final XMLStreamReader xsr = provider.getStreamReader(new StreamSource(authorizationsFile));
            final Unmarshaller unmarshaller = JAXB_AUTHORIZATIONS_CONTEXT.createUnmarshaller();
            unmarshaller.setSchema(authorizationsSchema);

            final JAXBElement<Authorizations> element = unmarshaller.unmarshal(xsr, Authorizations.class);
            return element.getValue();
        } catch (final ProcessingException e) {
            logger.error("Encountered an error reading authorizations file: ", e);
            throw new JAXBException("Error reading authorizations file", e);
        }
    }

    /**
     * Try to parse the flow configuration file to extract the root group id and port information.
     */
    private void parseFlow() {
        final FlowParser flowParser = new FlowParser();
        final File flowConfigurationFile = properties.getFlowConfigurationFile();
        final FlowInfo flowInfo = flowParser.parse(flowConfigurationFile);

        if (flowInfo != null) {
            rootGroupId = flowInfo.getRootGroupId();
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
            // grant access to read controller for syncing custom NARs
            addUserToAccessPolicy(authorizations, ResourceType.Controller.getValue(), node.getIdentifier(), READ_CODE);

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
     * Saves the Authorizations instance by marshalling to a file, then re-populates the
     * in-memory data structures and sets the new holder.
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
