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

import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * An Authorizer that provides management of users, groups, and policies.
 */
public abstract class AbstractPolicyBasedAuthorizer implements Authorizer {

    static final DocumentBuilderFactory DOCUMENT_BUILDER_FACTORY = DocumentBuilderFactory.newInstance();
    static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newInstance();

    static final String USER_ELEMENT = "user";
    static final String GROUP_USER_ELEMENT = "groupUser";
    static final String GROUP_ELEMENT = "group";
    static final String POLICY_ELEMENT = "policy";
    static final String POLICY_USER_ELEMENT = "policyUser";
    static final String POLICY_GROUP_ELEMENT = "policyGroup";
    static final String IDENTIFIER_ATTR = "identifier";
    static final String IDENTITY_ATTR = "identity";
    static final String NAME_ATTR = "name";
    static final String RESOURCE_ATTR = "resource";
    static final String ACTIONS_ATTR = "actions";

    public static final String EMPTY_FINGERPRINT = "EMPTY";

    @Override
    public final void onConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        doOnConfigured(configurationContext);

        // ensure that only one policy per resource-action exists
        for (AccessPolicy accessPolicy : getAccessPolicies()) {
            if (policyExists(accessPolicy)) {
                throw new AuthorizerCreationException(String.format("Found multiple policies for '%s' with '%s'.", accessPolicy.getResource(), accessPolicy.getAction()));
            }
        }

        // ensure that only one group exists per identity
        for (User user : getUsers()) {
            if (tenantExists(user.getIdentifier(), user.getIdentity())) {
                throw new AuthorizerCreationException(String.format("Found multiple users/user groups with identity '%s'.", user.getIdentity()));
            }
        }

        // ensure that only one group exists per identity
        for (Group group : getGroups()) {
            if (tenantExists(group.getIdentifier(), group.getName())) {
                throw new AuthorizerCreationException(String.format("Found multiple users/user groups with name '%s'.", group.getName()));
            }
        }
    }

    /**
     * Allows sub-classes to take action when onConfigured is called.
     *
     * @param configurationContext the configuration context
     * @throws AuthorizerCreationException if an error occurs during onConfigured process
     */
    protected abstract void doOnConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException;

    /**
     * Checks if another policy exists with the same resource and action as the given policy.
     *
     * @param checkAccessPolicy an access policy being checked
     * @return true if another access policy exists with the same resource and action, false otherwise
     */
    private boolean policyExists(final AccessPolicy checkAccessPolicy) {
        for (AccessPolicy accessPolicy : getAccessPolicies()) {
            if (!accessPolicy.getIdentifier().equals(checkAccessPolicy.getIdentifier())
                    && accessPolicy.getResource().equals(checkAccessPolicy.getResource())
                    && accessPolicy.getAction().equals(checkAccessPolicy.getAction())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if another user exists with the same identity.
     *
     * @param identifier identity of the user
     * @param identity identity of the user
     * @return true if another user exists with the same identity, false otherwise
     */
    private boolean tenantExists(final String identifier, final String identity) {
        for (User user : getUsers()) {
            if (!user.getIdentifier().equals(identifier)
                    && user.getIdentity().equals(identity)) {
                return true;
            }
        }

        for (Group group : getGroups()) {
            if (!group.getIdentifier().equals(identifier)
                    && group.getName().equals(identity)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public final AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException {
        final UsersAndAccessPolicies usersAndAccessPolicies = getUsersAndAccessPolicies();
        final String resourceIdentifier = request.getResource().getIdentifier();

        final AccessPolicy policy = usersAndAccessPolicies.getAccessPolicy(resourceIdentifier, request.getAction());
        if (policy == null) {
            return AuthorizationResult.resourceNotFound();
        }

        final User user = usersAndAccessPolicies.getUser(request.getIdentity());
        if (user == null) {
            return AuthorizationResult.denied(String.format("Unknown user with identity '%s'.", request.getIdentity()));
        }

        final Set<Group> userGroups = usersAndAccessPolicies.getGroups(user.getIdentity());
        if (policy.getUsers().contains(user.getIdentifier()) || containsGroup(userGroups, policy)) {
            return AuthorizationResult.approved();
        }


        return AuthorizationResult.denied();
    }

    /**
     * Determines if the policy contains one of the user's groups.
     *
     * @param userGroups the set of the user's groups
     * @param policy the policy
     * @return true if one of the Groups in userGroups is contained in the policy
     */
    private boolean containsGroup(final Set<Group> userGroups, final AccessPolicy policy) {
        if (userGroups.isEmpty() || policy.getGroups().isEmpty()) {
            return false;
        }

        for (Group userGroup : userGroups) {
            if (policy.getGroups().contains(userGroup.getIdentifier())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Adds a new group.
     *
     * @param group the Group to add
     * @return the added Group
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws IllegalStateException if a group with the same name already exists
     */
    public final synchronized Group addGroup(Group group) throws AuthorizationAccessException {
        if (tenantExists(group.getIdentifier(), group.getName())) {
            throw new IllegalStateException(String.format("User/user group already exists with the identity '%s'.", group.getName()));
        }
        return doAddGroup(group);
    }

    /**
     * Adds a new group.
     *
     * @param group the Group to add
     * @return the added Group
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Group doAddGroup(Group group) throws AuthorizationAccessException;

    /**
     * Retrieves a Group by id.
     *
     * @param identifier the identifier of the Group to retrieve
     * @return the Group with the given identifier, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Group getGroup(String identifier) throws AuthorizationAccessException;

    /**
     * The group represented by the provided instance will be updated based on the provided instance.
     *
     * @param group an updated group instance
     * @return the updated group instance, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws IllegalStateException if there is already a group with the same name
     */
    public final synchronized Group updateGroup(Group group) throws AuthorizationAccessException {
        if (tenantExists(group.getIdentifier(), group.getName())) {
            throw new IllegalStateException(String.format("User/user group already exists with the identity '%s'.", group.getName()));
        }
        return doUpdateGroup(group);
    }

    /**
     * The group represented by the provided instance will be updated based on the provided instance.
     *
     * @param group an updated group instance
     * @return the updated group instance, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Group doUpdateGroup(Group group) throws AuthorizationAccessException;

    /**
     * Deletes the given group.
     *
     * @param group the group to delete
     * @return the deleted group, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Group deleteGroup(Group group) throws AuthorizationAccessException;

    /**
     * Retrieves all groups.
     *
     * @return a list of groups
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Set<Group> getGroups() throws AuthorizationAccessException;


    /**
     * Adds the given user.
     *
     * @param user the user to add
     * @return the user that was added
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws IllegalStateException if there is already a user with the same identity
     */
    public final synchronized User addUser(User user) throws AuthorizationAccessException {
        if (tenantExists(user.getIdentifier(), user.getIdentity())) {
            throw new IllegalStateException(String.format("User/user group already exists with the identity '%s'.", user.getIdentity()));
        }
        return doAddUser(user);
    }

    /**
     * Adds the given user.
     *
     * @param user the user to add
     * @return the user that was added
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User doAddUser(User user) throws AuthorizationAccessException;

    /**
     * Retrieves the user with the given identifier.
     *
     * @param identifier the id of the user to retrieve
     * @return the user with the given id, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User getUser(String identifier) throws AuthorizationAccessException;

    /**
     * Retrieves the user with the given identity.
     *
     * @param identity the identity of the user to retrieve
     * @return the user with the given identity, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User getUserByIdentity(String identity) throws AuthorizationAccessException;

    /**
     * The user represented by the provided instance will be updated based on the provided instance.
     *
     * @param user an updated user instance
     * @return the updated user instance, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws IllegalStateException if there is already a user with the same identity
     */
    public final synchronized User updateUser(final User user) throws AuthorizationAccessException {
        if (tenantExists(user.getIdentifier(), user.getIdentity())) {
            throw new IllegalStateException(String.format("User/user group already exists with the identity '%s'.", user.getIdentity()));
        }
        return doUpdateUser(user);
    }

    /**
     * The user represented by the provided instance will be updated based on the provided instance.
     *
     * @param user an updated user instance
     * @return the updated user instance, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User doUpdateUser(User user) throws AuthorizationAccessException;

    /**
     * Deletes the given user.
     *
     * @param user the user to delete
     * @return the user that was deleted, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User deleteUser(User user) throws AuthorizationAccessException;

    /**
     * Retrieves all users.
     *
     * @return a list of users
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Set<User> getUsers() throws AuthorizationAccessException;

    /**
     * Adds the given policy ensuring that multiple policies can not be added for the same resource and action.
     *
     * @param accessPolicy the policy to add
     * @return the policy that was added
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public final synchronized AccessPolicy addAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (policyExists(accessPolicy)) {
            throw new IllegalStateException(String.format("Found multiple policies for '%s' with '%s'.", accessPolicy.getResource(), accessPolicy.getAction()));
        }
        return doAddAccessPolicy(accessPolicy);
    }

    /**
     * Adds the given policy.
     *
     * @param accessPolicy the policy to add
     * @return the policy that was added
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    protected abstract AccessPolicy doAddAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException;

    /**
     * Retrieves the policy with the given identifier.
     *
     * @param identifier the id of the policy to retrieve
     * @return the policy with the given id, or null if no matching policy exists
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException;

    /**
     * The policy represented by the provided instance will be updated based on the provided instance.
     *
     * @param accessPolicy an updated policy
     * @return the updated policy, or null if no matching policy was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException;

    /**
     * Deletes the given policy.
     *
     * @param policy the policy to delete
     * @return the deleted policy, or null if no matching policy was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract AccessPolicy deleteAccessPolicy(AccessPolicy policy) throws AuthorizationAccessException;

    /**
     * Retrieves all access policies.
     *
     * @return a list of policies
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException;

    /**
     * Returns the UserAccessPolicies instance.
     *
     * @return the UserAccessPolicies instance
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract UsersAndAccessPolicies getUsersAndAccessPolicies() throws AuthorizationAccessException;

    /**
     * Parses the fingerprint and adds any users, groups, and policies to the current Authorizer.
     *
     * @param fingerprint the fingerprint that was obtained from calling getFingerprint() on another Authorizer.
     */
    public final void inheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        if (fingerprint == null || fingerprint.trim().isEmpty()) {
            return;
        }

        final byte[] fingerprintBytes = fingerprint.getBytes(StandardCharsets.UTF_8);

        try (final ByteArrayInputStream in = new ByteArrayInputStream(fingerprintBytes)) {
            final DocumentBuilder docBuilder = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
            final Document document = docBuilder.parse(in);
            final Element rootElement = document.getDocumentElement();

            // parse all the users and add them to the current authorizer
            NodeList userNodes = rootElement.getElementsByTagName(USER_ELEMENT);
            for (int i=0; i < userNodes.getLength(); i++) {
                Node userNode = userNodes.item(i);
                User user = parseUser((Element) userNode);
                addUser(user);
            }

            // parse all the groups and add them to the current authorizer
            NodeList groupNodes = rootElement.getElementsByTagName(GROUP_ELEMENT);
            for (int i=0; i < groupNodes.getLength(); i++) {
                Node groupNode = groupNodes.item(i);
                Group group = parseGroup((Element) groupNode);
                addGroup(group);
            }

            // parse all the policies and add them to the current authorizer
            NodeList policyNodes = rootElement.getElementsByTagName(POLICY_ELEMENT);
            for (int i=0; i < policyNodes.getLength(); i++) {
                Node policyNode = policyNodes.item(i);
                AccessPolicy policy = parsePolicy((Element) policyNode);
                addAccessPolicy(policy);
            }

        } catch (SAXException | ParserConfigurationException | IOException e) {
            throw new AuthorizationAccessException("Unable to parse fingerprint", e);
        }
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

    /**
     * Returns a fingerprint representing the authorizations managed by this authorizer. The fingerprint will be
     * used for comparison to determine if two policy-based authorizers represent a compatible set of users,
     * groups, and policies.
     *
     * @return the fingerprint for this Authorizer
     */
    public final String getFingerprint() throws AuthorizationAccessException {
        final List<User> users = getSortedUsers();
        final List<Group> groups = getSortedGroups();
        final List<AccessPolicy> policies = getSortedAccessPolicies();

        // when there are no users, groups, policies we want to always return a simple indicator so
        // it can easily be determined when comparing fingerprints
        if (users.isEmpty() && groups.isEmpty() && policies.isEmpty()) {
            return EMPTY_FINGERPRINT;
        }

        XMLStreamWriter writer = null;
        final StringWriter out = new StringWriter();
        try {
            writer = XML_OUTPUT_FACTORY.createXMLStreamWriter(out);
            writer.writeStartDocument();
            writer.writeStartElement("authorizations");

            for (User user : users) {
                writeUser(writer, user);
            }
            for (Group group : groups) {
                writeGroup(writer, group);
            }
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

    private List<AccessPolicy> getSortedAccessPolicies() {
        final List<AccessPolicy> policies = new ArrayList<>(getAccessPolicies());

        Collections.sort(policies, new Comparator<AccessPolicy>() {
            @Override
            public int compare(AccessPolicy p1, AccessPolicy p2) {
                return p1.getIdentifier().compareTo(p2.getIdentifier());
            }
        });
        return policies;
    }

    private List<Group> getSortedGroups() {
        final List<Group> groups = new ArrayList<>(getGroups());

        Collections.sort(groups, new Comparator<Group>() {
            @Override
            public int compare(Group g1, Group g2) {
                return g1.getIdentifier().compareTo(g2.getIdentifier());
            }
        });
        return groups;
    }

    private List<User> getSortedUsers() {
        final List<User> users = new ArrayList<>(getUsers());

        Collections.sort(users, new Comparator<User>() {
            @Override
            public int compare(User u1, User u2) {
                return u1.getIdentifier().compareTo(u2.getIdentifier());
            }
        });
        return users;
    }

}
