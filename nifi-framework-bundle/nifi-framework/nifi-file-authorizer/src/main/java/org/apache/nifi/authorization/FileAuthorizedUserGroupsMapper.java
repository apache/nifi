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

import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.DocumentProvider;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.apache.nifi.xml.processing.transform.StandardTransformProvider;
import org.apache.nifi.xml.processing.validation.SchemaValidator;
import org.apache.nifi.xml.processing.validation.StandardSchemaValidator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

class FileAuthorizedUserGroupsMapper implements AuthorizedUserGroupsMapper {
    private static final String TENANTS_XSD = "/tenants.xsd";

    private static final String TENANTS = "tenants";
    private static final String USER = "user";
    private static final String USERS = "users";
    private static final String GROUP = "group";
    private static final String GROUPS = "groups";
    private static final String IDENTIFIER = "identifier";
    private static final String IDENTITY = "identity";
    private static final String NAME = "name";

    @Override
    public AuthorizedUserGroups readUserGroups(final InputStream inputStream) {
        Objects.requireNonNull(inputStream, "Input Stream required");

        final SchemaValidator schemaValidator = new StandardSchemaValidator();
        final Schema schema = getSchema();
        final DocumentProvider documentProvider = new StandardDocumentProvider();
        final Document document = documentProvider.parse(inputStream);
        final Source source = new DOMSource(document);

        // Validate Document using Schema before parsing
        schemaValidator.validate(schema, source);

        final List<Group> groups = new ArrayList<>();
        final List<User> users = new ArrayList<>();

        final NodeList rootNodes = document.getElementsByTagName(TENANTS);
        final Element tenants = (Element) rootNodes.item(0);

        final NodeList groupsNodes = tenants.getElementsByTagName(GROUPS);
        if (groupsNodes.getLength() == 1) {
            final Element groupsElement = (Element) groupsNodes.item(0);
            final NodeList groupNodes = groupsElement.getElementsByTagName(GROUP);
            for (int i = 0; i < groupNodes.getLength(); i++) {
                final Element groupNode = (Element) groupNodes.item(i);
                final Group group = readGroup(groupNode);
                groups.add(group);
            }
        }

        final NodeList usersNodes = tenants.getElementsByTagName(USERS);
        if (usersNodes.getLength() == 1) {
            final Element usersElement = (Element) usersNodes.item(0);
            final NodeList userNodes = usersElement.getElementsByTagName(USER);
            for (int i = 0; i < userNodes.getLength(); i++) {
                final Element userNode = (Element) userNodes.item(i);
                final User user = readUser(userNode);
                users.add(user);
            }
        }

        return new AuthorizedUserGroups(users, groups);
    }

    @Override
    public void writeUserGroups(final AuthorizedUserGroups userGroups, final OutputStream outputStream) {
        Objects.requireNonNull(userGroups, "User Groups required");
        Objects.requireNonNull(outputStream, "Output Stream required");

        final DocumentProvider documentProvider = new StandardDocumentProvider();
        final Document document = documentProvider.newDocument();

        final Element tenants = document.createElement(TENANTS);
        document.appendChild(tenants);

        final Iterator<Group> groups = userGroups.groups().iterator();
        if (groups.hasNext()) {
            final Element groupsElement = document.createElement(GROUPS);
            tenants.appendChild(groupsElement);

            while (groups.hasNext()) {
                final Group group = groups.next();
                final Element groupElement = document.createElement(GROUP);
                groupElement.setAttribute(IDENTIFIER, group.getIdentifier());
                groupElement.setAttribute(NAME, group.getName());

                for (final String user : group.getUsers()) {
                    final Element userElement = document.createElement(USER);
                    userElement.setAttribute(IDENTIFIER, user);
                    groupElement.appendChild(userElement);
                }

                groupsElement.appendChild(groupElement);
            }
        }

        final Iterator<User> users = userGroups.users().iterator();
        if (users.hasNext()) {
            final Element usersElement = document.createElement(USERS);
            tenants.appendChild(usersElement);

            while (users.hasNext()) {
                final User user = users.next();
                final Element userElement = document.createElement(USER);
                userElement.setAttribute(IDENTIFIER, user.getIdentifier());
                userElement.setAttribute(IDENTITY, user.getIdentity());
                usersElement.appendChild(userElement);
            }
        }

        final StandardTransformProvider transformProvider = new StandardTransformProvider();
        transformProvider.setIndent(true);

        final Source source = new DOMSource(document);
        final Result result = new StreamResult(outputStream);
        transformProvider.transform(source, result);
    }

    private User readUser(final Element userNode) {
        final String identifier = userNode.getAttribute(IDENTIFIER);
        final String identity = userNode.getAttribute(IDENTITY);
        return new User.Builder().identifier(identifier).identity(identity).build();
    }

    private Group readGroup(final Element groupNode) {
        final String identifier = groupNode.getAttribute(IDENTIFIER);
        final String name = groupNode.getAttribute(NAME);

        final Set<String> users = new HashSet<>();
        final NodeList userNodes = groupNode.getElementsByTagName(USER);
        for (int i = 0; i < userNodes.getLength(); i++) {
            final Element userNode = (Element) userNodes.item(i);
            final String userIdentifier = userNode.getAttribute(IDENTIFIER);
            users.add(userIdentifier);
        }

        return new Group.Builder().identifier(identifier).name(name).addUsers(users).build();
    }

    private Schema getSchema() {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        try {
            return schemaFactory.newSchema(getClass().getResource(TENANTS_XSD));
        } catch (final SAXException e) {
            throw new ProcessingException("Failed to read Tenants Schema", e);
        }
    }
}
