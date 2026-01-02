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
import org.apache.nifi.xml.processing.parsers.DocumentProvider;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

class FingerprintAuthorizedUserGroupsMapper implements AuthorizedUserGroupsMapper {

    private static final String TENANTS = "tenants";
    private static final String USER = "user";
    private static final String GROUP_USER = "groupUser";
    private static final String GROUP = "group";
    private static final String IDENTIFIER = "identifier";
    private static final String IDENTITY = "identity";
    private static final String NAME = "name";

    @Override
    public AuthorizedUserGroups readUserGroups(final InputStream inputStream) {
        Objects.requireNonNull(inputStream, "Input Stream required");

        final DocumentProvider documentProvider = new StandardDocumentProvider();
        final Document document = documentProvider.parse(inputStream);
        final NodeList tenantsFound = document.getElementsByTagName(TENANTS);
        final Element tenants = (Element) tenantsFound.item(0);

        final List<User> users = new ArrayList<>();
        final List<Group> groups = new ArrayList<>();

        final NodeList userNodes = tenants.getElementsByTagName(USER);
        for (int i = 0; i < userNodes.getLength(); i++) {
            final Element userNode = (Element) userNodes.item(i);
            final String identifier = userNode.getAttribute(IDENTIFIER);
            final String identity = userNode.getAttribute(IDENTITY);
            final User user = new User.Builder().identifier(identifier).identity(identity).build();
            users.add(user);
        }

        final NodeList groupNodes = tenants.getElementsByTagName(GROUP);
        for (int i = 0; i < groupNodes.getLength(); i++) {
            final Element groupNode = (Element) groupNodes.item(i);
            final String identifier = groupNode.getAttribute(IDENTIFIER);
            final String name = groupNode.getAttribute(NAME);

            final Set<String> groupUsers = new HashSet<>();
            final NodeList groupUserNodes = groupNode.getElementsByTagName(GROUP_USER);
            for (int j = 0; j < groupUserNodes.getLength(); j++) {
                final Element groupUserNode = (Element) groupUserNodes.item(j);
                final String groupUserIdentifier = groupUserNode.getAttribute(IDENTIFIER);
                groupUsers.add(groupUserIdentifier);
            }

            final Group group = new Group.Builder().identifier(identifier).name(name).addUsers(groupUsers).build();
            groups.add(group);
        }

        return new AuthorizedUserGroups(users, groups);
    }

    @Override
    public void writeUserGroups(final AuthorizedUserGroups userGroups, final OutputStream outputStream) {
        Objects.requireNonNull(userGroups, "User Groups required");
        Objects.requireNonNull(outputStream, "Output Stream required");

        final List<User> users = userGroups.users();
        users.sort(Comparator.comparing(User::getIdentifier));

        final List<Group> groups = userGroups.groups();
        groups.sort(Comparator.comparing(Group::getIdentifier));

        final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = null;
        try {
            writer = outputFactory.createXMLStreamWriter(outputStream);
            writer.writeStartDocument();
            writer.writeStartElement(TENANTS);

            for (final User user : users) {
                writer.writeStartElement(USER);
                writer.writeAttribute(IDENTIFIER, user.getIdentifier());
                writer.writeAttribute(IDENTITY, user.getIdentity());
                writer.writeEndElement();
            }
            for (final Group group : groups) {
                writeGroup(writer, group);
            }

            writer.writeEndElement();
            writer.writeEndDocument();
            writer.flush();
        } catch (final XMLStreamException e) {
            throw new AuthorizationAccessException("User Groups Fingerprint generation failed", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (XMLStreamException ignored) {

                }
            }
        }
    }

    private void writeGroup(final XMLStreamWriter writer, final Group group) throws XMLStreamException {
        final List<String> users = new ArrayList<>(group.getUsers());
        Collections.sort(users);

        writer.writeStartElement(GROUP);
        writer.writeAttribute(IDENTIFIER, group.getIdentifier());
        writer.writeAttribute(NAME, group.getName());

        for (final String user : users) {
            writer.writeStartElement(GROUP_USER);
            writer.writeAttribute(IDENTIFIER, user);
            writer.writeEndElement();
        }

        writer.writeEndElement();
    }

}
