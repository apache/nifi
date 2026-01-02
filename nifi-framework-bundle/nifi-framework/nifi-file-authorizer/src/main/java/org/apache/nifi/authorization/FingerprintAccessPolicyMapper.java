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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Fingerprint implementation of Access Policy Mapper for serialized authorization configuration
 */
class FingerprintAccessPolicyMapper implements AccessPolicyMapper {
    private static final String ACCESS_POLICIES = "accessPolicies";
    private static final String POLICY = "policy";
    private static final String POLICY_USER = "policyUser";
    private static final String POLICY_GROUP = "policyGroup";
    private static final String IDENTIFIER = "identifier";
    private static final String RESOURCE = "resource";
    private static final String ACTIONS = "actions";

    @Override
    public List<AccessPolicy> readAccessPolicies(final InputStream inputStream) {
        Objects.requireNonNull(inputStream, "Input Stream required");

        final DocumentProvider documentProvider = new StandardDocumentProvider();
        final Document document = documentProvider.parse(inputStream);

        final List<AccessPolicy> accessPolicies = new ArrayList<>();

        final Element rootElement = document.getDocumentElement();
        final NodeList policyNodes = rootElement.getElementsByTagName(POLICY);
        for (int i = 0; i < policyNodes.getLength(); i++) {
            final Element policy = (Element) policyNodes.item(i);
            final AccessPolicy accessPolicy = readAccessPolicy(policy);
            accessPolicies.add(accessPolicy);
        }

        return accessPolicies;
    }

    @Override
    public void writeAccessPolicies(final List<AccessPolicy> accessPolicies, final OutputStream outputStream) {
        Objects.requireNonNull(accessPolicies, "Access Policies required");
        Objects.requireNonNull(outputStream, "Output Stream required");

        final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter streamWriter = null;
        try {
            streamWriter = outputFactory.createXMLStreamWriter(outputStream);

            streamWriter.writeStartDocument();
            streamWriter.writeStartElement(ACCESS_POLICIES);

            accessPolicies.sort(Comparator.comparing(AccessPolicy::getIdentifier));
            for (final AccessPolicy accessPolicy : accessPolicies) {
                writePolicy(streamWriter, accessPolicy);
            }

            streamWriter.writeEndElement();
            streamWriter.writeEndDocument();
        } catch (final XMLStreamException e) {
            throw new ProcessingException("Writing Access Policies failed", e);
        } finally {
            if (streamWriter != null) {
                try {
                    streamWriter.close();
                } catch (final XMLStreamException ignored) {

                }
            }
        }
    }

    private AccessPolicy readAccessPolicy(final Element policy) {
        final String identifier = policy.getAttribute(IDENTIFIER);
        final String resource = policy.getAttribute(RESOURCE);
        final String actions = policy.getAttribute(ACTIONS);

        final RequestAction requestAction;
        if (RequestAction.READ.name().equals(actions)) {
            requestAction = RequestAction.READ;
        } else if (RequestAction.WRITE.name().equals(actions)) {
            requestAction = RequestAction.WRITE;
        } else {
            throw new IllegalStateException("Unknown Policy Action [%s]".formatted(actions));
        }

        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier(identifier)
                .resource(resource)
                .action(requestAction);

        final NodeList users = policy.getElementsByTagName(POLICY_USER);
        for (int i = 0; i < users.getLength(); i++) {
            final Element user = (Element) users.item(i);
            final String userIdentifier = user.getAttribute(IDENTIFIER);
            builder.addUser(userIdentifier);
        }

        final NodeList groups = policy.getElementsByTagName(POLICY_GROUP);
        for (int i = 0; i < groups.getLength(); i++) {
            final Element group = (Element) groups.item(i);
            final String groupIdentifier = group.getAttribute(IDENTIFIER);
            builder.addGroup(groupIdentifier);
        }

        return builder.build();
    }

    private void writePolicy(final XMLStreamWriter writer, final AccessPolicy accessPolicy) throws XMLStreamException {
        final List<String> users = new ArrayList<>(accessPolicy.getUsers());
        Collections.sort(users);

        final List<String> groups = new ArrayList<>(accessPolicy.getGroups());
        Collections.sort(groups);

        writer.writeStartElement(POLICY);
        writer.writeAttribute(IDENTIFIER, accessPolicy.getIdentifier());
        writer.writeAttribute(RESOURCE, accessPolicy.getResource());

        final String action;
        final RequestAction requestAction = accessPolicy.getAction();
        if (RequestAction.READ == requestAction) {
            action = requestAction.name();
        } else if (RequestAction.WRITE == requestAction) {
            action = requestAction.name();
        } else {
            throw new IllegalStateException("Request Action [%s] not supported".formatted(requestAction));
        }
        writer.writeAttribute(ACTIONS, action);

        for (final String user : users) {
            writer.writeStartElement(POLICY_USER);
            writer.writeAttribute(IDENTIFIER, user);
            writer.writeEndElement();
        }

        for (final String group : groups) {
            writer.writeStartElement(POLICY_GROUP);
            writer.writeAttribute(IDENTIFIER, group);
            writer.writeEndElement();
        }

        writer.writeEndElement();
    }
}
