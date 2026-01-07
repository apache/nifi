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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.xml.XMLConstants;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

/**
 * File implementation of Access Policy Mapper for authorizations.xml according to XML Schema Definition
 */
class FileAccessPolicyMapper implements AccessPolicyMapper {

    static final String READ = "R";
    static final String WRITE = "W";

    private static final String AUTHORIZATIONS_XSD = "/authorizations.xsd";

    private static final String AUTHORIZATIONS = "authorizations";
    private static final String POLICIES = "policies";
    private static final String POLICY = "policy";
    private static final String USER = "user";
    private static final String GROUP = "group";
    private static final String IDENTIFIER = "identifier";
    private static final String RESOURCE = "resource";
    private static final String ACTION = "action";

    @Override
    public List<AccessPolicy> readAccessPolicies(final InputStream inputStream) {
        Objects.requireNonNull(inputStream, "Input Stream required");

        final SchemaValidator schemaValidator = new StandardSchemaValidator();
        final Schema schema = getSchema();
        final DocumentProvider documentProvider = new StandardDocumentProvider();
        final Document document = documentProvider.parse(inputStream);
        final Source source = new DOMSource(document);

        // Validate Document using Schema before parsing
        schemaValidator.validate(schema, source);

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

        final DocumentProvider documentProvider = new StandardDocumentProvider();
        final Document document = documentProvider.newDocument();

        final Element authorizations = document.createElement(AUTHORIZATIONS);
        document.appendChild(authorizations);
        final Element policies = document.createElement(POLICIES);
        authorizations.appendChild(policies);

        for (final AccessPolicy accessPolicy : accessPolicies) {
            final Element policy = writePolicy(document, accessPolicy);
            policies.appendChild(policy);
        }

        final StandardTransformProvider transformProvider = new StandardTransformProvider();
        transformProvider.setIndent(true);

        final Source source = new DOMSource(document);
        final Result result = new StreamResult(outputStream);
        transformProvider.transform(source, result);
    }

    private AccessPolicy readAccessPolicy(final Element policy) {
        final String identifier = policy.getAttribute(IDENTIFIER);
        final String resource = policy.getAttribute(RESOURCE);
        final String action = policy.getAttribute(ACTION);

        final RequestAction requestAction;
        if (READ.equals(action)) {
            requestAction = RequestAction.READ;
        } else if (WRITE.equals(action)) {
            requestAction = RequestAction.WRITE;
        } else {
            throw new IllegalStateException("Unknown Policy Action [%s]".formatted(action));
        }

        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier(identifier)
                .resource(resource)
                .action(requestAction);

        final NodeList users = policy.getElementsByTagName(USER);
        for (int i = 0; i < users.getLength(); i++) {
            final Element user = (Element) users.item(i);
            final String userIdentifier = user.getAttribute(IDENTIFIER);
            builder.addUser(userIdentifier);
        }

        final NodeList groups = policy.getElementsByTagName(GROUP);
        for (int i = 0; i < groups.getLength(); i++) {
            final Element group = (Element) groups.item(i);
            final String groupIdentifier = group.getAttribute(IDENTIFIER);
            builder.addGroup(groupIdentifier);
        }

        return builder.build();
    }

    private Element writePolicy(final Document document, final AccessPolicy accessPolicy) {
        final List<String> users = new ArrayList<>(accessPolicy.getUsers());
        Collections.sort(users);

        final List<String> groups = new ArrayList<>(accessPolicy.getGroups());
        Collections.sort(groups);

        final Element policy = document.createElement(POLICY);
        policy.setAttribute(IDENTIFIER, accessPolicy.getIdentifier());
        policy.setAttribute(RESOURCE, accessPolicy.getResource());

        final String action;
        final RequestAction requestAction = accessPolicy.getAction();
        if (RequestAction.READ == requestAction) {
            action = READ;
        } else if (RequestAction.WRITE == requestAction) {
            action = WRITE;
        } else {
            throw new IllegalStateException("Request Action [%s] not supported".formatted(requestAction));
        }
        policy.setAttribute(ACTION, action);

        for (final String group : groups) {
            final Element policyGroup = document.createElement(GROUP);
            policyGroup.setAttribute(IDENTIFIER, group);
            policy.appendChild(policyGroup);
        }

        for (final String user : users) {
            final Element policyUser = document.createElement(USER);
            policyUser.setAttribute(IDENTIFIER, user);
            policy.appendChild(policyUser);
        }

        return policy;
    }

    private Schema getSchema() {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        try {
            return schemaFactory.newSchema(getClass().getResource(AUTHORIZATIONS_XSD));
        } catch (final SAXException e) {
            throw new ProcessingException("Failed to read Authorizations Schema", e);
        }
    }
}
