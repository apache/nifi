/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.ranger.authorization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.lang.StringUtils;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.AccessPolicyProviderInitializationContext;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.ConfigurableUserGroupProvider;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderLookup;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.security.xml.XmlUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ManagedRangerAuthorizer extends RangerNiFiAuthorizer implements ManagedAuthorizer {
    private static final String USER_GROUP_PROVIDER_ELEMENT = "userGroupProvider";

    private UserGroupProviderLookup userGroupProviderLookup;
    private UserGroupProvider userGroupProvider;
    private RangerBasePluginWithPolicies nifiPlugin;

    @Override
    public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
        userGroupProviderLookup = initializationContext.getUserGroupProviderLookup();

        super.initialize(initializationContext);
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        final String userGroupProviderKey = configurationContext.getProperty("User Group Provider").getValue();
        userGroupProvider = userGroupProviderLookup.getUserGroupProvider(userGroupProviderKey);

        // ensure the desired access policy provider has a user group provider
        if (userGroupProvider == null) {
            throw new AuthorizerCreationException(String.format("Unable to locate configured User Group Provider: %s", userGroupProviderKey));
        }

        super.onConfigured(configurationContext);
    }

    @Override
    protected RangerBasePluginWithPolicies createRangerBasePlugin(final String serviceType, final String appId) {
        // override the method for creating the ranger base plugin so a user group provider can be specified
        nifiPlugin = new RangerBasePluginWithPolicies(serviceType, appId, userGroupProvider);
        return nifiPlugin;
    }

    @Override
    public AccessPolicyProvider getAccessPolicyProvider() {
        return new AccessPolicyProvider() {
            @Override
            public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                return nifiPlugin.getAccessPolicies();
            }

            @Override
            public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                return nifiPlugin.getAccessPolicy(identifier);
            }

            @Override
            public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
                return nifiPlugin.getAccessPolicy(resourceIdentifier, action);
            }

            @Override
            public UserGroupProvider getUserGroupProvider() {
                return userGroupProvider;
            }

            @Override
            public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {
            }

            @Override
            public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
            }

            @Override
            public void preDestruction() throws AuthorizerDestructionException {
            }
        };
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        final StringWriter out = new StringWriter();
        try {
            // create the document
            final DocumentBuilder documentBuilder = XmlUtils.createSafeDocumentBuilder(false);
            final Document document = documentBuilder.newDocument();

            // create the root element
            final Element managedRangerAuthorizationsElement = document.createElement("managedRangerAuthorizations");
            document.appendChild(managedRangerAuthorizationsElement);

            // create the user group provider element
            final Element userGroupProviderElement = document.createElement(USER_GROUP_PROVIDER_ELEMENT);
            managedRangerAuthorizationsElement.appendChild(userGroupProviderElement);

            // append fingerprint if the provider is configurable
            if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
                userGroupProviderElement.appendChild(document.createTextNode(((ConfigurableUserGroupProvider) userGroupProvider).getFingerprint()));
            }

            final Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(document), new StreamResult(out));
        } catch (ParserConfigurationException | TransformerException e) {
            throw new AuthorizationAccessException("Unable to generate fingerprint", e);
        }

        return out.toString();
    }

    @Override
    public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
        if (StringUtils.isBlank(fingerprint)) {
            return;
        }

        final String userGroupFingerprint = parseFingerprint(fingerprint);

        if (StringUtils.isNotBlank(userGroupFingerprint) && userGroupProvider instanceof ConfigurableUserGroupProvider) {
            ((ConfigurableUserGroupProvider) userGroupProvider).inheritFingerprint(userGroupFingerprint);
        }
    }

    @Override
    public void forciblyInheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        final String userGroupFingerprint = parseFingerprint(fingerprint);

        if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
            ((ConfigurableUserGroupProvider) userGroupProvider).forciblyInheritFingerprint(userGroupFingerprint);
        }
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
        final String userGroupFingerprint = parseFingerprint(proposedFingerprint);

        if (StringUtils.isNotBlank(userGroupFingerprint)) {
            if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
                ((ConfigurableUserGroupProvider) userGroupProvider).checkInheritability(userGroupFingerprint);
            } else {
                throw new UninheritableAuthorizationsException("User/Group fingerprint is not blank and the configured UserGroupProvider does not support fingerprinting.");
            }
        }
    }

    private String parseFingerprint(final String fingerprint) throws AuthorizationAccessException {
        final byte[] fingerprintBytes = fingerprint.getBytes(StandardCharsets.UTF_8);

        try (final ByteArrayInputStream in = new ByteArrayInputStream(fingerprintBytes)) {
            final DocumentBuilder docBuilder = XmlUtils.createSafeDocumentBuilder(false);
            final Document document = docBuilder.parse(in);
            final Element rootElement = document.getDocumentElement();

            final NodeList userGroupProviderList = rootElement.getElementsByTagName(USER_GROUP_PROVIDER_ELEMENT);
            if (userGroupProviderList.getLength() != 1) {
                throw new AuthorizationAccessException(String.format("Only one %s element is allowed: %s", USER_GROUP_PROVIDER_ELEMENT, fingerprint));
            }

            final Node userGroupProvider = userGroupProviderList.item(0);
            return userGroupProvider.getTextContent();
        } catch (SAXException | ParserConfigurationException | IOException e) {
            throw new AuthorizationAccessException("Unable to parse fingerprint", e);
        }
    }
}
