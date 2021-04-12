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
package org.apache.nifi.authorization.single.user;

import org.apache.nifi.authentication.single.user.SingleUserLoginIdentityProvider;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Single User Authorizer requires Single User Login Identity Provider to be configured and authorizes all requests
 */
public class SingleUserAuthorizer implements Authorizer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUserAuthorizer.class);

    private static final String REQUIRED_PROVIDER = SingleUserLoginIdentityProvider.class.getName();

    private static final String IDENTIFIER_TAG = "identifier";

    private static final String CLASS_TAG = "class";

    private static final String BLANK_PROVIDER = "provider";

    /**
     * Set NiFi Properties using method injection
     *
     * @param niFiProperties NiFi Properties
     */
    @AuthorizerContext
    public void setProperties(final NiFiProperties niFiProperties) {
        final File configuration = niFiProperties.getLoginIdentityProviderConfigurationFile();
        final String identifier = niFiProperties.getProperty(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER, BLANK_PROVIDER);
        if (isSingleUserLoginIdentityProviderConfigured(identifier, configuration)) {
            LOGGER.debug("Required Login Identity Provider Configured [{}]", REQUIRED_PROVIDER);
        } else {
            final String message = String.format("%s requires %s to be configured", getClass().getSimpleName(), REQUIRED_PROVIDER);
            throw new AuthorizerCreationException(message);
        }
    }

    @Override
    public AuthorizationResult authorize(final AuthorizationRequest request) throws AuthorizationAccessException {
        return AuthorizationResult.approved();
    }

    @Override
    public void initialize(final AuthorizerInitializationContext initializationContext) {
        LOGGER.info("Initializing Authorizer");
    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) {
        LOGGER.info("Configuring Authorizer");
    }

    @Override
    public void preDestruction() {
        LOGGER.info("Destroying Authorizer");
    }

    private boolean isSingleUserLoginIdentityProviderConfigured(final String configuredIdentifier, final File configuration) {
        try (final InputStream inputStream = new FileInputStream(configuration)) {
            final XMLEventReader reader = getProvidersReader(inputStream);
            final boolean configured = isSingleUserLoginIdentityProviderConfigured(configuredIdentifier, reader);
            reader.close();
            return configured;
        } catch (final XMLStreamException | IOException e) {
            throw new AuthorizerCreationException("Failed to read Login Identity Providers Configuration", e);
        }
    }

    private boolean isSingleUserLoginIdentityProviderConfigured(final String configuredIdentifier, final XMLEventReader reader) throws XMLStreamException {
        boolean providerConfigured = false;

        boolean identifierFound = false;
        while (reader.hasNext()) {
            final XMLEvent event = reader.nextEvent();
            if (isStartElement(event, IDENTIFIER_TAG)) {
                final String providerIdentifier = reader.getElementText().trim();
                identifierFound = configuredIdentifier.equals(providerIdentifier);
            }

            if (identifierFound) {
                // Compare class after finding configured provider identifier
                if (isStartElement(event, CLASS_TAG)) {
                    final String providerClass = reader.getElementText().trim();
                    providerConfigured = REQUIRED_PROVIDER.equals(providerClass);
                }
            }
        }

        return providerConfigured;
    }

    private boolean isStartElement(final XMLEvent event, final String localElementName) {
        boolean found = false;

        if (event.isStartElement()) {
            final StartElement startElement = event.asStartElement();
            found = localElementName.equals(startElement.getName().getLocalPart());
        }

        return found;
    }

    private XMLEventReader getProvidersReader(final InputStream inputStream) throws XMLStreamException {
        final XMLInputFactory inputFactory = XMLInputFactory.newFactory();
        inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        return inputFactory.createXMLEventReader(inputStream);
    }
}
