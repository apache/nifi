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
package org.apache.nifi.web.security.saml.impl;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.saml.NiFiSAMLContextProvider;
import org.apache.nifi.web.security.saml.SAMLConfiguration;
import org.apache.nifi.web.security.saml.SAMLConfigurationFactory;
import org.apache.nifi.web.security.saml.SAMLEndpoints;
import org.apache.nifi.web.security.saml.SAMLService;
import org.opensaml.common.SAMLException;
import org.opensaml.common.SAMLRuntimeException;
import org.opensaml.common.binding.decoding.URIComparator;
import org.opensaml.saml2.core.Attribute;
import org.opensaml.saml2.core.LogoutRequest;
import org.opensaml.saml2.core.LogoutResponse;
import org.opensaml.saml2.metadata.Endpoint;
import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.xml.XMLObject;
import org.opensaml.xml.encryption.DecryptionException;
import org.opensaml.xml.schema.XSString;
import org.opensaml.xml.schema.impl.XSAnyImpl;
import org.opensaml.xml.validation.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.saml.SAMLConstants;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.security.saml.SAMLLogoutProcessingFilter;
import org.springframework.security.saml.SAMLProcessingFilter;
import org.springframework.security.saml.context.SAMLMessageContext;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.log.SAMLLogger;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.ExtendedMetadataDelegate;
import org.springframework.security.saml.metadata.MetadataGenerator;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.security.saml.metadata.MetadataMemoryProvider;
import org.springframework.security.saml.processor.SAMLProcessor;
import org.springframework.security.saml.util.DefaultURLComparator;
import org.springframework.security.saml.util.SAMLUtil;
import org.springframework.security.saml.websso.SingleLogoutProfile;
import org.springframework.security.saml.websso.WebSSOProfile;
import org.springframework.security.saml.websso.WebSSOProfileConsumer;
import org.springframework.security.saml.websso.WebSSOProfileOptions;
import org.springframework.security.web.authentication.logout.LogoutHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class StandardSAMLService implements SAMLService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardSAMLService.class);

    private final NiFiProperties properties;
    private final SAMLConfigurationFactory samlConfigurationFactory;

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean spMetadataInitialized = new AtomicBoolean(false);
    private final AtomicReference<String> spBaseUrl = new AtomicReference<>(null);
    private final URIComparator uriComparator = new DefaultURLComparator();

    private SAMLConfiguration samlConfiguration;


    public StandardSAMLService(final SAMLConfigurationFactory samlConfigurationFactory, final NiFiProperties properties) {
        this.properties = properties;
        this.samlConfigurationFactory = samlConfigurationFactory;
    }

    @Override
    public synchronized void initialize() {
        // this method will always be called so if SAML is not configured just return, don't throw an exception
        if (!properties.isSamlEnabled()) {
            return;
        }

        // already initialized so return
        if (initialized.get()) {
            return;
        }

        try {
            LOGGER.info("Initializing SAML Service...");
            samlConfiguration = samlConfigurationFactory.create(properties);
            initialized.set(true);
            LOGGER.info("Finished initializing SAML Service");
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize SAML configuration due to: " + e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() {
        // this method will always be called so if SAML is not configured just return, don't throw an exception
        if (!properties.isSamlEnabled()) {
            return;
        }

        LOGGER.info("Shutting down SAML Service...");

        if (samlConfiguration != null) {
            try {
                final Timer backgroundTimer = samlConfiguration.getBackgroundTaskTimer();
                backgroundTimer.purge();
                backgroundTimer.cancel();
            } catch (final Exception e) {
                LOGGER.warn("Error shutting down background timer: " + e.getMessage(), e);
            }

            try {
                final MetadataManager metadataManager = samlConfiguration.getMetadataManager();
                metadataManager.destroy();
            } catch (final Exception e) {
                LOGGER.warn("Error shutting down metadata manager: " + e.getMessage(), e);
            }
        }

        samlConfiguration = null;
        initialized.set(false);
        spMetadataInitialized.set(false);
        spBaseUrl.set(null);

        LOGGER.info("Finished shutting down SAML Service");
    }

    @Override
    public boolean isSamlEnabled() {
        return properties.isSamlEnabled();
    }

    @Override
    public boolean isServiceProviderInitialized() {
        return spMetadataInitialized.get();
    }

    @Override
    public synchronized void initializeServiceProvider(final String baseUrl) {
        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }

        if (StringUtils.isBlank(baseUrl)) {
            throw new IllegalArgumentException("baseUrl is required when initializing the service provider");
        }

        if (isServiceProviderInitialized()) {
            final String existingBaseUrl = spBaseUrl.get();
            LOGGER.info("Service provider already initialized with baseUrl = '{}'", new Object[]{existingBaseUrl});
            return;
        }

        LOGGER.info("Initializing SAML service provider with baseUrl = '{}'", new Object[]{baseUrl});
        try {
            initializeServiceProviderMetadata(baseUrl);
            spBaseUrl.set(baseUrl);
            spMetadataInitialized.set(true);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize SAML service provider: " + e.getMessage(), e);
        }
        LOGGER.info("Done initializing SAML service provider");
    }

    @Override
    public String getServiceProviderMetadata() {
        verifyReadyForSamlOperations();
        try {
            final KeyManager keyManager = samlConfiguration.getKeyManager();
            final MetadataManager metadataManager = samlConfiguration.getMetadataManager();

            final String spEntityId = samlConfiguration.getSpEntityId();
            final EntityDescriptor descriptor = metadataManager.getEntityDescriptor(spEntityId);

            final String metadataString = SAMLUtil.getMetadataAsString(metadataManager, keyManager, descriptor, null);
            return metadataString;
        } catch (Exception e) {
            throw new RuntimeException("Unable to obtain SAML service provider metadata", e);
        }
    }

    @Override
    public long getAuthExpiration() {
        verifyReadyForSamlOperations();
        return samlConfiguration.getAuthExpiration();
    }

    @Override
    public void initiateLogin(final HttpServletRequest request, final HttpServletResponse response, final String relayState) {
        verifyReadyForSamlOperations();

        final SAMLLogger samlLogger = samlConfiguration.getLogger();
        final NiFiSAMLContextProvider contextProvider = samlConfiguration.getContextProvider();

        final SAMLMessageContext context;
        try {
            context = contextProvider.getLocalAndPeerEntity(request, response, Collections.emptyMap());
        } catch (final MetadataProviderException e) {
            throw new IllegalStateException("Unable to create SAML Message Context: " + e.getMessage(), e);
        }

        // Generate options for the current SSO request
        final WebSSOProfileOptions options = samlConfiguration.getWebSSOProfileOptions().clone();
        options.setRelayState(relayState);

        // Send WebSSO AuthN request
        final WebSSOProfile webSSOProfile = samlConfiguration.getWebSSOProfile();
        try {
            webSSOProfile.sendAuthenticationRequest(context, options);
            samlLogger.log(SAMLConstants.AUTH_N_REQUEST, SAMLConstants.SUCCESS, context);
        } catch (Exception e) {
            samlLogger.log(SAMLConstants.AUTH_N_REQUEST, SAMLConstants.FAILURE, context);
            throw new RuntimeException("Unable to initiate SAML authentication request: " + e.getMessage(), e);
        }
    }

    @Override
    public SAMLCredential processLogin(final HttpServletRequest request, final HttpServletResponse response, final Map<String,String> parameters) {
        verifyReadyForSamlOperations();

        LOGGER.info("Attempting SAML2 authentication using profile {}", SAMLConstants.SAML2_WEBSSO_PROFILE_URI);

        final SAMLMessageContext context;
        try {
            final NiFiSAMLContextProvider contextProvider = samlConfiguration.getContextProvider();
            context = contextProvider.getLocalEntity(request, response, parameters);
        } catch (MetadataProviderException e) {
            throw new IllegalStateException("Unable to create SAML Message Context: " + e.getMessage(), e);
        }

        final SAMLProcessor samlProcessor = samlConfiguration.getProcessor();
        try {
            samlProcessor.retrieveMessage(context);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load SAML message: " + e.getMessage(), e);
        }

        // Override set values
        context.setCommunicationProfileId(SAMLConstants.SAML2_WEBSSO_PROFILE_URI);
        try {
            context.setLocalEntityEndpoint(getLocalEntityEndpoint(context));
        } catch (SAMLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        if (!SAMLConstants.SAML2_WEBSSO_PROFILE_URI.equals(context.getCommunicationProfileId())) {
            throw new IllegalStateException("Unsupported profile encountered in the context: " + context.getCommunicationProfileId());
        }

        final SAMLLogger samlLogger = samlConfiguration.getLogger();
        final WebSSOProfileConsumer webSSOProfileConsumer = samlConfiguration.getWebSSOProfileConsumer();

        try {
            final SAMLCredential credential = webSSOProfileConsumer.processAuthenticationResponse(context);
            LOGGER.debug("SAML Response contains successful authentication for NameID: " + credential.getNameID().getValue());
            samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.SUCCESS, context);
            return credential;
        } catch (SAMLException | SAMLRuntimeException e) {
            LOGGER.error("Error validating SAML message", e);
            samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.FAILURE, context, e);
            throw new RuntimeException("Error validating SAML message: " + e.getMessage(), e);
        } catch (org.opensaml.xml.security.SecurityException | ValidationException e) {
            LOGGER.error("Error validating signature", e);
            samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.FAILURE, context, e);
            throw new RuntimeException("Error validating SAML message signature: " + e.getMessage(), e);
        } catch (DecryptionException e) {
            LOGGER.error("Error decrypting SAML message", e);
            samlLogger.log(SAMLConstants.AUTH_N_RESPONSE, SAMLConstants.FAILURE, context, e);
            throw new RuntimeException("Error decrypting SAML message: " + e.getMessage(), e);
        }
    }

    @Override
    public String getUserIdentity(final SAMLCredential credential) {
        verifyReadyForSamlOperations();

        if (credential == null) {
            throw new IllegalArgumentException("SAML Credential is required");
        }

        String userIdentity = null;

        final String identityAttributeName = samlConfiguration.getIdentityAttributeName();
        if (StringUtils.isBlank(identityAttributeName)) {
            userIdentity = credential.getNameID().getValue();
            LOGGER.info("No identity attribute specified, using NameID for user identity: {}", userIdentity);
        } else {
            LOGGER.debug("Looking for SAML attribute {} ...", identityAttributeName);

            final List<Attribute> attributes = credential.getAttributes();
            if (attributes == null || attributes.isEmpty()) {
                userIdentity = credential.getNameID().getValue();
                LOGGER.warn("No attributes returned in SAML response, using NameID for user identity: {}", userIdentity);
            } else {
                for (final Attribute attribute : attributes) {
                    if (!identityAttributeName.equals(attribute.getName())) {
                        LOGGER.trace("Skipping SAML attribute {}", attribute.getName());
                        continue;
                    }

                    for (final XMLObject value : attribute.getAttributeValues()) {
                        if (value instanceof XSString) {
                            final XSString valueXSString = (XSString) value;
                            userIdentity = valueXSString.getValue();
                            break;
                        } else {
                            LOGGER.debug("Value was not XSString, but was " + value.getClass().getCanonicalName());
                        }
                    }

                    if (userIdentity != null) {
                        LOGGER.info("Found user identity {} in attribute {}", userIdentity, attribute.getName());
                        break;
                    }
                }
            }

            if (userIdentity == null) {
                userIdentity = credential.getNameID().getValue();
                LOGGER.warn("No attribute found named {}, using NameID for user identity: {}", identityAttributeName, userIdentity);
            }
        }

        return userIdentity;
    }

    @Override
    public Set<String> getUserGroups(final SAMLCredential credential) {
        verifyReadyForSamlOperations();

        if (credential == null) {
            throw new IllegalArgumentException("SAML Credential is required");
        }

        final String userIdentity = credential.getNameID().getValue();
        final String groupAttributeName = samlConfiguration.getGroupAttributeName();
        if (StringUtils.isBlank(groupAttributeName)) {
            LOGGER.warn("Cannot obtain groups for {} because no group attribute name has been configured", userIdentity);
            return Collections.emptySet();
        }

        final Set<String> groups = new HashSet<>();
        if (credential.getAttributes() != null) {
            for (final Attribute attribute : credential.getAttributes()) {
                if (!groupAttributeName.equals(attribute.getName())) {
                    LOGGER.debug("Skipping SAML attribute {}", attribute.getName());
                    continue;
                }

                for (final XMLObject value : attribute.getAttributeValues()) {
                    if (value instanceof XSString) {
                        final XSString valueXSString = (XSString) value;
                        final String groupName = valueXSString.getValue();
                        LOGGER.debug("Found group {} for {}", groupName, userIdentity);
                        groups.add(groupName);
                    } else if (value instanceof XSAnyImpl) {
                        final XSAnyImpl valueXSAnyImpl = (XSAnyImpl) value;
                        final String groupName = valueXSAnyImpl.getTextContent();
                        LOGGER.debug("Found group {} for {}", groupName, userIdentity);
                        groups.add(groupName);
                    } else {
                        LOGGER.debug("Value was not XSString and XSAnyImpl, but was " + value.getClass().getCanonicalName());
                    }
                }
            }
        }

        return groups;
    }

    @Override
    public void initiateLogout(final HttpServletRequest request, final HttpServletResponse response, final SAMLCredential credential) {
        verifyReadyForSamlOperations();

        final SAMLMessageContext context;
        try {
            final NiFiSAMLContextProvider contextProvider = samlConfiguration.getContextProvider();
            context = contextProvider.getLocalAndPeerEntity(request, response, Collections.emptyMap());
        } catch (MetadataProviderException e) {
            throw new IllegalStateException("Unable to create SAML Message Context: " + e.getMessage(), e);
        }

        final SAMLLogger samlLogger = samlConfiguration.getLogger();
        final SingleLogoutProfile singleLogoutProfile = samlConfiguration.getSingleLogoutProfile();

        try {
            singleLogoutProfile.sendLogoutRequest(context, credential);
            samlLogger.log(SAMLConstants.LOGOUT_REQUEST, SAMLConstants.SUCCESS, context);
        } catch (Exception e) {
            samlLogger.log(SAMLConstants.LOGOUT_REQUEST, SAMLConstants.FAILURE, context);
            throw new RuntimeException("Unable to initiate SAML logout request: " + e.getMessage(), e);
        }
    }

    @Override
    public void processLogout(final HttpServletRequest request, final HttpServletResponse response, final Map<String, String> parameters) {
        verifyReadyForSamlOperations();

        final SAMLMessageContext context;
        try {
            final NiFiSAMLContextProvider contextProvider = samlConfiguration.getContextProvider();
            context = contextProvider.getLocalAndPeerEntity(request, response, parameters);
        } catch (MetadataProviderException e) {
            throw new IllegalStateException("Unable to create SAML Message Context: " + e.getMessage(), e);
        }

        final SAMLProcessor samlProcessor = samlConfiguration.getProcessor();
        try {
            samlProcessor.retrieveMessage(context);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load SAML message: " + e.getMessage(), e);
        }

        // Override set values
        context.setCommunicationProfileId(SAMLConstants.SAML2_SLO_PROFILE_URI);
        try {
            context.setLocalEntityEndpoint(getLocalEntityEndpoint(context));
        } catch (SAMLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        // Determine if the incoming SAML messages is a response to a logout we initiated, or a request initiated by the IDP
        if (context.getInboundSAMLMessage() instanceof LogoutResponse) {
            processLogoutResponse(context);
        } else if (context.getInboundSAMLMessage() instanceof LogoutRequest) {
            processLogoutRequest(context);
        }
    }

    private void processLogoutResponse(final SAMLMessageContext context) {
        final SAMLLogger samlLogger = samlConfiguration.getLogger();
        final SingleLogoutProfile logoutProfile = samlConfiguration.getSingleLogoutProfile();

        try {
            logoutProfile.processLogoutResponse(context);
            samlLogger.log(SAMLConstants.LOGOUT_RESPONSE, SAMLConstants.SUCCESS, context);
        } catch (Exception e) {
            LOGGER.error("Received logout response is invalid", e);
            samlLogger.log(SAMLConstants.LOGOUT_RESPONSE, SAMLConstants.FAILURE, context, e);
            throw new RuntimeException("Received logout response is invalid: " + e.getMessage(), e);
        }
    }

    private void processLogoutRequest(final SAMLMessageContext context) {
        throw new UnsupportedOperationException("Apache NiFi currently does not support IDP initiated logout");
    }

    private Endpoint getLocalEntityEndpoint(final SAMLMessageContext context) throws SAMLException {
        return SAMLUtil.getEndpoint(
                context.getLocalEntityRoleMetadata().getEndpoints(),
                context.getInboundSAMLBinding(),
                context.getInboundMessageTransport(),
                uriComparator);
    }

    private void initializeServiceProviderMetadata(final String baseUrl) throws MetadataProviderException {
        // Create filters so MetadataGenerator can get URLs, but we don't actually use the filters, the filter
        // paths are the URLs from AccessResource that match up with the corresponding SAML endpoint
        final SAMLProcessingFilter ssoProcessingFilter = new SAMLProcessingFilter();
        ssoProcessingFilter.setFilterProcessesUrl(SAMLEndpoints.LOGIN_CONSUMER);

        final LogoutHandler noOpLogoutHandler = (request, response, authentication) -> {
            return;
        };
        final SAMLLogoutProcessingFilter sloProcessingFilter = new SAMLLogoutProcessingFilter("/nifi", noOpLogoutHandler);
        sloProcessingFilter.setFilterProcessesUrl(SAMLEndpoints.SINGLE_LOGOUT_CONSUMER);

        // Create the MetadataGenerator...
        final MetadataGenerator metadataGenerator = new MetadataGenerator();
        metadataGenerator.setEntityId(samlConfiguration.getSpEntityId());
        metadataGenerator.setEntityBaseURL(baseUrl);
        metadataGenerator.setExtendedMetadata(samlConfiguration.getExtendedMetadata());
        metadataGenerator.setIncludeDiscoveryExtension(false);
        metadataGenerator.setKeyManager(samlConfiguration.getKeyManager());
        metadataGenerator.setSamlWebSSOFilter(ssoProcessingFilter);
        metadataGenerator.setSamlLogoutProcessingFilter(sloProcessingFilter);
        metadataGenerator.setRequestSigned(samlConfiguration.isRequestSigningEnabled());
        metadataGenerator.setWantAssertionSigned(samlConfiguration.isWantAssertionsSigned());

        // Generate service provider metadata...
        final EntityDescriptor descriptor = metadataGenerator.generateMetadata();
        final ExtendedMetadata extendedMetadata = metadataGenerator.generateExtendedMetadata();

        // Create the MetadataProvider to hold SP metadata
        final MetadataMemoryProvider memoryProvider = new MetadataMemoryProvider(descriptor);
        memoryProvider.initialize();

        final MetadataProvider spMetadataProvider = new ExtendedMetadataDelegate(memoryProvider, extendedMetadata);

        // Update the MetadataManager with the service provider MetadataProvider
        final MetadataManager metadataManager = samlConfiguration.getMetadataManager();
        metadataManager.addMetadataProvider(spMetadataProvider);
        metadataManager.setHostedSPName(descriptor.getEntityID());
        metadataManager.refreshMetadata();
    }

    private void verifyReadyForSamlOperations() {
        if (!isSamlEnabled()) {
            throw new IllegalStateException(SAML_SUPPORT_IS_NOT_CONFIGURED);
        }

        if (!initialized.get()) {
            throw new IllegalStateException("StandardSAMLService has not been initialized");
        }

        if (!isServiceProviderInitialized()) {
            throw new IllegalStateException("Service Provider is not initialized");
        }
    }

}
