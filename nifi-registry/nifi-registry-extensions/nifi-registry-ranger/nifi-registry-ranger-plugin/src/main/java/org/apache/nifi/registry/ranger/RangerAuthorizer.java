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
package org.apache.nifi.registry.ranger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authorization.AccessPolicy;
import org.apache.nifi.registry.security.authorization.AccessPolicyProvider;
import org.apache.nifi.registry.security.authorization.AccessPolicyProviderInitializationContext;
import org.apache.nifi.registry.security.authorization.AuthorizationAuditor;
import org.apache.nifi.registry.security.authorization.AuthorizationRequest;
import org.apache.nifi.registry.security.authorization.AuthorizationResult;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.AuthorizerInitializationContext;
import org.apache.nifi.registry.security.authorization.ConfigurableUserGroupProvider;
import org.apache.nifi.registry.security.authorization.ManagedAuthorizer;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.UserContextKeys;
import org.apache.nifi.registry.security.authorization.UserGroupProvider;
import org.apache.nifi.registry.security.authorization.UserGroupProviderLookup;
import org.apache.nifi.registry.security.authorization.annotation.AuthorizerContext;
import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.util.PropertyValue;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Authorizer implementation that uses Apache Ranger to make authorization decisions.
 */
public class RangerAuthorizer implements ManagedAuthorizer, AuthorizationAuditor {

    private static final Logger logger = LoggerFactory.getLogger(RangerAuthorizer.class);

    private static final DocumentBuilderFactory DOCUMENT_BUILDER_FACTORY = DocumentBuilderFactory.newInstance();

    private static final String USER_GROUP_PROVIDER_ELEMENT = "userGroupProvider";

    static final String USER_GROUP_PROVIDER = "User Group Provider";

    static final String RANGER_AUDIT_PATH_PROP = "Ranger Audit Config Path";
    static final String RANGER_SECURITY_PATH_PROP = "Ranger Security Config Path";
    static final String RANGER_KERBEROS_ENABLED_PROP = "Ranger Kerberos Enabled";
    static final String RANGER_ADMIN_IDENTITY_PROP = "Ranger Admin Identity";
    static final String RANGER_SERVICE_TYPE_PROP = "Ranger Service Type";
    static final String RANGER_APP_ID_PROP = "Ranger Application Id";

    static final String RANGER_NIFI_REG_RESOURCE_NAME = "nifi-registry-resource";
    private static final String DEFAULT_SERVICE_TYPE = "nifi-registry";
    private static final String DEFAULT_APP_ID = "nifi-registry";
    static final String RESOURCES_RESOURCE = "/policies";
    static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    private static final String KERBEROS_AUTHENTICATION = "kerberos";

    private final Map<AuthorizationRequest, RangerAccessResult> resultLookup = new WeakHashMap<>();

    private volatile RangerBasePluginWithPolicies rangerPlugin = null;
    private volatile RangerDefaultAuditHandler defaultAuditHandler = null;
    private volatile String rangerAdminIdentity = null;
    private volatile NiFiRegistryProperties registryProperties;

    private UserGroupProviderLookup userGroupProviderLookup;
    private UserGroupProvider userGroupProvider;


    @Override
    public void initialize(AuthorizerInitializationContext initializationContext) throws SecurityProviderCreationException {
        userGroupProviderLookup = initializationContext.getUserGroupProviderLookup();
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
        final String userGroupProviderKey = configurationContext.getProperty(USER_GROUP_PROVIDER).getValue();
        if (StringUtils.isEmpty(userGroupProviderKey)) {
            throw new SecurityProviderCreationException(USER_GROUP_PROVIDER + " must be specified.");
        }
        userGroupProvider = userGroupProviderLookup.getUserGroupProvider(userGroupProviderKey);

        // ensure the desired access policy provider has a user group provider
        if (userGroupProvider == null) {
            throw new SecurityProviderCreationException(String.format("Unable to locate configured User Group Provider: %s", userGroupProviderKey));
        }

        try {
            if (rangerPlugin == null) {
                logger.info("initializing base plugin");

                final String serviceType = getConfigValue(configurationContext, RANGER_SERVICE_TYPE_PROP, DEFAULT_SERVICE_TYPE);
                final String appId = getConfigValue(configurationContext, RANGER_APP_ID_PROP, DEFAULT_APP_ID);

                rangerPlugin = createRangerBasePlugin(serviceType, appId);

                final RangerPluginConfig pluginConfig = rangerPlugin.getConfig();

                final PropertyValue securityConfigValue = configurationContext.getProperty(RANGER_SECURITY_PATH_PROP);
                addRequiredResource(RANGER_SECURITY_PATH_PROP, securityConfigValue, pluginConfig);

                final PropertyValue auditConfigValue = configurationContext.getProperty(RANGER_AUDIT_PATH_PROP);
                addRequiredResource(RANGER_AUDIT_PATH_PROP, auditConfigValue, pluginConfig);

                boolean rangerKerberosEnabled = Boolean.valueOf(getConfigValue(configurationContext, RANGER_KERBEROS_ENABLED_PROP, Boolean.FALSE.toString()));

                if (rangerKerberosEnabled) {
                    // configure UGI for when RangerAdminRESTClient calls UserGroupInformation.isSecurityEnabled()
                    final Configuration securityConf = new Configuration();
                    securityConf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS_AUTHENTICATION);
                    UserGroupInformation.setConfiguration(securityConf);

                    // login with the nifi registry principal and keytab, RangerAdminRESTClient will use Ranger's MiscUtil which
                    // will grab UserGroupInformation.getLoginUser() and call ugi.checkTGTAndReloginFromKeytab();
                    final String registryPrincipal = registryProperties.getKerberosServicePrincipal();
                    final String registryKeytab = registryProperties.getKerberosServiceKeytabLocation();

                    if (StringUtils.isBlank(registryPrincipal) || StringUtils.isBlank(registryKeytab)) {
                        throw new SecurityProviderCreationException("Principal and Keytab must be provided when Kerberos is enabled");
                    }

                    UserGroupInformation.loginUserFromKeytab(registryPrincipal.trim(), registryKeytab.trim());
                }

                rangerPlugin.init();

                defaultAuditHandler = new RangerDefaultAuditHandler();
                rangerAdminIdentity = getConfigValue(configurationContext, RANGER_ADMIN_IDENTITY_PROP, null);

            } else {
                logger.info("base plugin already initialized");
            }
        } catch (Throwable t) {
            throw new SecurityProviderCreationException("Error creating RangerBasePlugin", t);
        }
    }

    protected RangerBasePluginWithPolicies createRangerBasePlugin(final String serviceType, final String appId) {
        return new RangerBasePluginWithPolicies(serviceType, appId, userGroupProvider);
    }

    @Override
    public AuthorizationResult authorize(final AuthorizationRequest request) throws SecurityProviderCreationException {
        final String identity = request.getIdentity();
        final Set<String> userGroups = request.getGroups();
        final String resourceIdentifier = request.getResource().getIdentifier();

        // if a ranger admin identity was provided, and it equals the identity making the request,
        // and the request is to retrieve the resources, then allow it through
        if (StringUtils.isNotBlank(rangerAdminIdentity) && rangerAdminIdentity.equals(identity)
                && resourceIdentifier.equals(RESOURCES_RESOURCE)) {
            return AuthorizationResult.approved();
        }

        final String clientIp;
        if (request.getUserContext() != null) {
            clientIp = request.getUserContext().get(UserContextKeys.CLIENT_ADDRESS.name());
        } else {
            clientIp = null;
        }

        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RANGER_NIFI_REG_RESOURCE_NAME, resourceIdentifier);

        final RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl();
        rangerRequest.setResource(resource);
        rangerRequest.setAction(request.getAction().name());
        rangerRequest.setAccessType(request.getAction().name());
        rangerRequest.setUser(identity);
        rangerRequest.setUserGroups(userGroups);
        rangerRequest.setAccessTime(new Date());

        if (!StringUtils.isBlank(clientIp)) {
            rangerRequest.setClientIPAddress(clientIp);
        }

        final RangerAccessResult result = rangerPlugin.isAccessAllowed(rangerRequest);

        // store the result for auditing purposes later if appropriate
        if (request.isAccessAttempt()) {
            synchronized (resultLookup) {
                resultLookup.put(request, result);
            }
        }

        if (result != null && result.getIsAllowed()) {
            // return approved
            return AuthorizationResult.approved();
        } else {
            // if result.getIsAllowed() is false, then we need to determine if it was because no policy exists for the
            // given resource, or if it was because a policy exists but not for the given user or action
            final boolean doesPolicyExist = rangerPlugin.doesPolicyExist(request.getResource().getIdentifier(), request.getAction());

            if (doesPolicyExist) {
                final String reason = result == null ? null : result.getReason();
                if (reason != null) {
                    logger.debug(String.format("Unable to authorize %s due to %s", identity, reason));
                }

                // a policy does exist for the resource so we were really denied access here
                return AuthorizationResult.denied(request.getExplanationSupplier().get());
            } else {
                // a policy doesn't exist so return resource not found so NiFi Registry can work back up the resource hierarchy
                return AuthorizationResult.resourceNotFound();
            }
        }
    }

    @Override
    public void auditAccessAttempt(final AuthorizationRequest request, final AuthorizationResult result) {
        final RangerAccessResult rangerResult;
        synchronized (resultLookup) {
            rangerResult = resultLookup.remove(request);
        }

        if (rangerResult != null && rangerResult.getIsAudited()) {
            AuthzAuditEvent event = defaultAuditHandler.getAuthzEvents(rangerResult);

            // update the event with the originally requested resource
            event.setResourceType(RANGER_NIFI_REG_RESOURCE_NAME);
            event.setResourcePath(request.getRequestedResource().getIdentifier());

            defaultAuditHandler.logAuthzAudit(event);
        }
    }

    @Override
    public void preDestruction() throws SecurityProviderCreationException {
        if (rangerPlugin != null) {
            try {
                rangerPlugin.cleanup();
                rangerPlugin = null;
            } catch (Throwable t) {
                throw new SecurityProviderCreationException("Error cleaning up RangerBasePlugin", t);
            }
        }
    }

    @AuthorizerContext
    public void setRegistryProperties(final NiFiRegistryProperties properties) {
        this.registryProperties = properties;
    }

    /**
     * Adds a resource to the RangerConfiguration singleton so it is already there by the time RangerBasePlugin.init()
     * is called.
     *
     * @param name          the name of the given PropertyValue from the AuthorizationConfigurationContext
     * @param resourceValue the value for the given name, should be a full path to a file
     * @param configuration the RangerConfiguration to add the resource to
     */
    private void addRequiredResource(final String name, final PropertyValue resourceValue, final RangerConfiguration configuration) {
        if (resourceValue == null || StringUtils.isBlank(resourceValue.getValue())) {
            throw new SecurityProviderCreationException(name + " must be specified.");
        }

        final File resourceFile = new File(resourceValue.getValue());
        if (!resourceFile.exists() || !resourceFile.canRead()) {
            throw new SecurityProviderCreationException(resourceValue + " does not exist, or can not be read");
        }

        try {
            configuration.addResource(resourceFile.toURI().toURL());
        } catch (MalformedURLException e) {
            throw new SecurityProviderCreationException("Error creating URI for " + resourceValue, e);
        }
    }

    private String getConfigValue(final AuthorizerConfigurationContext context, final String name, final String defaultValue) {
        final PropertyValue configValue = context.getProperty(name);

        String retValue = defaultValue;
        if (configValue != null && !StringUtils.isBlank(configValue.getValue())) {
            retValue = configValue.getValue();
        }

        return retValue;
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        final StringWriter out = new StringWriter();
        try {
            // create the document
            final DocumentBuilder documentBuilder = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
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

    private String parseFingerprint(final String fingerprint) throws AuthorizationAccessException {
        final byte[] fingerprintBytes = fingerprint.getBytes(StandardCharsets.UTF_8);

        try (final ByteArrayInputStream in = new ByteArrayInputStream(fingerprintBytes)) {
            final DocumentBuilder docBuilder = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
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

    @Override
    public AccessPolicyProvider getAccessPolicyProvider() {
        return new AccessPolicyProvider() {
            @Override
            public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                return rangerPlugin.getAccessPolicies();
            }

            @Override
            public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                return rangerPlugin.getAccessPolicy(identifier);
            }

            @Override
            public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
                return rangerPlugin.getAccessPolicy(resourceIdentifier, action);
            }

            @Override
            public UserGroupProvider getUserGroupProvider() {
                return userGroupProvider;
            }

            @Override
            public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws SecurityProviderCreationException {
            }

            @Override
            public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
            }

            @Override
            public void preDestruction() throws SecurityProviderCreationException {
            }
        };
    }
}