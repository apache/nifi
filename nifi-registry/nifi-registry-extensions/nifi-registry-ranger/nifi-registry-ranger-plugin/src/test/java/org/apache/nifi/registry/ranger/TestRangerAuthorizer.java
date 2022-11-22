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
package org.apache.nifi.registry.ranger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authorization.AuthorizationRequest;
import org.apache.nifi.registry.security.authorization.AuthorizationResult;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.AuthorizerInitializationContext;
import org.apache.nifi.registry.security.authorization.ConfigurableUserGroupProvider;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.Resource;
import org.apache.nifi.registry.security.authorization.UserContextKeys;
import org.apache.nifi.registry.security.authorization.UserGroupProvider;
import org.apache.nifi.registry.security.authorization.UserGroupProviderLookup;
import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.util.StandardPropertyValue;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRangerAuthorizer {

    private static final String TENANT_FINGERPRINT =
            "<tenants>"
                    + "<user identifier=\"user-id-1\" identity=\"user-1\"></user>"
                    + "<group identifier=\"group-id-1\" name=\"group-1\">"
                    + "<groupUser identifier=\"user-id-1\"></groupUser>"
                    + "</group>"
                    + "</tenants>";

    private static final String EMPTY_FINGERPRINT = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>"
            + "<managedRangerAuthorizations>"
            + "<userGroupProvider/>"
            + "</managedRangerAuthorizations>";

    private static final String NON_EMPTY_FINGERPRINT = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>"
            + "<managedRangerAuthorizations>"
            + "<userGroupProvider>"
            + "&lt;tenants&gt;"
            + "&lt;user identifier=\"user-id-1\" identity=\"user-1\"&gt;&lt;/user&gt;"
            + "&lt;group identifier=\"group-id-1\" name=\"group-1\"&gt;"
            + "&lt;groupUser identifier=\"user-id-1\"&gt;&lt;/groupUser&gt;"
            + "&lt;/group&gt;"
            + "&lt;/tenants&gt;"
            + "</userGroupProvider>"
            + "</managedRangerAuthorizations>";

    private MockRangerAuthorizer authorizer;
    private RangerBasePluginWithPolicies rangerBasePlugin;

    private final String serviceType = "nifiRegistryService";
    private final String appId = "nifiRegistryAppId";

    private RangerAccessResult allowedResult;
    private RangerAccessResult notAllowedResult;
    private Map<String, String> authorizersXmlContent = null;

    @BeforeEach
    public void initialization() {
        authorizersXmlContent = Stream.of(new String[][] {
                {RangerAuthorizer.USER_GROUP_PROVIDER, "user-group-provider"},
                {RangerAuthorizer.RANGER_SECURITY_PATH_PROP, "src/test/resources/ranger/ranger-nifi-registry-security.xml"},
                {RangerAuthorizer.RANGER_AUDIT_PATH_PROP, "src/test/resources/ranger/ranger-nifi-registry-audit.xml"},
                {RangerAuthorizer.RANGER_APP_ID_PROP, appId},
                {RangerAuthorizer.RANGER_SERVICE_TYPE_PROP, serviceType}
        }).collect(Collectors.toMap(entry -> entry[0], entry -> entry[1]));
    }

    private void setup(final NiFiRegistryProperties registryProperties,
                      final UserGroupProvider userGroupProvider,
                      final AuthorizerConfigurationContext configurationContext) {
        // have to initialize this system property before anything else
        File krb5conf = new File("src/test/resources/krb5.conf");
        assertTrue(krb5conf.exists());
        System.setProperty("java.security.krb5.conf", krb5conf.getAbsolutePath());

        // rest the authentication to simple in case any tests set it to kerberos
        final Configuration securityConf = new Configuration();
        securityConf.set(RangerAuthorizer.HADOOP_SECURITY_AUTHENTICATION, "simple");
        UserGroupInformation.setConfiguration(securityConf);

        rangerBasePlugin = mock(RangerBasePluginWithPolicies.class);

        final RangerPluginConfig pluginConfig = new RangerPluginConfig(serviceType, null, appId, null, null, null);
        when(rangerBasePlugin.getConfig()).thenReturn(pluginConfig);

        authorizer = new MockRangerAuthorizer(rangerBasePlugin);

        final UserGroupProviderLookup userGroupProviderLookup = mock(UserGroupProviderLookup.class);
        when(userGroupProviderLookup.getUserGroupProvider(eq("user-group-provider"))).thenReturn(userGroupProvider);

        final AuthorizerInitializationContext initializationContext = mock(AuthorizerInitializationContext.class);
        when(initializationContext.getUserGroupProviderLookup()).thenReturn(userGroupProviderLookup);

        authorizer.setRegistryProperties(registryProperties);
        authorizer.initialize(initializationContext);
        authorizer.onConfigured(configurationContext);

        assertFalse(UserGroupInformation.isSecurityEnabled());

        allowedResult = mock(RangerAccessResult.class);
        when(allowedResult.getIsAllowed()).thenReturn(true);

        notAllowedResult = mock(RangerAccessResult.class);
        when(notAllowedResult.getIsAllowed()).thenReturn(false);
    }

    private AuthorizerConfigurationContext createMockConfigContext() {
        AuthorizerConfigurationContext configurationContext = mock(AuthorizerConfigurationContext.class);

        for (Map.Entry<String, String> entry : authorizersXmlContent.entrySet()) {
            when(configurationContext.getProperty(eq(entry.getKey())))
                    .thenReturn(new StandardPropertyValue(entry.getValue()));
        }

        when(configurationContext.getProperties()).thenReturn(authorizersXmlContent);

        return configurationContext;
    }

    @Test
    public void testOnConfigured() {
        setup(mock(NiFiRegistryProperties.class), mock(UserGroupProvider.class), createMockConfigContext());

        verify(rangerBasePlugin, times(1)).init();

        assertEquals(appId, authorizer.mockRangerBasePlugin.getAppId());
        assertEquals(serviceType, authorizer.mockRangerBasePlugin.getServiceType());
    }

    @Test
    public void testKerberosEnabledWithoutKeytab() {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();

        when(configurationContext.getProperty(eq(RangerAuthorizer.RANGER_KERBEROS_ENABLED_PROP)))
                .thenReturn(new StandardPropertyValue("true"));

        NiFiRegistryProperties registryProperties = mock(NiFiRegistryProperties.class);
        when(registryProperties.getKerberosServicePrincipal()).thenReturn("");

        assertThrows(SecurityProviderCreationException.class, () -> setup(registryProperties, mock(UserGroupProvider.class), configurationContext));
    }

    @Test
    public void testKerberosEnabledWithoutPrincipal() {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();

        when(configurationContext.getProperty(eq(RangerAuthorizer.RANGER_KERBEROS_ENABLED_PROP)))
                .thenReturn(new StandardPropertyValue("true"));

        NiFiRegistryProperties registryProperties = mock(NiFiRegistryProperties.class);
        when(registryProperties.getKerberosServiceKeytabLocation()).thenReturn("");

        assertThrows(SecurityProviderCreationException.class, () -> setup(registryProperties, mock(UserGroupProvider.class), configurationContext));
    }

    @Test
    public void testKerberosEnabledWithoutKeytabOrPrincipal() {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();

        when(configurationContext.getProperty(eq(RangerAuthorizer.RANGER_KERBEROS_ENABLED_PROP)))
                .thenReturn(new StandardPropertyValue("true"));

        NiFiRegistryProperties registryProperties = mock(NiFiRegistryProperties.class);
        when(registryProperties.getKerberosServiceKeytabLocation()).thenReturn("");
        when(registryProperties.getKerberosServicePrincipal()).thenReturn("");

        assertThrows(SecurityProviderCreationException.class, () -> setup(registryProperties, mock(UserGroupProvider.class), configurationContext));
    }

    @Test
    public void testKerberosEnabled() {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();

        when(configurationContext.getProperty(eq(RangerAuthorizer.RANGER_KERBEROS_ENABLED_PROP)))
                .thenReturn(new StandardPropertyValue("true"));

        NiFiRegistryProperties registryProperties = mock(NiFiRegistryProperties.class);
        when(registryProperties.getKerberosServiceKeytabLocation()).thenReturn("test");
        when(registryProperties.getKerberosServicePrincipal()).thenReturn("test");

        assertThrows(SecurityProviderCreationException.class, () -> setup(registryProperties, mock(UserGroupProvider.class), configurationContext));
    }

    @Test
    public void testApprovedWithDirectAccess() {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), mock(UserGroupProvider.class), configurationContext);

        final String systemResource = "/system";
        final RequestAction action = RequestAction.WRITE;
        final String user = "admin";
        final String clientIp = "192.168.1.1";

        final Map<String,String> userContext = new HashMap<>();
        userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), clientIp);

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(systemResource, systemResource))
                .action(action)
                .identity(user)
                .resourceContext(new HashMap<>())
                .userContext(userContext)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerAuthorizer.RANGER_NIFI_REG_RESOURCE_NAME, systemResource);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());
        expectedRangerRequest.setClientIPAddress(clientIp);

        // a non-null result processor should be used for direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)))
        ).thenReturn(allowedResult);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.approved().getResult(), result.getResult());
    }

    @Test
    public void testApprovedWithNonDirectAccess() {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), mock(UserGroupProvider.class), configurationContext);

        final String systemResource = "/system";
        final RequestAction action = RequestAction.WRITE;
        final String user = "admin";

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(systemResource, systemResource))
                .action(action)
                .identity(user)
                .resourceContext(new HashMap<>())
                .accessAttempt(false)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerAuthorizer.RANGER_NIFI_REG_RESOURCE_NAME, systemResource);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());

        // no result processor should be provided used non-direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)))
        ).thenReturn(allowedResult);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.approved().getResult(), result.getResult());
    }

    @Test
    public void testResourceNotFound() {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), mock(UserGroupProvider.class), configurationContext);

        final String systemResource = "/system";
        final RequestAction action = RequestAction.WRITE;
        final String user = "admin";

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(systemResource, systemResource))
                .action(action)
                .identity(user)
                .resourceContext(new HashMap<>())
                .accessAttempt(true)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerAuthorizer.RANGER_NIFI_REG_RESOURCE_NAME, systemResource);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());

        // no result processor should be provided used non-direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)),
                any(RangerAccessResultProcessor.class))
        ).thenReturn(notAllowedResult);

        // return false when checking if a policy exists for the resource
        when(rangerBasePlugin.doesPolicyExist(systemResource, action)).thenReturn(false);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.resourceNotFound().getResult(), result.getResult());
    }

    @Test
    public void testDenied() {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), mock(UserGroupProvider.class), configurationContext);

        final String systemResource = "/system";
        final RequestAction action = RequestAction.WRITE;
        final String user = "admin";

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(systemResource, systemResource))
                .action(action)
                .identity(user)
                .resourceContext(new HashMap<>())
                .accessAttempt(true)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerAuthorizer.RANGER_NIFI_REG_RESOURCE_NAME, systemResource);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());

        // no result processor should be provided used non-direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)))
        ).thenReturn(notAllowedResult);

        // return true when checking if a policy exists for the resource
        when(rangerBasePlugin.doesPolicyExist(systemResource, action)).thenReturn(true);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.denied().getResult(), result.getResult());
    }

    @Test
    public void testRangerAdminApproved() {
        final String acceptableIdentity = "ranger-admin";
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX, acceptableIdentity);

        final String requestIdentity = "ranger-admin";
        runRangerAdminTest(RangerAuthorizer.RESOURCES_RESOURCE, requestIdentity, AuthorizationResult.approved().getResult());
    }

    @Test
    public void testRangerAdminApprovedMultipleAcceptableIdentities() {
        final String acceptableIdentity1 = "ranger-admin1";
        final String acceptableIdentity2 = "ranger-admin2";
        final String acceptableIdentity3 = "ranger-admin3";
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX, acceptableIdentity1);
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX + " 2", acceptableIdentity2);
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX + " 3", acceptableIdentity3);

        final String requestIdentity = "ranger-admin2";
        runRangerAdminTest(RangerAuthorizer.RESOURCES_RESOURCE, requestIdentity, AuthorizationResult.approved().getResult());
    }

    @Test
    public void testRangerAdminApprovedMultipleAcceptableIdentities2() {
        final String acceptableIdentity1 = "ranger-admin1";
        final String acceptableIdentity2 = "ranger-admin2";
        final String acceptableIdentity3 = "ranger-admin3";
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX, acceptableIdentity1);
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX + " 2", acceptableIdentity2);
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX + " 3", acceptableIdentity3);

        final String requestIdentity = "ranger-admin3";
        runRangerAdminTest(RangerAuthorizer.RESOURCES_RESOURCE, requestIdentity, AuthorizationResult.approved().getResult());
    }

    @Test
    public void testRangerAdminDenied() {
        final String acceptableIdentity = "ranger-admin";
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX, acceptableIdentity);

        final String requestIdentity = "ranger-admin";
        runRangerAdminTest("/flow", requestIdentity, AuthorizationResult.denied().getResult());
    }

    @Test
    public void testRangerAdminDeniedMultipleAcceptableIdentities() {
        final String acceptableIdentity1 = "ranger-admin1";
        final String acceptableIdentity2 = "ranger-admin2";
        final String acceptableIdentity3 = "ranger-admin3";
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX, acceptableIdentity1);
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX + " 2", acceptableIdentity2);
        authorizersXmlContent.put(RangerAuthorizer.RANGER_ADMIN_IDENTITY_PROP_PREFIX + " 3", acceptableIdentity3);

        final String requestIdentity = "ranger-admin4";
        runRangerAdminTest(RangerAuthorizer.RESOURCES_RESOURCE, requestIdentity, AuthorizationResult.denied().getResult());
    }

    private void runRangerAdminTest(final String resourceIdentifier, final String requestIdentity, final AuthorizationResult.Result expectedResult) {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();

        setup(mock(NiFiRegistryProperties.class), mock(UserGroupProvider.class), configurationContext);

        final RequestAction action = RequestAction.WRITE;

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(resourceIdentifier, resourceIdentifier))
                .action(action)
                .identity(requestIdentity)
                .resourceContext(new HashMap<>())
                .accessAttempt(true)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerAuthorizer.RANGER_NIFI_REG_RESOURCE_NAME, resourceIdentifier);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());

        // return true when checking if a policy exists for the resource
        when(rangerBasePlugin.doesPolicyExist(resourceIdentifier, action)).thenReturn(true);

        // a non-null result processor should be used for direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)))
        ).thenReturn(notAllowedResult);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(expectedResult, result.getResult());
    }

    /**
     * Extend RangerAuthorizer to inject a mock base plugin for testing.
     */
    private static class MockRangerAuthorizer extends RangerAuthorizer {

        RangerBasePluginWithPolicies mockRangerBasePlugin;

        MockRangerAuthorizer(RangerBasePluginWithPolicies mockRangerBasePlugin) {
            this.mockRangerBasePlugin = mockRangerBasePlugin;
        }

        @Override
        protected RangerBasePluginWithPolicies createRangerBasePlugin(String serviceType, String appId) {
            when(mockRangerBasePlugin.getAppId()).thenReturn(appId);
            when(mockRangerBasePlugin.getServiceType()).thenReturn(serviceType);
            return mockRangerBasePlugin;
        }
    }

    /**
     * Resource implementation for testing.
     */
    private static class MockResource implements Resource {

        private final String identifier;
        private final String name;

        MockResource(String identifier, String name) {
            this.identifier = identifier;
            this.name = name;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getSafeDescription() {
            return name;
        }
    }

    /**
     * Custom Mockito matcher for RangerAccessRequest objects.
     */
    private static class RangerAccessRequestMatcher implements ArgumentMatcher<RangerAccessRequest> {

        private final RangerAccessRequest request;

        RangerAccessRequestMatcher(RangerAccessRequest request) {
            this.request = request;
        }

        @Override
        public boolean matches(RangerAccessRequest other) {
            final boolean clientIpsMatch = (other.getClientIPAddress() == null && request.getClientIPAddress() == null)
                    || (other.getClientIPAddress() != null && request.getClientIPAddress() != null && other.getClientIPAddress().equals(request.getClientIPAddress()));

            return other.getResource().equals(request.getResource())
                    && other.getAccessType().equals(request.getAccessType())
                    && other.getAction().equals(request.getAction())
                    && other.getUser().equals(request.getUser())
                    && clientIpsMatch;
        }
    }

    @Test
    public void testNonConfigurableFingerPrint() {
        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), mock(UserGroupProvider.class), configurationContext);

        assertEquals(EMPTY_FINGERPRINT, authorizer.getFingerprint());
    }

    @Test
    public void testConfigurableEmptyFingerPrint() {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);
        when(userGroupProvider.getFingerprint()).thenReturn("");

        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), userGroupProvider, configurationContext);

        assertEquals(EMPTY_FINGERPRINT, authorizer.getFingerprint());
    }

    @Test
    public void testConfigurableFingerPrint() {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);
        when(userGroupProvider.getFingerprint()).thenReturn(TENANT_FINGERPRINT);

        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), userGroupProvider, configurationContext);

        assertEquals(NON_EMPTY_FINGERPRINT, authorizer.getFingerprint());
    }

    @Test
    public void testInheritEmptyFingerprint() {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), userGroupProvider, configurationContext);

        authorizer.inheritFingerprint(EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(0)).inheritFingerprint(anyString());
    }

    @Test
    public void testInheritInvalidFingerprint() {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), userGroupProvider, configurationContext);

        assertThrows(AuthorizationAccessException.class, () -> authorizer.inheritFingerprint("not a valid fingerprint"));
    }

    @Test
    public void testInheritNonEmptyFingerprint() {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), userGroupProvider, configurationContext);

        authorizer.inheritFingerprint(NON_EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(1)).inheritFingerprint(TENANT_FINGERPRINT);
    }

    @Test
    public void testCheckInheritEmptyFingerprint() {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), userGroupProvider, configurationContext);

        authorizer.checkInheritability(EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(0)).inheritFingerprint(anyString());
    }

    @Test
    public void testCheckInheritInvalidFingerprint() {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), userGroupProvider, configurationContext);

        assertThrows(AuthorizationAccessException.class, () -> authorizer.checkInheritability("not a valid fingerprint"));
    }

    @Test
    public void testCheckInheritNonEmptyFingerprint() {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), userGroupProvider, configurationContext);

        authorizer.checkInheritability(NON_EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(1)).checkInheritability(TENANT_FINGERPRINT);
    }

    @Test
    public void testCheckInheritNonConfigurableUserGroupProvider() {
        final UserGroupProvider userGroupProvider = mock(UserGroupProvider.class);

        final AuthorizerConfigurationContext configurationContext = createMockConfigContext();
        setup(mock(NiFiRegistryProperties.class), userGroupProvider, configurationContext);

        assertThrows(UninheritableAuthorizationsException.class, () -> authorizer.checkInheritability(NON_EMPTY_FINGERPRINT));
    }

}
