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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.UserContextKeys;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.NiFiProperties;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRangerNiFiAuthorizer {

    private MockRangerNiFiAuthorizer authorizer;
    private RangerBasePluginWithPolicies rangerBasePlugin;
    private AuthorizerConfigurationContext configurationContext;
    private NiFiProperties nifiProperties;

    private final String serviceType = "nifiService";
    private final String appId = "nifiAppId";

    private RangerAccessResult allowedResult;
    private RangerAccessResult notAllowedResult;

    @Before
    public void setup() {
        // have to initialize this system property before anything else
        File krb5conf = new File("src/test/resources/krb5.conf");
        assertTrue(krb5conf.exists());
        System.setProperty("java.security.krb5.conf", krb5conf.getAbsolutePath());

        // rest the authentication to simple in case any tests set it to kerberos
        final Configuration securityConf = new Configuration();
        securityConf.set(RangerNiFiAuthorizer.HADOOP_SECURITY_AUTHENTICATION, "simple");
        UserGroupInformation.setConfiguration(securityConf);

        configurationContext = createMockConfigContext();
        rangerBasePlugin = Mockito.mock(RangerBasePluginWithPolicies.class);
        authorizer = new MockRangerNiFiAuthorizer(rangerBasePlugin);
        authorizer.onConfigured(configurationContext);

        assertFalse(UserGroupInformation.isSecurityEnabled());

        allowedResult = Mockito.mock(RangerAccessResult.class);
        when(allowedResult.getIsAllowed()).thenReturn(true);

        notAllowedResult = Mockito.mock(RangerAccessResult.class);
        when(notAllowedResult.getIsAllowed()).thenReturn(false);
    }

    private AuthorizerConfigurationContext createMockConfigContext() {
        AuthorizerConfigurationContext configurationContext = Mockito.mock(AuthorizerConfigurationContext.class);

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_SECURITY_PATH_PROP)))
                .thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-security.xml"));

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_AUDIT_PATH_PROP)))
                .thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-audit.xml"));

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_APP_ID_PROP)))
                .thenReturn(new MockPropertyValue(appId));

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_SERVICE_TYPE_PROP)))
                .thenReturn(new MockPropertyValue(serviceType));

        return configurationContext;
    }

    @Test
    public void testOnConfigured() {
        verify(rangerBasePlugin, times(1)).init();

        assertEquals(appId, authorizer.mockRangerBasePlugin.getAppId());
        assertEquals(serviceType, authorizer.mockRangerBasePlugin.getServiceType());
    }

    @Test
    public void testKerberosEnabledWithoutKeytab() {
        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_KERBEROS_ENABLED_PROP)))
                .thenReturn(new MockPropertyValue("true"));

        nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.getKerberosServicePrincipal()).thenReturn("");

        authorizer = new MockRangerNiFiAuthorizer(rangerBasePlugin);
        authorizer.setNiFiProperties(nifiProperties);

        try {
            authorizer.onConfigured(configurationContext);
            Assert.fail("Should have thrown exception");
        } catch (AuthorizerCreationException e) {
            // want to make sure this exception is from our authorizer code
            verifyOnlyAuthorizeCreationExceptions(e);
        }
    }

    @Test
    public void testKerberosEnabledWithoutPrincipal() {
        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_KERBEROS_ENABLED_PROP)))
                .thenReturn(new MockPropertyValue("true"));

        nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.getKerberosServiceKeytabLocation()).thenReturn("");

        authorizer = new MockRangerNiFiAuthorizer(rangerBasePlugin);
        authorizer.setNiFiProperties(nifiProperties);

        try {
            authorizer.onConfigured(configurationContext);
            Assert.fail("Should have thrown exception");
        } catch (AuthorizerCreationException e) {
            // want to make sure this exception is from our authorizer code
            verifyOnlyAuthorizeCreationExceptions(e);
        }
    }

    @Test
    public void testKerberosEnabledWithoutKeytabOrPrincipal() {
        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_KERBEROS_ENABLED_PROP)))
                .thenReturn(new MockPropertyValue("true"));

        nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.getKerberosServiceKeytabLocation()).thenReturn("");
        when(nifiProperties.getKerberosServicePrincipal()).thenReturn("");

        authorizer = new MockRangerNiFiAuthorizer(rangerBasePlugin);
        authorizer.setNiFiProperties(nifiProperties);

        try {
            authorizer.onConfigured(configurationContext);
            Assert.fail("Should have thrown exception");
        } catch (AuthorizerCreationException e) {
            // want to make sure this exception is from our authorizer code
            verifyOnlyAuthorizeCreationExceptions(e);
        }
    }

    private void verifyOnlyAuthorizeCreationExceptions(AuthorizerCreationException e) {
        boolean foundOtherException = false;
        Throwable cause = e.getCause();
        while (cause != null) {
            if (!(cause instanceof AuthorizerCreationException)) {
                foundOtherException = true;
                break;
            }
            cause = cause.getCause();
        }
        assertFalse(foundOtherException);
    }

    @Test
    public void testKerberosEnabled() {
        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_KERBEROS_ENABLED_PROP)))
                .thenReturn(new MockPropertyValue("true"));

        nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.getKerberosServiceKeytabLocation()).thenReturn("test");
        when(nifiProperties.getKerberosServicePrincipal()).thenReturn("test");

        authorizer = new MockRangerNiFiAuthorizer(rangerBasePlugin);
        authorizer.setNiFiProperties(nifiProperties);

        try {
            authorizer.onConfigured(configurationContext);
            Assert.fail("Should have thrown exception");
        } catch (AuthorizerCreationException e) {
            // getting a LoginException here means we attempted to login which is what we want
            boolean foundLoginException = false;
            Throwable cause = e.getCause();
            while (cause != null) {
                if (cause instanceof LoginException) {
                    foundLoginException = true;
                    break;
                }
                cause = cause.getCause();
            }
            assertTrue(foundLoginException);
        }
    }

    @Test
    public void testApprovedWithDirectAccess() {
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
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, systemResource);

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
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, systemResource);

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
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, systemResource);

        final RangerAccessRequestImpl expectedRangerRequest = new RangerAccessRequestImpl();
        expectedRangerRequest.setResource(resource);
        expectedRangerRequest.setAction(request.getAction().name());
        expectedRangerRequest.setAccessType(request.getAction().name());
        expectedRangerRequest.setUser(request.getIdentity());

        // no result processor should be provided used non-direct access
        when(rangerBasePlugin.isAccessAllowed(
                argThat(new RangerAccessRequestMatcher(expectedRangerRequest)),
                notNull(RangerAccessResultProcessor.class))
        ).thenReturn(notAllowedResult);

        // return false when checking if a policy exists for the resource
        when(rangerBasePlugin.doesPolicyExist(systemResource, action)).thenReturn(false);

        final AuthorizationResult result = authorizer.authorize(request);
        assertEquals(AuthorizationResult.resourceNotFound().getResult(), result.getResult());
    }

    @Test
    public void testDenied() {
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
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, systemResource);

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
        runRangerAdminTest(RangerNiFiAuthorizer.RESOURCES_RESOURCE, AuthorizationResult.approved().getResult());
    }

    @Test
    public void testRangerAdminDenied() {
        runRangerAdminTest("/flow", AuthorizationResult.denied().getResult());
    }

    private void runRangerAdminTest(final String resourceIdentifier, final AuthorizationResult.Result expectedResult) {
        configurationContext = createMockConfigContext();

        final String rangerAdminIdentity = "ranger-admin";
        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_ADMIN_IDENTITY_PROP)))
                .thenReturn(new MockPropertyValue(rangerAdminIdentity));

        rangerBasePlugin = Mockito.mock(RangerBasePluginWithPolicies.class);
        authorizer = new MockRangerNiFiAuthorizer(rangerBasePlugin);
        authorizer.onConfigured(configurationContext);

        final RequestAction action = RequestAction.WRITE;

        // the incoming NiFi request to test
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(new MockResource(resourceIdentifier, resourceIdentifier))
                .action(action)
                .identity(rangerAdminIdentity)
                .resourceContext(new HashMap<>())
                .accessAttempt(true)
                .anonymous(false)
                .build();

        // the expected Ranger resource and request that are created
        final RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RangerNiFiAuthorizer.RANGER_NIFI_RESOURCE_NAME, resourceIdentifier);

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

    @Test
    @Ignore
    public void testIntegration() {
        final AuthorizerInitializationContext initializationContext = Mockito.mock(AuthorizerInitializationContext.class);
        final AuthorizerConfigurationContext configurationContext = Mockito.mock(AuthorizerConfigurationContext.class);

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_SECURITY_PATH_PROP)))
                .thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-security.xml"));

        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_AUDIT_PATH_PROP)))
                .thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-audit.xml"));

        Authorizer authorizer = new RangerNiFiAuthorizer();
        try {
            authorizer.initialize(initializationContext);
            authorizer.onConfigured(configurationContext);

            final AuthorizationRequest request = new AuthorizationRequest.Builder()
                    .resource(new Resource() {
                        @Override
                        public String getIdentifier() {
                            return "/system";
                        }

                        @Override
                        public String getName() {
                            return "/system";
                        }

                        @Override
                        public String getSafeDescription() {
                            return "system";
                        }
                    })
                    .action(RequestAction.WRITE)
                    .identity("admin")
                    .resourceContext(new HashMap<>())
                    .accessAttempt(true)
                    .anonymous(false)
                    .build();


            final AuthorizationResult result = authorizer.authorize(request);

            Assert.assertEquals(AuthorizationResult.denied().getResult(), result.getResult());

        } finally {
            authorizer.preDestruction();
        }
    }

    /**
     * Extend RangerNiFiAuthorizer to inject a mock base plugin for testing.
     */
    private static class MockRangerNiFiAuthorizer extends RangerNiFiAuthorizer {

        RangerBasePluginWithPolicies mockRangerBasePlugin;

        public MockRangerNiFiAuthorizer(RangerBasePluginWithPolicies mockRangerBasePlugin) {
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

        public MockResource(String identifier, String name) {
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
    private static class RangerAccessRequestMatcher extends ArgumentMatcher<RangerAccessRequest> {

        private final RangerAccessRequest request;

        public RangerAccessRequestMatcher(RangerAccessRequest request) {
            this.request = request;
        }

        @Override
        public boolean matches(Object o) {
            if (!(o instanceof RangerAccessRequest)) {
                return false;
            }

            final RangerAccessRequest other = (RangerAccessRequest) o;

            final boolean clientIpsMatch = (other.getClientIPAddress() == null && request.getClientIPAddress() == null)
                    || (other.getClientIPAddress() != null && request.getClientIPAddress() != null && other.getClientIPAddress().equals(request.getClientIPAddress()));

            return other.getResource().equals(request.getResource())
                    && other.getAccessType().equals(request.getAccessType())
                    && other.getAction().equals(request.getAction())
                    && other.getUser().equals(request.getUser())
                    && clientIpsMatch;
        }
    }

}
