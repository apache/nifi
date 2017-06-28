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
package org.apache.nifi.ranger.authorization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.ConfigurableUserGroupProvider;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderLookup;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ManagedRangerAuthorizerTest {

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

    private final String serviceType = "nifiService";
    private final String appId = "nifiAppId";

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

        assertFalse(UserGroupInformation.isSecurityEnabled());
    }

    @Test
    public void testNonConfigurableFingerPrint() throws Exception {
        final UserGroupProvider userGroupProvider = mock(UserGroupProvider.class);

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        Assert.assertEquals(EMPTY_FINGERPRINT, managedRangerAuthorizer.getFingerprint());
    }

    @Test
    public void testConfigurableEmptyFingerPrint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);
        when(userGroupProvider.getFingerprint()).thenReturn("");

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        Assert.assertEquals(EMPTY_FINGERPRINT, managedRangerAuthorizer.getFingerprint());
    }

    @Test
    public void testConfigurableFingerPrint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);
        when(userGroupProvider.getFingerprint()).thenReturn(TENANT_FINGERPRINT);

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        Assert.assertEquals(NON_EMPTY_FINGERPRINT, managedRangerAuthorizer.getFingerprint());
    }

    @Test
    public void testInheritEmptyFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        managedRangerAuthorizer.inheritFingerprint(EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(0)).inheritFingerprint(anyString());
    }

    @Test(expected = AuthorizationAccessException.class)
    public void testInheritInvalidFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        managedRangerAuthorizer.inheritFingerprint("not a valid fingerprint");
    }

    @Test
    public void testInheritNonEmptyFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        managedRangerAuthorizer.inheritFingerprint(NON_EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(1)).inheritFingerprint(TENANT_FINGERPRINT);
    }

    @Test
    public void testCheckInheritEmptyFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        managedRangerAuthorizer.checkInheritability(EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(0)).inheritFingerprint(anyString());
    }

    @Test(expected = AuthorizationAccessException.class)
    public void testCheckInheritInvalidFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        managedRangerAuthorizer.checkInheritability("not a valid fingerprint");
    }

    @Test
    public void testCheckInheritNonEmptyFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        managedRangerAuthorizer.checkInheritability(NON_EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(1)).checkInheritability(TENANT_FINGERPRINT);
    }

    @Test(expected = UninheritableAuthorizationsException.class)
    public void testCheckInheritNonConfigurableUserGroupProvider() throws Exception {
        final UserGroupProvider userGroupProvider = mock(UserGroupProvider.class);

        final ManagedRangerAuthorizer managedRangerAuthorizer = getStandardManagedAuthorizer(userGroupProvider);
        managedRangerAuthorizer.checkInheritability(NON_EMPTY_FINGERPRINT);
    }

    private ManagedRangerAuthorizer getStandardManagedAuthorizer(final UserGroupProvider userGroupProvider) {
        final ManagedRangerAuthorizer managedAuthorizer = new ManagedRangerAuthorizer();

        final AuthorizerConfigurationContext configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperty(eq("User Group Provider"))).thenReturn(new MockPropertyValue("user-group-provider", null));
        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_SECURITY_PATH_PROP))).thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-security.xml"));
        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_AUDIT_PATH_PROP))).thenReturn(new MockPropertyValue("src/test/resources/ranger/ranger-nifi-audit.xml"));
        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_APP_ID_PROP))).thenReturn(new MockPropertyValue(appId));
        when(configurationContext.getProperty(eq(RangerNiFiAuthorizer.RANGER_SERVICE_TYPE_PROP))).thenReturn(new MockPropertyValue(serviceType));

        final UserGroupProviderLookup userGroupProviderLookup = mock(UserGroupProviderLookup.class);
        when(userGroupProviderLookup.getUserGroupProvider("user-group-provider")).thenReturn(userGroupProvider);

        final AuthorizerInitializationContext initializationContext = mock(AuthorizerInitializationContext.class);
        when(initializationContext.getUserGroupProviderLookup()).thenReturn(userGroupProviderLookup);

        managedAuthorizer.initialize(initializationContext);
        managedAuthorizer.onConfigured(configurationContext);

        return managedAuthorizer;
    }
}