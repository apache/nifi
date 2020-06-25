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

package org.apache.nifi.authorization.azure;

import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureGraphUserGroupProviderIT {
    private static final Logger logger = LoggerFactory.getLogger(AzureGraphUserGroupProviderIT.class);

    private static final Properties CONFIG;

    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/azure-aad-app-reg.PROPERTIES";

    static {
        CONFIG = new Properties();
        try {
            final FileInputStream fis = new FileInputStream(CREDENTIALS_FILE);
            try {
                CONFIG.load(fis);
            } catch (IOException e) {
                fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e.getLocalizedMessage());
            } finally {
                FileUtils.closeQuietly(fis);
            }
        } catch (FileNotFoundException e) {
            fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e.getLocalizedMessage());
        }
    }

    protected static String getAuthorityEndpoint() {
        return CONFIG.getProperty("AUTHORITY_ENDPOINT");
    }

    protected static String getTenantId() {
        return CONFIG.getProperty("TENANT_ID");
    }

    protected static String getAppRegClientId() {
        return CONFIG.getProperty("APP_REG_CLIENT_ID");
    }

    protected static String getAppRegClientSecret() {
        return CONFIG.getProperty("APP_REG_CLIENT_SECRET");
    }

    protected static String getKnownTestUserName() {
        return CONFIG.getProperty("KNOWN_TEST_USER");

    }

    protected static String getKnownTestGroupName() {
        return CONFIG.getProperty("KNOWN_TEST_GROUP");
    }

    protected static String getGroupListInclusion() {
        return CONFIG.getProperty("GROUP_FILTER_LIST_INCLUSION");
    }

    private AuthorizerConfigurationContext authContext = Mockito.mock(AuthorizerConfigurationContext.class);
    private AzureGraphUserGroupProvider testingProvider;
    private UserGroupProviderInitializationContext initContext;

    @Before
    public void setup() throws IOException {
        authContext = Mockito.mock(AuthorizerConfigurationContext.class);
        initContext = Mockito.mock(UserGroupProviderInitializationContext.class);

        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.AUTHORITY_ENDPOINT_PROPERTY)))
            .thenReturn(new MockPropertyValue(AzureGraphUserGroupProviderIT.getAuthorityEndpoint()));

        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.TENANT_ID_PROPERTY)))
            .thenReturn(new MockPropertyValue(AzureGraphUserGroupProviderIT.getTenantId()));

        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.APP_REG_CLIENT_ID_PROPERTY)))
            .thenReturn(new MockPropertyValue(AzureGraphUserGroupProviderIT.getAppRegClientId()));
        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.APP_REG_CLIENT_SECRET_PROPERTY)))
            .thenReturn(new MockPropertyValue(AzureGraphUserGroupProviderIT.getAppRegClientSecret()));
    }

    private void setupTestingProvider() {
        testingProvider = new AzureGraphUserGroupProvider();
        try {
            testingProvider.initialize(initContext);
            testingProvider.onConfigured(authContext);
        } catch (final Exception exc) {
            logger.error("Error during setup; tests cannot run on this system.");
            return;
        }
    }


    @After
    public void tearDown() {
        testingProvider.preDestruction();
    }

    @Test
    public void testWithGroupListFilter(){
        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.GROUP_FILTER_LIST_PROPERTY)))
            .thenReturn(new MockPropertyValue(getGroupListInclusion()));

        setupTestingProvider();

        Assert.assertTrue(testingProvider.getGroups().size() > 0);
        Assert.assertTrue(testingProvider.getUsers().size() > 0);
        UserAndGroups uag  = testingProvider.getUserAndGroups(getKnownTestUserName());
        Assert.assertNotNull(uag.getUser());
        Assert.assertTrue(uag.getGroups().size() > 0);

    }

    @Test
    public void testWithPaging(){
        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.GROUP_FILTER_LIST_PROPERTY)))
            .thenReturn(new MockPropertyValue(getGroupListInclusion()));
        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.PAGE_SIZE_PROPERTY)))
            .thenReturn(new MockPropertyValue("3")); // in the real scenario, this should be 20 or bigger.

        setupTestingProvider();

        Assert.assertTrue(testingProvider.getGroups().size() > 0);
        Assert.assertTrue(testingProvider.getUsers().size() > 0);
        UserAndGroups uag  = testingProvider.getUserAndGroups(getKnownTestUserName());
        Assert.assertNotNull(uag.getUser());
        Assert.assertTrue(uag.getGroups().size() > 0);

        String knownGroupName = getKnownTestGroupName();
        List<Group> search = testingProvider.getGroups().stream().filter(g-> g.getName().equals(knownGroupName)).collect(Collectors.toList());
        Assert.assertTrue(search.size() > 0);
    }

    @Test
    public void testWithGroupFilterPrefix(){
        // make sure to set up a test group name whose name length is longer than 5
        String knownGroupName = getKnownTestGroupName();
        String prefix = knownGroupName.substring(0, 2);
        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.GROUP_FILTER_PREFIX_PROPERTY)))
        .thenReturn(new MockPropertyValue(prefix));

        setupTestingProvider();
        Assert.assertTrue(testingProvider.getGroups().size() > 0);
        List<Group> search = testingProvider.getGroups().stream().filter(g-> g.getName().equals(knownGroupName)).collect(Collectors.toList());
        Assert.assertTrue(search.size() > 0);
    }

    @Test
    public void testWithGroupFilterSuffix(){
        // make sure to set up a test group name whose name length is longer than 5
        String knownGroupName = getKnownTestGroupName();
        String suffix = knownGroupName.substring(knownGroupName.length()-2);
        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.GROUP_FILTER_SUFFIX_PROPERTY)))
            .thenReturn(new MockPropertyValue(suffix));

        setupTestingProvider();
        Assert.assertTrue(testingProvider.getGroups().size() > 0);
        List<Group> search = testingProvider.getGroups().stream().filter(g-> g.getName().equals(knownGroupName)).collect(Collectors.toList());
        Assert.assertTrue(search.size() > 0);
    }

    @Test
    public void testWithGroupFilterSubstring(){
        // make sure to set up a test group name whose name length is longer than 5
        String knownGroupName = getKnownTestGroupName();
        String substring = knownGroupName.substring(1, knownGroupName.length()-1);
        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.GROUP_FILTER_SUBSTRING_PROPERTY)))
            .thenReturn(new MockPropertyValue(substring));

        setupTestingProvider();
        Assert.assertTrue(testingProvider.getGroups().size() > 0);
        List<Group> search = testingProvider.getGroups().stream().filter( g-> g.getName().equals(knownGroupName)).collect(Collectors.toList());
        Assert.assertTrue(search.size() > 0);
    }

    @Test
    public void testWithGroupFilterOperatorAndListInclusion(){
        // make sure to set up a test group name whose name length is longer than 5
        String knownGroupName = getKnownTestGroupName();
        String substring = knownGroupName.substring(1, knownGroupName.length()-1);
        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.GROUP_FILTER_SUBSTRING_PROPERTY)))
            .thenReturn(new MockPropertyValue(substring));
        Mockito.when(authContext.getProperty(Mockito.eq(AzureGraphUserGroupProvider.GROUP_FILTER_LIST_PROPERTY)))
            .thenReturn(new MockPropertyValue(getGroupListInclusion()));

        setupTestingProvider();
        Assert.assertTrue(testingProvider.getGroups().size() > 0);
        Set<Group> search = testingProvider.getGroups().stream().collect(Collectors.toSet());
        // check there is no duplicate group
        Assert.assertEquals(search.size(), testingProvider.getGroups().size());
    }
}
