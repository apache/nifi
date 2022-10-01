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

import com.microsoft.graph.models.extensions.AppRole;
import com.microsoft.graph.models.extensions.Application;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.IApplicationRequest;
import com.microsoft.graph.requests.extensions.IApplicationRequestBuilder;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AzureAppRolesUserGroupProviderIT {
    private AuthorizerConfigurationContext authContext = Mockito.mock(AuthorizerConfigurationContext.class);
    private AzureAppRolesUserGroupProvider testingProvider;
    private UserGroupProviderInitializationContext initContext;


    @BeforeEach
    public void setup() {
        authContext = Mockito.mock(AuthorizerConfigurationContext.class);
        initContext = Mockito.mock(UserGroupProviderInitializationContext.class);

        Consumer<String> mockProperty = name -> {
            Mockito.when(authContext.getProperty(Mockito.eq(name))).thenReturn(new MockPropertyValue(name));
        };

        mockProperty.accept(AzureAppRolesUserGroupProvider.AUTHORITY_ENDPOINT_PROPERTY);
        mockProperty.accept(AzureAppRolesUserGroupProvider.TENANT_ID_PROPERTY);
        mockProperty.accept(AzureAppRolesUserGroupProvider.APP_REG_CLIENT_ID_PROPERTY);
        mockProperty.accept(AzureAppRolesUserGroupProvider.APP_REG_CLIENT_SECRET_PROPERTY);
        mockProperty.accept(AzureAppRolesUserGroupProvider.APP_REG_OBJECT_ID_PROPERTY);
    }

    private void setupTestingProvider(List<AppRole> appRoles) {
        IGraphServiceClient graphServiceClient = Mockito.mock(IGraphServiceClient.class);
        IApplicationRequestBuilder applicationRequestBuilder = Mockito.mock(IApplicationRequestBuilder.class);
        IApplicationRequest applicationRequest = Mockito.mock(IApplicationRequest.class);
        Mockito.when(graphServiceClient.applications(AzureAppRolesUserGroupProvider.APP_REG_OBJECT_ID_PROPERTY)).thenReturn(
                applicationRequestBuilder
        );
        Mockito.when(applicationRequestBuilder.buildRequest()).thenReturn(applicationRequest);
        Application application = new Application();
        application.appRoles = appRoles;
        Mockito.when(applicationRequest.get()).thenReturn(application);
        testingProvider = new AzureAppRolesUserGroupProvider() {
            protected IGraphServiceClient getGraphServiceClient(ClientCredentialAuthProvider authProvider) {
                return graphServiceClient;
            }
        };
        testingProvider.initialize(initContext);
        testingProvider.onConfigured(authContext);
    }

    @AfterEach
    public void tearDown() {
        testingProvider.preDestruction();
    }

    @Test
    public void testGetGroups() {
        AppRole testAppRole = new AppRole();
        testAppRole.id = UUID.randomUUID();
        testAppRole.displayName = "Test";
        List<AppRole> appRoles = Arrays.asList(testAppRole);
        setupTestingProvider(appRoles);
        assertEquals(testingProvider.getGroups(), Stream.of(new Group.Builder().identifier(testAppRole.id.toString()).name("Test").build())
                .collect(Collectors.toSet()));
    }
}
