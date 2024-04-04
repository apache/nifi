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
package org.apache.nifi.controller.asana;

import com.asana.models.Project;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.controller.asana.StandardAsanaClientProviderService.PROP_ASANA_API_BASE_URL;
import static org.apache.nifi.controller.asana.StandardAsanaClientProviderService.PROP_ASANA_PERSONAL_ACCESS_TOKEN;
import static org.apache.nifi.controller.asana.StandardAsanaClientProviderService.PROP_ASANA_WORKSPACE_NAME;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardAsanaClientProviderServiceTest {

    private static final String LOCALHOST = "localhost";

    private static final String WORKSPACES = """
            {
              "data": [
                {
                  "gid": "1202898619267352",
                  "name": "My Workspace",
                  "resource_type": "workspace"
                },
                {
                  "gid": "1202939205399549",
                  "name": "Company or Team Name",
                  "resource_type": "workspace"
                },
                {
                  "gid": "1202946450806837",
                  "name": "Company or Team Name",
                  "resource_type": "workspace"
                }
              ],
              "next_page": null
            }""";

    private static final String PROJECTS = """
            {
              "data": [
                {
                  "gid": "1202898619637000",
                  "name": "Our First Project",
                  "resource_type": "project"
                },
                {
                  "gid": "1202986168388325",
                  "name": "Another Project Again",
                  "resource_type": "project"
                }
              ],
              "next_page": null
            }""";

    private TestRunner runner;
    private StandardAsanaClientProviderService service;
    private MockWebServer mockWebServer;

    @BeforeEach
    public void init() throws InitializationException {
        runner = newTestRunner(NoOpProcessor.class);
        service = new StandardAsanaClientProviderService();
        runner.addControllerService(AsanaClientProviderService.class.getName(), service);
        mockWebServer = new MockWebServer();
    }

    @AfterEach
    public void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testInvalidIfNoAccessTokenProvided() {
        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidWithAccessTokenButNoWorkspace() {
        runner.setProperty(service, PROP_ASANA_PERSONAL_ACCESS_TOKEN, "12345");
        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidWithWorkspaceButNoAccessToken() {
        runner.setProperty(service, PROP_ASANA_WORKSPACE_NAME, "My Workspace");
        runner.assertNotValid(service);
    }

    @Test
    public void testValidWithAccessTokenAndWorkspaceProvided() {
        runner.setProperty(service, PROP_ASANA_PERSONAL_ACCESS_TOKEN, "12345");
        runner.setProperty(service, PROP_ASANA_WORKSPACE_NAME, "My Workspace");
        runner.assertValid(service);
    }

    @Test
    public void testInvalidWithIncorrectApiUrlFormat() {
        runner.setProperty(service, PROP_ASANA_PERSONAL_ACCESS_TOKEN, "12345");
        runner.setProperty(service, PROP_ASANA_WORKSPACE_NAME, "My Workspace");
        runner.setProperty(service, PROP_ASANA_API_BASE_URL, "Foo::Bar::1234");
        runner.assertNotValid(service);
    }

    @Test
    public void testInvalidWithEmptyUrl() {
        runner.setProperty(service, PROP_ASANA_PERSONAL_ACCESS_TOKEN, "12345");
        runner.setProperty(service, PROP_ASANA_WORKSPACE_NAME, "My Workspace");
        runner.setProperty(service, PROP_ASANA_API_BASE_URL, StringUtils.EMPTY);
        runner.assertNotValid(service);
    }

    @Test
    public void testClientCreatedWithConfiguredApiEndpoint() throws InterruptedException {
        runner.setProperty(service, PROP_ASANA_PERSONAL_ACCESS_TOKEN, "12345");
        runner.setProperty(service, PROP_ASANA_WORKSPACE_NAME, "My Workspace");
        runner.setProperty(service, PROP_ASANA_API_BASE_URL, getMockWebServerUrl());

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(WORKSPACES));
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(PROJECTS));

        runner.enableControllerService(service);
        final Map<String, Project> projects = service.createClient().getProjects()
                .collect(toMap(project -> project.gid, project -> project));

        assertEquals(2, projects.size());
        assertEquals("Our First Project", projects.get("1202898619637000").name);
        assertEquals("Another Project Again", projects.get("1202986168388325").name);

        assertEquals(2, mockWebServer.getRequestCount());
        assertEquals("Bearer 12345", mockWebServer.takeRequest().getHeader("Authorization"));
        assertTrue(mockWebServer.takeRequest().getRequestLine().contains("workspace=1202898619267352"));
    }

    @Test
    public void testCreateClientThrowsIfMultipleWorkspacesExistWithSameName() {
        runner.setProperty(service, PROP_ASANA_PERSONAL_ACCESS_TOKEN, "12345");
        runner.setProperty(service, PROP_ASANA_WORKSPACE_NAME, "Company or Team Name");
        runner.setProperty(service, PROP_ASANA_API_BASE_URL, getMockWebServerUrl());

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(WORKSPACES));
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(PROJECTS));

        runner.enableControllerService(service);
        assertThrows(RuntimeException.class, service::createClient);
    }

    private String getMockWebServerUrl() {
        return mockWebServer.url("asana").newBuilder().host(LOCALHOST).build().toString();
    }
}
