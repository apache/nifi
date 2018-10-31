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
package org.apache.nifi.integration.accesscontrol;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.integration.util.NiFiTestServer;
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.integration.util.SourceTestProcessor;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.AccessConfigurationDTO;
import org.apache.nifi.web.api.dto.AccessStatusDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AccessConfigurationEntity;
import org.apache.nifi.web.api.entity.AccessStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.util.WebUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Access token endpoint test.
 */
public class ITAccessTokenEndpoint {

    private static final String CLIENT_ID = "token-endpoint-id";
    private static final String CONTEXT_PATH = "/nifi-api";

    private static String flowXmlPath;
    private static NiFiTestServer SERVER;
    private static NiFiTestUser TOKEN_USER;
    private static String BASE_URL;

    @BeforeClass
    public static void setup() throws Exception {
        // configure the location of the nifi properties
        File nifiPropertiesFile = new File("src/test/resources/access-control/nifi.properties");
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, nifiPropertiesFile.getAbsolutePath());

        NiFiProperties props = NiFiProperties.createBasicNiFiProperties(null, null);
        flowXmlPath = props.getProperty(NiFiProperties.FLOW_CONFIGURATION_FILE);

        // delete the database directory to avoid issues with re-registration in testRequestAccessUsingToken
        FileUtils.deleteDirectory(props.getDatabaseRepositoryPath().toFile());

        final File libTargetDir = new File("target/test-classes/access-control/lib");
        libTargetDir.mkdirs();

        final File libSourceDir = new File("src/test/resources/lib");
        for (final File libFile : libSourceDir.listFiles()) {
            final File libDestFile = new File(libTargetDir, libFile.getName());
            Files.copy(libFile.toPath(), libDestFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        final Bundle systemBundle = SystemBundle.create(props);
        NarUnpacker.unpackNars(props, systemBundle);
        NarClassLoadersHolder.getInstance().init(props.getFrameworkWorkingDirectory(), props.getExtensionsWorkingDirectory());

        // load extensions
        final ExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, NarClassLoadersHolder.getInstance().getBundles());
        ExtensionManagerHolder.init(extensionManager);

        // start the server
        SERVER = new NiFiTestServer("src/main/webapp", CONTEXT_PATH, props);
        SERVER.startServer();
        SERVER.loadFlow();

        // get the base url
        BASE_URL = SERVER.getBaseUrl() + CONTEXT_PATH;

        // create the user
        final Client client = WebUtils.createClient(null, createTrustContext(props));
        TOKEN_USER = new NiFiTestUser(client, null);
    }

    private static SSLContext createTrustContext(final NiFiProperties props) throws Exception {
        return SslContextFactory.createTrustSslContext(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE),
                props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD).toCharArray(),
                props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE), "TLS");
    }

    // -----------
    // LOGIN CONIG
    // -----------
    /**
     * Test getting access configuration.
     *
     * @throws Exception ex
     */
    @Test
    public void testGetAccessConfig() throws Exception {
        String url = BASE_URL + "/access/config";

        Response response = TOKEN_USER.testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // extract the process group
        AccessConfigurationEntity accessConfigEntity = response.readEntity(AccessConfigurationEntity.class);

        // ensure there is content
        Assert.assertNotNull(accessConfigEntity);

        // extract the process group dto
        AccessConfigurationDTO accessConfig = accessConfigEntity.getConfig();

        // verify config
        Assert.assertTrue(accessConfig.getSupportsLogin());
    }

    /**
     * Obtains a token and creates a processor using it.
     *
     * @throws Exception ex
     */
    @Test
    public void testCreateProcessorUsingToken() throws Exception {
        String url = BASE_URL + "/access/token";

        Response response = TOKEN_USER.testCreateToken(url, "user@nifi", "whatever");

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the token
        String token = response.readEntity(String.class);

        // attempt to create a processor with it
        createProcessor(token);
    }

    private ProcessorDTO createProcessor(final String token) throws Exception {
        String url = BASE_URL + "/process-groups/root/processors";

        // authorization header
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);

        // create the processor
        ProcessorDTO processor = new ProcessorDTO();
        processor.setName("Copy");
        processor.setType(SourceTestProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(0l);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        Response response = TOKEN_USER.testPostWithHeaders(url, entity, headers);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.readEntity(ProcessorEntity.class);

        // verify creation
        processor = entity.getComponent();
        Assert.assertEquals("Copy", processor.getName());
        Assert.assertEquals("org.apache.nifi.integration.util.SourceTestProcessor", processor.getType());

        return processor;
    }

    /**
     * Verifies the response when bad credentials are specified.
     *
     * @throws Exception ex
     */
    @Test
    public void testInvalidCredentials() throws Exception {
        String url = BASE_URL + "/access/token";

        Response response = TOKEN_USER.testCreateToken(url, "user@nifi", "not a real password");

        // ensure the request is successful
        Assert.assertEquals(400, response.getStatus());
    }

    /**
     * Verifies the response when the user is known.
     *
     * @throws Exception ex
     */
    @Test
    public void testUnknownUser() throws Exception {
        String url = BASE_URL + "/access/token";

        Response response = TOKEN_USER.testCreateToken(url, "not a real user", "not a real password");

        // ensure the request is successful
        Assert.assertEquals(400, response.getStatus());
    }

    /**
     * Request access using access token.
     *
     * @throws Exception ex
     */
    @Test
    public void testRequestAccessUsingToken() throws Exception {
        String accessStatusUrl = BASE_URL + "/access";
        String accessTokenUrl = BASE_URL + "/access/token";

        Response response = TOKEN_USER.testGet(accessStatusUrl);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        AccessStatusEntity accessStatusEntity = response.readEntity(AccessStatusEntity.class);
        AccessStatusDTO accessStatus = accessStatusEntity.getAccessStatus();

        // verify unknown
        Assert.assertEquals("UNKNOWN", accessStatus.getStatus());

        response = TOKEN_USER.testCreateToken(accessTokenUrl, "unregistered-user@nifi", "password");

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the token
        String token = response.readEntity(String.class);

        // authorization header
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);

        // check the status with the token
        response = TOKEN_USER.testGetWithHeaders(accessStatusUrl, null, headers);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        accessStatusEntity = response.readEntity(AccessStatusEntity.class);
        accessStatus = accessStatusEntity.getAccessStatus();

        // verify unregistered
        Assert.assertEquals("ACTIVE", accessStatus.getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        // shutdown the server
        SERVER.shutdownServer();
        SERVER = null;

        // look for the flow.xml
        File flow = new File(flowXmlPath);
        if (flow.exists()) {
            flow.delete();
        }
    }
}
