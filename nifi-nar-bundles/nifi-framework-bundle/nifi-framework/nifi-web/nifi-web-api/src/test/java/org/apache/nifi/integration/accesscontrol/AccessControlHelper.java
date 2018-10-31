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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.integration.NiFiWebApiTest;
import org.apache.nifi.integration.util.NiFiTestAuthorizer;
import org.apache.nifi.integration.util.NiFiTestServer;
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.util.NiFiProperties;

import javax.ws.rs.core.Response;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static org.junit.Assert.assertEquals;

/**
 * Access control test for the dfm user.
 */
public class AccessControlHelper {

    public static final String NONE_CLIENT_ID = "client-id";
    public static final String READ_CLIENT_ID = "r-client-id";
    public static final String WRITE_CLIENT_ID = "w-client-id";
    public static final String READ_WRITE_CLIENT_ID = "rw-client-id";

    private NiFiTestUser readUser;
    private NiFiTestUser writeUser;
    private NiFiTestUser readWriteUser;
    private NiFiTestUser noneUser;
    private NiFiTestUser privilegedUser;
    private NiFiTestUser executeCodeUser;

    private static final String CONTEXT_PATH = "/nifi-api";

    private NiFiTestServer server;
    private String baseUrl;
    private String flowXmlPath;

    public AccessControlHelper() throws Exception {
        this("src/test/resources/access-control/nifi.properties");
    }

    public AccessControlHelper(final String nifiPropertiesPath) throws Exception {
        // configure the location of the nifi properties
        File nifiPropertiesFile = new File(nifiPropertiesPath);
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, nifiPropertiesFile.getAbsolutePath());
        NiFiProperties props = NiFiProperties.createBasicNiFiProperties(null, null);
        flowXmlPath = props.getProperty(NiFiProperties.FLOW_CONFIGURATION_FILE);

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
        server = new NiFiTestServer("src/main/webapp", CONTEXT_PATH, props);
        server.startServer();
        server.loadFlow();

        // get the base url
        baseUrl = server.getBaseUrl() + CONTEXT_PATH;

        // create the users - user purposefully decoupled from clientId (same user different browsers tabs)
        readUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.READ_USER_DN);
        writeUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.WRITE_USER_DN);
        readWriteUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.READ_WRITE_USER_DN);
        noneUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.NONE_USER_DN);
        privilegedUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.PRIVILEGED_USER_DN);
        executeCodeUser = new NiFiTestUser(server.getClient(), NiFiTestAuthorizer.EXECUTED_CODE_USER_DN);

        // populate the initial data flow
        NiFiWebApiTest.populateFlow(server.getClient(), baseUrl, readWriteUser, READ_WRITE_CLIENT_ID);
    }

    public NiFiTestUser getReadUser() {
        return readUser;
    }

    public NiFiTestUser getWriteUser() {
        return writeUser;
    }

    public NiFiTestUser getReadWriteUser() {
        return readWriteUser;
    }

    public NiFiTestUser getNoneUser() {
        return noneUser;
    }

    public NiFiTestUser getPrivilegedUser() {
        return privilegedUser;
    }

    public NiFiTestUser getExecuteCodeUser() {
        return executeCodeUser;
    }

    public void testGenericGetUri(final String uri) throws Exception {
        Response response;

        // read
        response = getReadUser().testGet(uri);
        assertEquals(200, response.getStatus());

        // read/write
        response = getReadWriteUser().testGet(uri);
        assertEquals(200, response.getStatus());

        // write
        response = getWriteUser().testGet(uri);
        assertEquals(403, response.getStatus());

        // none
        response = getNoneUser().testGet(uri);
        assertEquals(403, response.getStatus());
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void cleanup() throws Exception {
        // shutdown the server
        server.shutdownServer();
        server = null;

        // look for the flow.xml and toss it
        File flow = new File(flowXmlPath);
        if (flow.exists()) {
            flow.delete();
        }
    }
}
