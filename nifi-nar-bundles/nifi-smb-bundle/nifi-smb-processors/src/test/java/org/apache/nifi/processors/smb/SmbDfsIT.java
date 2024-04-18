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
package org.apache.nifi.processors.smb;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.smb.SmbjClientProviderService;
import org.apache.nifi.smb.common.SmbProperties;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.nifi.processor.util.list.AbstractListProcessor.NO_TRACKING;
import static org.apache.nifi.processors.smb.ListSmb.SMB_CLIENT_PROVIDER_SERVICE;
import static org.apache.nifi.services.smb.SmbjClientProviderService.HOSTNAME;
import static org.apache.nifi.services.smb.SmbjClientProviderService.PASSWORD;
import static org.apache.nifi.services.smb.SmbjClientProviderService.PORT;
import static org.apache.nifi.services.smb.SmbjClientProviderService.SHARE;
import static org.apache.nifi.services.smb.SmbjClientProviderService.USERNAME;
import static org.apache.nifi.smb.common.SmbProperties.ENABLE_DFS;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SmbDfsIT {

    private final static Logger LOGGER = LoggerFactory.getLogger(SmbDfsIT.class);

    private static final int DEFAULT_SMB_PORT = 445;

    // DFS works only on the default SMB port (445). Not sure if it is a generic DFS vs Samba DFS constraint, or an issue in the smbj client library.
    private final GenericContainer<?> sambaContainer = new FixedHostPortGenericContainer<>("dperson/samba")
            .withFixedExposedPort(DEFAULT_SMB_PORT, DEFAULT_SMB_PORT)
            .waitingFor(Wait.forListeningPort())
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withCommand("-u", "myuser;mypass",
                    "-s", "share;/share-dir;;no;no;myuser;;;",
                    "-s", "dfs-share;/dfs-share-dir;;no;no;myuser;;;",
                    "-p",
                    "-g", "host msdfs = yes",
                    "-G", "dfs-share;msdfs root = yes");

    @BeforeEach
    void beforeEach() throws Exception {
        sambaContainer.start();

        sambaContainer.execInContainer("ln", "-s", "msdfs:" + sambaContainer.getHost() + "\\share", "/dfs-share-dir/dfs-link");
        Thread.sleep(100);
    }

    @AfterEach
    void afterEach() {
        sambaContainer.stop();
    }

    @Test
    void testFetchSmb() throws Exception {
        writeFile("fetch_file", "fetch_content");

        TestRunner testRunner = newTestRunner(FetchSmb.class);
        testRunner.setProperty(FetchSmb.REMOTE_FILE, "dfs-link/fetch_file");
        SmbjClientProviderService smbjClientProviderService = configureSmbClient(testRunner);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertTransferCount(FetchSmb.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchSmb.REL_SUCCESS).get(0);
        assertEquals("fetch_content", flowFile.getContent());

        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    void testFetchFileFailsWhenDfsIsDisabled() throws Exception {
        writeFile("fetch_file", "fetch_content");

        TestRunner testRunner = newTestRunner(FetchSmb.class);
        testRunner.setProperty(FetchSmb.REMOTE_FILE, "dfs-link/fetch_file");
        SmbjClientProviderService smbjClientProviderService = configureSmbClient(testRunner, false);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertTransferCount(FetchSmb.REL_FAILURE, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchSmb.REL_FAILURE).get(0);
        assertEquals(0, flowFile.getSize());

        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    void testListSmbWithDfsLink() throws Exception {
        testListSmb("dfs-link");
    }

    @Test
    @Disabled("Listing folders recursively from the DFS root or a parent directory of the DFS link does not work on Samba due to https://github.com/hierynomus/smbj/issues/717#")
    void testListSmbWithDfsRoot() throws Exception {
        testListSmb(null);
    }

    private void testListSmb(String directory) throws Exception {
        writeFile("list_file", "list_content");

        TestRunner testRunner = newTestRunner(ListSmb.class);
        if (directory != null) {
            testRunner.setProperty(ListSmb.DIRECTORY, directory);
        }
        testRunner.setProperty(ListSmb.LISTING_STRATEGY, NO_TRACKING);
        testRunner.setProperty(ListSmb.MINIMUM_AGE, "0 ms");
        SmbjClientProviderService smbjClientProviderService = configureSmbClient(testRunner);

        testRunner.run();

        testRunner.assertTransferCount(ListSmb.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListSmb.REL_SUCCESS).get(0);
        assertEquals(0, flowFile.getSize());
        assertEquals("dfs-link", flowFile.getAttribute(CoreAttributes.PATH.key()));
        assertEquals("list_file", flowFile.getAttribute(CoreAttributes.FILENAME.key()));

        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    void testPutSmbFile() {
        TestRunner testRunner = newTestRunner(PutSmbFile.class);
        testRunner.setProperty(PutSmbFile.HOSTNAME, sambaContainer.getHost());
        testRunner.setProperty(PutSmbFile.SHARE, "dfs-share");
        testRunner.setProperty(PutSmbFile.DIRECTORY, "dfs-link");
        testRunner.setProperty(PutSmbFile.USERNAME, "myuser");
        testRunner.setProperty(PutSmbFile.PASSWORD, "mypass");
        testRunner.setProperty(SmbProperties.ENABLE_DFS, "true");

        testRunner.enqueue("put_content", Map.of(CoreAttributes.FILENAME.key(), "put_file"));
        testRunner.run();

        testRunner.assertTransferCount(PutSmbFile.REL_SUCCESS, 1);

        String fileContent = readFile("put_file");
        assertEquals("put_content", fileContent);
    }

    @Test
    void testGetSmbFile() {
        writeFile("get_file", "get_content");

        TestRunner testRunner = newTestRunner(GetSmbFile.class);
        testRunner.setProperty(GetSmbFile.HOSTNAME, sambaContainer.getHost());
        testRunner.setProperty(GetSmbFile.SHARE, "dfs-share");
        testRunner.setProperty(GetSmbFile.DIRECTORY, "dfs-link");
        testRunner.setProperty(GetSmbFile.USERNAME, "myuser");
        testRunner.setProperty(GetSmbFile.PASSWORD, "mypass");
        testRunner.setProperty(SmbProperties.ENABLE_DFS, "true");

        testRunner.run();

        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GetSmbFile.REL_SUCCESS).get(0);
        assertEquals("get_content", flowFile.getContent());
        assertEquals("dfs-link", flowFile.getAttribute(CoreAttributes.PATH.key()));
        assertEquals("get_file", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
    }

    private SmbjClientProviderService configureSmbClient(TestRunner testRunner) throws InitializationException {
        return configureSmbClient(testRunner, true);
    }

    private SmbjClientProviderService configureSmbClient(TestRunner testRunner, boolean enableDfs) throws InitializationException {
        SmbjClientProviderService smbjClientProviderService = new SmbjClientProviderService();

        testRunner.addControllerService("client-provider", smbjClientProviderService);

        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, "client-provider");

        testRunner.setProperty(smbjClientProviderService, HOSTNAME, sambaContainer.getHost());
        testRunner.setProperty(smbjClientProviderService, PORT, Integer.toString(DEFAULT_SMB_PORT));
        testRunner.setProperty(smbjClientProviderService, USERNAME, "myuser");
        testRunner.setProperty(smbjClientProviderService, PASSWORD, "mypass");
        testRunner.setProperty(smbjClientProviderService, SHARE, "dfs-share");
        testRunner.setProperty(smbjClientProviderService, ENABLE_DFS, Boolean.toString(enableDfs));

        testRunner.enableControllerService(smbjClientProviderService);

        return smbjClientProviderService;
    }

    private void writeFile(String filename, String content) {
        String containerPath = "/share-dir/" + filename;
        sambaContainer.copyFileToContainer(Transferable.of(content), containerPath);
    }

    private String readFile(String filename) {
        String containerPath = "/share-dir/" + filename;
        return sambaContainer.copyFileFromContainer(containerPath, is -> IOUtils.toString(is, StandardCharsets.UTF_8));
    }

}
