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

import static java.util.Arrays.asList;
import static java.util.Arrays.fill;
import static java.util.stream.Collectors.toSet;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.LISTING_STRATEGY;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.RECORD_WRITER;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.smb.ListSmb.DIRECTORY;
import static org.apache.nifi.processors.smb.ListSmb.FILE_NAME_SUFFIX_FILTER;
import static org.apache.nifi.processors.smb.ListSmb.MINIMUM_AGE;
import static org.apache.nifi.processors.smb.ListSmb.MINIMUM_SIZE;
import static org.apache.nifi.processors.smb.ListSmb.SMB_CLIENT_PROVIDER_SERVICE;
import static org.apache.nifi.services.smb.SmbjClientProviderService.DOMAIN;
import static org.apache.nifi.services.smb.SmbjClientProviderService.HOSTNAME;
import static org.apache.nifi.services.smb.SmbjClientProviderService.PASSWORD;
import static org.apache.nifi.services.smb.SmbjClientProviderService.PORT;
import static org.apache.nifi.services.smb.SmbjClientProviderService.SHARE;
import static org.apache.nifi.services.smb.SmbjClientProviderService.USERNAME;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.shaded.org.apache.commons.io.IOUtils.copy;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbListableEntity;
import org.apache.nifi.services.smb.SmbjClientProviderService;
import org.apache.nifi.services.smb.SmbjClientService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class ListSmbIT {

    private final static Integer DEFAULT_SAMBA_PORT = 445;
    private final static Logger logger = LoggerFactory.getLogger(ListSmbTest.class);
    private final static AtomicLong currentMillis = new AtomicLong();
    private final static AtomicLong currentNanos = new AtomicLong();
    private final GenericContainer<?> sambaContainer = new GenericContainer<>(DockerImageName.parse("dperson/samba"))
            .withExposedPorts(DEFAULT_SAMBA_PORT, 139)
            .waitingFor(Wait.forListeningPort())
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .withCommand("-w domain -u username;password -s share;/folder;;no;no;username;;; -p");
    private final AuthenticationContext authenticationContext =
            new AuthenticationContext("username", "password".toCharArray(), "domain");
    private SMBClient smbClient;
    private SmbjClientService smbjClientService;

    @BeforeEach
    public void beforeEach() throws Exception {
        sambaContainer.start();
        smbClient = new SMBClient();
        smbjClientService = createClient();
    }

    @AfterEach
    public void afterEach() throws IOException {
        smbjClientService.close();
        Connection c = smbClient.connect(sambaContainer.getHost(), sambaContainer.getMappedPort(445));
        c.close(true);
        smbClient.close();
        sambaContainer.stop();
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 50, 45000})
    public void shouldFillSizeAttributeProperly(int size) throws Exception {
        writeFile("1.txt", generateContentWithSize(size));
        waitForFilesToAppear(1);
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "none");
        testRunner.setProperty(MINIMUM_AGE, "0 ms");
        SmbjClientProviderService smbjClientProviderService = configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.enableControllerService(smbjClientProviderService);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.getFlowFilesForRelationship(REL_SUCCESS)
                .forEach(flowFile -> assertEquals(size, Integer.valueOf(flowFile.getAttribute("size"))));
        testRunner.assertValid();
        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    public void shouldShowBulletinOnMissingDirectory() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "none");
        testRunner.setProperty(MINIMUM_AGE, "0 ms");
        testRunner.setProperty(DIRECTORY, "folderDoesNotExists");
        SmbjClientProviderService smbjClientProviderService = configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.enableControllerService(smbjClientProviderService);
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        testRunner.assertValid();
        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    public void shouldShowBulletinWhenShareIsInvalid() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        SmbjClientProviderService smbjClientProviderService = configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.setProperty(smbjClientProviderService, SHARE, "invalid_share");
        testRunner.enableControllerService(smbjClientProviderService);
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        testRunner.assertValid();
        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    public void shouldShowBulletinWhenSMBPortIsInvalid() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final SmbClientProviderService smbClientProviderService =
                configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.setProperty(smbClientProviderService, PORT, "1");
        testRunner.enableControllerService(smbClientProviderService);
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        testRunner.assertValid();
        testRunner.disableControllerService(smbClientProviderService);
    }

    @Test
    public void shouldShowBulletinWhenSMBHostIsInvalid() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final SmbClientProviderService smbClientProviderService =
                configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.setProperty(smbClientProviderService, HOSTNAME, "this.host.should.not.exists");
        testRunner.enableControllerService(smbClientProviderService);
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        testRunner.disableControllerService(smbClientProviderService);
    }

    @Test
    public void shouldUseRecordWriterProperly() throws Exception {
        final Set<String> testFiles = new HashSet<>(asList(
                "1.txt",
                "directory/2.txt",
                "directory/subdirectory/3.txt",
                "directory/subdirectory2/4.txt",
                "directory/subdirectory3/5.txt"
        ));
        smbjClientService.createDirectory("directory/subdirectory");
        smbjClientService.createDirectory("directory/subdirectory2");
        smbjClientService.createDirectory("directory/subdirectory3");
        testFiles.forEach(file -> writeFile(file, generateContentWithSize(4)));
        waitForFilesToAppear(5);

        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final MockRecordWriter writer = new MockRecordWriter(null, false);
        final SimpleRecordSchema simpleRecordSchema = SmbListableEntity.getRecordSchema();
        testRunner.addControllerService("writer", writer);
        testRunner.enableControllerService(writer);
        testRunner.setProperty(LISTING_STRATEGY, "none");
        testRunner.setProperty(RECORD_WRITER, "writer");
        testRunner.setProperty(MINIMUM_AGE, "0 ms");
        SmbjClientProviderService smbjClientProviderService = configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.enableControllerService(smbjClientProviderService);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        final String result = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getContent();
        final int identifierColumnIndex = simpleRecordSchema.getFieldNames().indexOf("identifier");
        final Set<String> actual = Arrays.stream(result.split("\n"))
                .map(row -> row.split(",")[identifierColumnIndex])
                .collect(toSet());
        assertEquals(testFiles, actual);
        testRunner.assertValid();
        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    public void shouldWriteFlowFileAttributesProperly() throws Exception {
        final Set<String> testFiles = new HashSet<>(asList(
                "file_name", "directory/file_name", "directory/subdirectory/file_name"
        ));
        smbjClientService.createDirectory("directory/subdirectory");
        testFiles.forEach(file -> writeFile(file, generateContentWithSize(4)));
        waitForFilesToAppear(3);
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "timestamps");
        testRunner.setProperty(MINIMUM_AGE, "0 ms");
        final SmbjClientProviderService smbjClientProviderService =
                configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.enableControllerService(smbjClientProviderService);
        testRunner.run(1);
        testRunner.assertTransferCount(REL_SUCCESS, 3);
        final Set<Map<String, String>> allAttributes = testRunner.getFlowFilesForRelationship(REL_SUCCESS)
                .stream()
                .map(MockFlowFile::getAttributes)
                .collect(toSet());

        final Set<String> identifiers = allAttributes.stream()
                .map(attributes -> attributes.get("identifier"))
                .collect(toSet());
        assertEquals(testFiles, identifiers);

        System.out.println(allAttributes);

        allAttributes.forEach(attribute -> assertEquals(
                Stream.of(attribute.get("path"), attribute.get("filename")).filter(s -> !s.isEmpty()).collect(
                        Collectors.joining("/")),
                attribute.get("absolute.path")));

        final Set<String> fileNames = allAttributes.stream()
                .map(attributes -> attributes.get("filename"))
                .collect(toSet());

        assertEquals(new HashSet<>(Arrays.asList("file_name")), fileNames);

        testRunner.assertValid();
        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    public void shouldFilterFilesBySizeCriteria() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final SmbClientProviderService smbClientProviderService =
                configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.enableControllerService(smbClientProviderService);
        testRunner.setProperty(MINIMUM_AGE, "0 ms");
        testRunner.setProperty(LISTING_STRATEGY, "none");

        writeFile("1.txt", generateContentWithSize(1));
        writeFile("10.txt", generateContentWithSize(10));
        writeFile("100.txt", generateContentWithSize(100));
        waitForFilesToAppear(3);

        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 3);
        testRunner.clearTransferState();

        testRunner.setProperty(MINIMUM_SIZE, "10 B");
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 2);
        testRunner.clearTransferState();

        testRunner.setProperty(MINIMUM_SIZE, "50 B");
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);

        testRunner.disableControllerService(smbClientProviderService);

    }

    @Test
    public void shouldFilterByGivenSuffix() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final SmbClientProviderService smbClientProviderService =
                configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.enableControllerService(smbClientProviderService);
        testRunner.setProperty(MINIMUM_AGE, "0 ms");
        testRunner.setProperty(FILE_NAME_SUFFIX_FILTER, ".suffix");
        testRunner.setProperty(LISTING_STRATEGY, "none");
        writeFile("should_list_this", generateContentWithSize(1));
        writeFile("should_skip_this.suffix", generateContentWithSize(1));
        waitForFilesToAppear(2);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.disableControllerService(smbClientProviderService);
    }

    private SmbjClientService createClient() throws IOException {
        SmbjClientService smbjClientService = new SmbjClientService(smbClient, authenticationContext);
        smbjClientService.connectToShare(sambaContainer.getHost(), sambaContainer.getMappedPort(DEFAULT_SAMBA_PORT),
                "share");
        return smbjClientService;
    }

    private SmbjClientProviderService configureTestRunnerForSambaDockerContainer(TestRunner testRunner)
            throws Exception {
        SmbjClientProviderService smbjClientProviderService = new SmbjClientProviderService();
        testRunner.addControllerService("connection-pool", smbjClientProviderService);
        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, "connection-pool");
        testRunner.setProperty(smbjClientProviderService, HOSTNAME, sambaContainer.getHost());
        testRunner.setProperty(smbjClientProviderService, PORT,
                String.valueOf(sambaContainer.getMappedPort(DEFAULT_SAMBA_PORT)));
        testRunner.setProperty(smbjClientProviderService, USERNAME, "username");
        testRunner.setProperty(smbjClientProviderService, PASSWORD, "password");
        testRunner.setProperty(smbjClientProviderService, SHARE, "share");
        testRunner.setProperty(smbjClientProviderService, DOMAIN, "domain");
        return smbjClientProviderService;
    }

    private String generateContentWithSize(int sizeInBytes) {
        byte[] bytes = new byte[sizeInBytes];
        fill(bytes, (byte) 1);
        return new String(bytes);
    }

    private void waitForFilesToAppear(Integer numberOfFiles) {
        await().until(() -> {
            try (Stream<SmbListableEntity> s = smbjClientService.listRemoteFiles("")) {
                return s.count() == numberOfFiles;
            }
        });
    }

    private void writeFile(String path, String content) {
        try (OutputStream outputStream = smbjClientService.getOutputStreamForFile(path)) {
            final InputStream inputStream = new ByteArrayInputStream(content.getBytes());
            copy(inputStream, outputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}