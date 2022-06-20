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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.LISTING_STRATEGY;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.smb.ListSmb.DIRECTORY;
import static org.apache.nifi.processors.smb.ListSmb.MINIMUM_AGE;
import static org.apache.nifi.processors.smb.ListSmb.SKIP_FILES_WITH_SUFFIX;
import static org.apache.nifi.processors.smb.ListSmb.SMB_CONNECTION_POOL_SERVICE;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.apache.commons.io.IOUtils.copy;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.services.smb.SmbConnectionPoolService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
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
    private final SMBClient smbClient = new SMBClient();
    private final AuthenticationContext authenticationContext =
            new AuthenticationContext("username", "password".toCharArray(), "domain");
    private NiFiSmbClient nifiSmbClient;

    public static long currentMillis() {
        return currentMillis.get();
    }

    public static long currentNanos() {
        return currentNanos.get();
    }

    public static void setTime(Long timeInMillis) {
        currentMillis.set(timeInMillis);
        currentNanos.set(NANOSECONDS.convert(timeInMillis, MILLISECONDS));
    }

    public static void timePassed(Long timeInMillis) {
        currentMillis.addAndGet(timeInMillis);
        currentNanos.addAndGet(NANOSECONDS.convert(timeInMillis, MILLISECONDS));
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        sambaContainer.start();
        nifiSmbClient = createClient();
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 50, 45000})
    public void shouldFillSizeAttributeProperly(int size) throws Exception {
        writeFile("1.txt", generateContentWithSize(size));
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "none");
        testRunner.setProperty(MINIMUM_AGE, "0");
        configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.run();
        testRunner.getFlowFilesForRelationship(REL_SUCCESS)
                .forEach(flowFile -> assertEquals(size, Integer.valueOf(flowFile.getAttribute("size"))));
        testRunner.assertValid();
    }

    @Test
    public void shouldShowBulletinOnMissingDirectory() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty(LISTING_STRATEGY, "none");
        testRunner.setProperty(MINIMUM_AGE, "0");
        testRunner.setProperty(DIRECTORY, "folderDoesNotExists");
        configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        testRunner.assertValid();
    }

    @Test
    public void shouldShowBulletinWhenShareIsInvalid() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final SmbConnectionPoolService connectionPoolService = mockSmbConnectionPoolService();
        when(connectionPoolService.getShareName()).thenReturn("invalidShare");
        testRunner.setProperty(SMB_CONNECTION_POOL_SERVICE, connectionPoolService.getIdentifier());
        testRunner.addControllerService(connectionPoolService.getIdentifier(), connectionPoolService);
        testRunner.enableControllerService(connectionPoolService);
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        testRunner.assertValid();
    }

    @Test
    public void shouldShowBulletinWhenSMBPortIsInvalid() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final SmbConnectionPoolService connectionPoolService = mockSmbConnectionPoolService();
        when(connectionPoolService.getPort()).thenReturn(1);
        testRunner.setProperty(SMB_CONNECTION_POOL_SERVICE, connectionPoolService.getIdentifier());
        testRunner.addControllerService(connectionPoolService.getIdentifier(), connectionPoolService);
        testRunner.enableControllerService(connectionPoolService);
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
        testRunner.assertValid();
    }

    @Test
    public void shouldShowBulletinWhenSMBHostIsInvalid() throws Exception {
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final SmbConnectionPoolService connectionPoolService = mockSmbConnectionPoolService();
        when(connectionPoolService.getHostname()).thenReturn("this.host.should.not.exists");
        testRunner.setProperty(SMB_CONNECTION_POOL_SERVICE, connectionPoolService.getIdentifier());
        testRunner.addControllerService(connectionPoolService.getIdentifier(), connectionPoolService);
        testRunner.enableControllerService(connectionPoolService);
        testRunner.run();
        assertEquals(1, testRunner.getLogger().getErrorMessages().size());
    }

    @Test
    public void shouldUseRecordWriterProperly() throws Exception {
        final Set<String> testFiles = new HashSet<>(asList(
                "1.txt",
                "directory\\2.txt",
                "directory\\subdirectory\\3.txt",
                "directory\\subdirectory2\\4.txt",
                "directory\\subdirectory3\\5.txt"
        ));
        nifiSmbClient.createDirectory("directory\\subdirectory");
        nifiSmbClient.createDirectory("directory\\subdirectory2");
        nifiSmbClient.createDirectory("directory\\subdirectory3");
        testFiles.forEach(file -> writeFile(file, generateContentWithSize(4)));

        waitForFilesToAppear(testFiles.size());

        final TestRunner testRunner = newTestRunner(ListSmb.class);
        final MockRecordWriter writer = new MockRecordWriter(null, false);
        final SimpleRecordSchema simpleRecordSchema = SmbListableEntity.getRecordSchema();
        testRunner.addControllerService("writer", writer);
        testRunner.enableControllerService(writer);
        testRunner.setProperty("listing-strategy", "none");
        testRunner.setProperty("record-writer", "writer");
        testRunner.setProperty("min-age", "0");
        configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        final String result = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getContent();
        final int identifierColumnIndex = simpleRecordSchema.getFieldNames().indexOf("identifier");
        final Set<String> actual = Arrays.stream(result.split("\n"))
                .map(row -> row.split(",")[identifierColumnIndex])
                .collect(toSet());
        assertEquals(testFiles, actual);
        testRunner.assertValid();
    }

    @Test
    public void shouldWriteFlowFileAttributesProperly() throws Exception {
        final Set<String> testFiles = new HashSet<>(asList(
                "1.txt", "directory\\2.txt", "directory\\subdirectory\\3.txt"
        ));
        nifiSmbClient.createDirectory("directory\\subdirectory");
        testFiles.forEach(file -> writeFile(file, generateContentWithSize(4)));
        Thread.sleep(1000);
        waitForFilesToAppear(3);
        final TestRunner testRunner = newTestRunner(ListSmb.class);
        testRunner.setProperty("listing-strategy", "none");
        testRunner.setProperty("min-age", "0");
        configureTestRunnerForSambaDockerContainer(testRunner);
        testRunner.run();
        testRunner.assertTransferCount(REL_SUCCESS, 3);
        final Set<String> result = testRunner.getFlowFilesForRelationship(REL_SUCCESS)
                .stream()
                .map(MockFlowFile::getAttributes)
                .map(attributes -> attributes.get("identifier"))
                .collect(toSet());
        assertEquals(testFiles, result);
        testRunner.assertValid();
    }

    @Test
    public void shouldFilterOlderFiles() throws Exception {
        final ListSmb underTest = new ListSmb();
        final ProcessContext mockProcessContext = mock(ProcessContext.class);
        final SmbConnectionPoolService connectionPoolService = mockSmbConnectionPoolService();
        mockProperty(mockProcessContext, MINIMUM_AGE, "0");
        mockProperty(mockProcessContext, DIRECTORY, new MockPropertyValue(null));
        mockProperty(mockProcessContext, SKIP_FILES_WITH_SUFFIX, new MockPropertyValue(null));
        mockProperty(mockProcessContext, SMB_CONNECTION_POOL_SERVICE, connectionPoolService);

        writeFile("1.txt", generateContentWithSize(1));
        waitForFilesToAppear(1);
        final long latestTimeStamp = nifiSmbClient.listRemoteFiles("").findFirst().get().getTimestamp();

        underTest.updateScheduledTrue();
        assertEquals(1, underTest.performListing(mockProcessContext, null, null).size());
        assertEquals(1, underTest.performListing(mockProcessContext, latestTimeStamp - 1, null).size());
        assertEquals(0, underTest.performListing(mockProcessContext, latestTimeStamp + 1, null).size());
    }

    @Test
    public void shouldFilterByGivenSuffix() throws Exception {
        final ListSmb underTest = new ListSmb();
        final ProcessContext mockProcessContext = mock(ProcessContext.class);
        final SmbConnectionPoolService connectionPoolService = mockSmbConnectionPoolService();
        mockProperty(mockProcessContext, DIRECTORY, null);
        mockProperty(mockProcessContext, MINIMUM_AGE, "0");
        mockProperty(mockProcessContext, SMB_CONNECTION_POOL_SERVICE, connectionPoolService);
        mockProperty(mockProcessContext, SKIP_FILES_WITH_SUFFIX, ".suffix");
        writeFile("should_list_this", generateContentWithSize(1));
        writeFile("should_skip_this.suffix", generateContentWithSize(1));
        waitForFilesToAppear(2);
        underTest.updateScheduledTrue();
        assertEquals(1, underTest.performListing(mockProcessContext, null, null).size());
    }

    @AfterEach
    public void afterEach() {
        nifiSmbClient.close();
        sambaContainer.stop();
    }

    private <T> void mockProperty(ProcessContext processContext, PropertyDescriptor descriptor, T value) {
        final PropertyValue mockValue = mock(PropertyValue.class);
        when(processContext.getProperty(descriptor)).thenReturn(mockValue);
        if (value instanceof String) {
            when(mockValue.getValue()).thenReturn((String) value);
        }
        if (value instanceof SmbConnectionPoolService) {
            when(mockValue.asControllerService(SmbConnectionPoolService.class)).thenReturn(
                    (SmbConnectionPoolService) value);
        }
        if (value instanceof Integer) {
            when(mockValue.asInteger()).thenReturn((Integer) value);
        }
    }

    private NiFiSmbClient createClient() throws IOException {
        return new NiFiSmbClientFactory().create(sambaContainer.getHost(),
                sambaContainer.getMappedPort(DEFAULT_SAMBA_PORT),
                "share",
                authenticationContext,
                smbClient);
    }

    private SmbConnectionPoolService mockSmbConnectionPoolService() {
        final SmbConnectionPoolService connectionPoolService = mock(SmbConnectionPoolService.class);
        when(connectionPoolService.getIdentifier()).thenReturn("connection-pool");
        when(connectionPoolService.getHostname()).thenReturn(sambaContainer.getHost());
        when(connectionPoolService.getPort()).thenReturn(sambaContainer.getMappedPort(DEFAULT_SAMBA_PORT));
        when(connectionPoolService.getShareName()).thenReturn("share");
        when(connectionPoolService.getSmbClient()).thenReturn(smbClient);
        when(connectionPoolService.getAuthenticationContext()).thenReturn(authenticationContext);
        return connectionPoolService;
    }

    private void configureTestRunnerForSambaDockerContainer(TestRunner testRunner) throws Exception {
        final SmbConnectionPoolService connectionPoolService = mockSmbConnectionPoolService();
        testRunner.setProperty(SMB_CONNECTION_POOL_SERVICE, "connection-pool");
        testRunner.addControllerService("connection-pool", connectionPoolService);
        testRunner.enableControllerService(connectionPoolService);
    }

    private String generateContentWithSize(int sizeInBytes) {
        byte[] bytes = new byte[sizeInBytes];
        fill(bytes, (byte) 1);
        return new String(bytes);
    }

    private void waitForFilesToAppear(Integer numberOfFiles) {
        await().until(() -> {
            try (Stream<SmbListableEntity> s = nifiSmbClient.listRemoteFiles("")) {
                return s.count() == numberOfFiles;
            }
        });
    }

    private void writeFile(String path, String content) {
        try (OutputStream outputStream = nifiSmbClient.getOutputStreamForFile(path)) {
            final InputStream inputStream = new ByteArrayInputStream(content.getBytes());
            copy(inputStream, outputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}