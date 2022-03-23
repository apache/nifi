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
package org.apache.nifi.processors.standard;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTestServer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestListSFTP {
    private static final String REMOTE_DIRECTORY = "/";

    private static final byte[] FILE_CONTENTS = String.class.getName().getBytes(StandardCharsets.UTF_8);

    private TestRunner runner;

    private SSHTestServer sshServer;

    private List<File> testFileNames;

    @Before
    public void setUp() throws Exception {
        sshServer = new SSHTestServer();
        sshServer.startServer();
        testFileNames = new ArrayList<File>();
        writeTempFile(3);
        runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(ListSFTP.HOSTNAME, sshServer.getHost());
        runner.setProperty(ListSFTP.USERNAME, sshServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshServer.getPassword());
        runner.setProperty(FTPTransfer.PORT, Integer.toString(sshServer.getSSHPort()));
        runner.setProperty(ListSFTP.REMOTE_PATH, REMOTE_DIRECTORY);
        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);
        runner.assertValid();
        assertVerificationSuccess();
    }

    @After
    public void tearDown() throws Exception {
        sshServer.stopServer();
        Files.deleteIfExists(Paths.get(sshServer.getVirtualFileSystemPath()));
    }

    @Test
    public void testRunFileFound() throws InterruptedException {
        runner.run(1);
        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 3);
        runner.assertAllFlowFilesContainAttribute("sftp.remote.host");
        runner.assertAllFlowFilesContainAttribute("sftp.remote.port");
        runner.assertAllFlowFilesContainAttribute("sftp.listing.user");
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_OWNER_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_GROUP_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_PERMISSIONS_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_SIZE_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute( "filename");

        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(ListSFTP.REL_SUCCESS).get(0);
        retrievedFile.assertAttributeEquals("sftp.listing.user", sshServer.getUsername());
    }

    @Test
    public void testRunWithRecordWriter() throws InitializationException, InterruptedException {
        RecordSetWriterFactory recordWriter = getCsvRecordWriter();
        runner.addControllerService("csv-record-writer", recordWriter);
        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "csv-record-writer");
        runner.enableControllerService(recordWriter);
        runner.assertValid(recordWriter);
        runner.run(2);
        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(CoreAttributes.MIME_TYPE.key());
    }

    @Test
    public void testRunWithRecordWriterNoTracking() throws InitializationException, InterruptedException {
        RecordSetWriterFactory recordWriter = getCsvRecordWriter();
        runner.addControllerService("csv-record-writer", recordWriter);
        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "csv-record-writer");
        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.NO_TRACKING);
        runner.enableControllerService(recordWriter);
        runner.assertValid(recordWriter);
        runner.run(2);
        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 2);
    }

    @Test
    public void testRunWithRecordWriterByTimestamps() throws InitializationException, InterruptedException {
        RecordSetWriterFactory recordWriter = getCsvRecordWriter();
        runner.addControllerService("csv-record-writer", recordWriter);
        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "csv-record-writer");
        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.BY_TIMESTAMPS);
        runner.enableControllerService(recordWriter);
        runner.assertValid(recordWriter);
        runner.run(2);
        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);
    }

    @Test
    public void testRunWithRecordWriterByEntities() throws InitializationException, InterruptedException {
        RecordSetWriterFactory recordWriter = getCsvRecordWriter();
        runner.addControllerService("csv-record-writer", recordWriter);
        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "csv-record-writer");
        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.BY_ENTITIES);
        runner.enableControllerService(recordWriter);
        DistributedMapCacheClient dmc = new MockCacheService<>();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(ListedEntityTracker.TRACKING_STATE_CACHE, "dmc");
        runner.enableControllerService(dmc);
        runner.assertValid(dmc);
        runner.assertValid(recordWriter);
        runner.run(2);
        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);
    }

    @Test
    public void testFilesWithRestart() throws InitializationException, InterruptedException {
        RecordSetWriterFactory recordWriter = getCsvRecordWriter();
        runner.addControllerService("csv-record-writer", recordWriter);
        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "csv-record-writer");
        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.BY_ENTITIES);
        runner.enableControllerService(recordWriter);
        DistributedMapCacheClient dmc = new MockCacheService<>();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(ListedEntityTracker.TRACKING_STATE_CACHE, "dmc");
        runner.enableControllerService(dmc);
        runner.assertValid();
        runner.run(2);
        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);
    }

    @Test
    public void testRunFileNotFoundMinSizeFiltered() {
        runner.setProperty(ListFile.MIN_SIZE, "1KB");

        runner.run(2);

        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 0);
    }

    private void assertVerificationSuccess() {
        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor())
                .verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());
        final ConfigVerificationResult result = results.get(0);
        assertEquals(Outcome.SUCCESSFUL, result.getOutcome());
    }

    private void writeTempFile(final int count) {
        for (int i = 0; i < count; i++) {
            final File file = new File(sshServer.getVirtualFileSystemPath(), String.format("%s-%s", getClass().getSimpleName(), UUID.randomUUID()));
            try {
                Files.write(file.toPath(), FILE_CONTENTS);
                file.setLastModified(0);
                testFileNames.add(file);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        assert(new File(sshServer.getVirtualFileSystemPath()).listFiles().length == count);
    }

    private RecordSetWriterFactory getCsvRecordWriter() {
        return new MockRecordWriter("name, age");
    }
}
