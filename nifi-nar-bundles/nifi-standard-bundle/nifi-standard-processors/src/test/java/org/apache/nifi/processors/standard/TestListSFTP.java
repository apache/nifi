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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTestServer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestListSFTP {
    private static final String REMOTE_DIRECTORY = "/";

    private static final int TEMPORARY_FILES = 3;

    private static final byte[] FILE_CONTENTS = String.class.getName().getBytes(StandardCharsets.UTF_8);

    private TestRunner runner;

    private SSHTestServer sshServer;

    @BeforeEach
    public void startServer() throws Exception {
        sshServer = new SSHTestServer();
        sshServer.startServer();
        writeTempFile();

        runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(SFTPTransfer.HOSTNAME, sshServer.getHost());
        runner.setProperty(SFTPTransfer.USERNAME, sshServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshServer.getPassword());
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshServer.getSSHPort()));
        runner.setProperty(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT, Boolean.FALSE.toString());

        runner.setProperty(ListSFTP.REMOTE_PATH, REMOTE_DIRECTORY);
        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);
        runner.assertValid();
    }

    @AfterEach
    public void stopServer() throws Exception {
        sshServer.stopServer();
        Files.deleteIfExists(Paths.get(sshServer.getVirtualFileSystemPath()));
    }

    @Test
    public void testRunFileFound() {
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
        runner.assertAllFlowFilesContainAttribute("filename");

        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(ListSFTP.REL_SUCCESS).get(0);
        retrievedFile.assertAttributeEquals("sftp.listing.user", sshServer.getUsername());
    }

    @Test
    public void testRunWithRecordWriter() throws InitializationException {
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
    public void testRunWithRecordWriterNoTracking() throws InitializationException {
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
    public void testRunWithRecordWriterByTimestamps() throws InitializationException {
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
    public void testRunWithRecordWriterByEntities() throws InitializationException {
        RecordSetWriterFactory recordWriter = getCsvRecordWriter();
        runner.addControllerService("csv-record-writer", recordWriter);
        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "csv-record-writer");
        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.BY_ENTITIES);
        runner.enableControllerService(recordWriter);
        DistributedMapCacheClient dmc = new MockCacheService();
        runner.addControllerService("dmc", dmc);
        runner.setProperty(ListedEntityTracker.TRACKING_STATE_CACHE, "dmc");
        runner.enableControllerService(dmc);
        runner.assertValid(dmc);
        runner.assertValid(recordWriter);
        runner.run(2);
        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);
    }

    @Test
    public void testFilesWithRestart() throws InitializationException {
        RecordSetWriterFactory recordWriter = getCsvRecordWriter();
        runner.addControllerService("csv-record-writer", recordWriter);
        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "csv-record-writer");
        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.BY_ENTITIES);
        runner.enableControllerService(recordWriter);
        DistributedMapCacheClient dmc = new MockCacheService();
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

    @Test
    public void testRemotePollBatchSizeEnforced() {
        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.NO_TRACKING);
        runner.setProperty(SFTPTransfer.REMOTE_POLL_BATCH_SIZE, "1");

        runner.run();
        // Of 3 items only 1 returned due to batch size
        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);

        runner.setProperty(SFTPTransfer.REMOTE_POLL_BATCH_SIZE, "2");

        runner.run();
        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 3);
    }

    @Test
    public void testVerificationSuccessful() {
        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor())
                .verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());
        final ConfigVerificationResult result = results.get(0);
        assertEquals(Outcome.SUCCESSFUL, result.getOutcome());
    }

    private void writeTempFile() {
        for (int i = 0; i < TEMPORARY_FILES; i++) {
            final File file = new File(sshServer.getVirtualFileSystemPath(), String.format("%s-%s", getClass().getSimpleName(), UUID.randomUUID()));
            try {
                Files.write(file.toPath(), FILE_CONTENTS);
                file.setLastModified(0);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private RecordSetWriterFactory getCsvRecordWriter() {
        return new MockRecordWriter("name, age");
    }
}
