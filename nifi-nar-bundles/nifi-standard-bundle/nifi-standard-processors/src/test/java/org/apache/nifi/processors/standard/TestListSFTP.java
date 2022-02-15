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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTestServer;
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

    private String tempFileName;

    @Before
    public void setUp() throws Exception {
        sshServer = new SSHTestServer();
        sshServer.startServer();

        writeTempFile();

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
    }

    @Test
    public void testRunFileFound() {
        runner.run();

        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);
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
        retrievedFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), tempFileName);
    }

    @Test
    public void testRunFileNotFoundMinSizeFiltered() {
        runner.setProperty(ListFile.MIN_SIZE, "1KB");

        runner.run();

        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 0);
    }

    private void assertVerificationSuccess() {
        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor())
                .verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());
        final ConfigVerificationResult result = results.get(0);
        assertEquals(Outcome.SUCCESSFUL, result.getOutcome());
    }

    private void writeTempFile() {
        final File file = new File(sshServer.getVirtualFileSystemPath(), String.format("%s-%s", getClass().getSimpleName(), UUID.randomUUID()));
        try {
            Files.write(file.toPath(), FILE_CONTENTS);
            tempFileName = file.getName();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
