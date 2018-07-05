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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.processors.standard.FetchSCP;
import org.apache.nifi.processors.standard.GetSCP;
import org.apache.nifi.processors.standard.ListRemoteFiles;
import org.apache.nifi.processors.standard.PutSCP;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import static org.apache.nifi.processors.standard.FetchFileTransfer.REMOTE_FILENAME;

public class TestSCPTransfer {

    private static final Logger logger = LoggerFactory.getLogger(TestSCPTransfer.class);

    private TestRunner putSCPRunner;


    private static SSHTestServer sshTestServer;

    @BeforeClass
    public static void setupSSHD() throws IOException {
        sshTestServer = new SSHTestServer();
        sshTestServer.startServer();
    }

    @AfterClass
    public static void cleanupSSHD() throws IOException {
        sshTestServer.stopServer();
    }

    @Before
    public void setup(){
        putSCPRunner = TestRunners.newTestRunner(PutSCP.class);
        putSCPRunner.setProperty(SSHTransfer.HOSTNAME, "localhost");
        putSCPRunner.setProperty(SSHTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        putSCPRunner.setProperty(SSHTransfer.USERNAME, sshTestServer.getUsername());
        putSCPRunner.setProperty(SSHTransfer.PASSWORD, sshTestServer.getPassword());
        putSCPRunner.setProperty(SSHTransfer.STRICT_HOST_KEY_CHECKING, "false");
        putSCPRunner.setProperty(SSHTransfer.BATCH_SIZE, "2");
        putSCPRunner.setProperty(SSHTransfer.REMOTE_PATH, "/");
        putSCPRunner.setProperty(SSHTransfer.TEMP_FILENAME, "${filename}");
        putSCPRunner.setProperty(SSHTransfer.REJECT_ZERO_BYTE, "true");
        putSCPRunner.setProperty(SSHTransfer.CONFLICT_RESOLUTION, "NONE");
        putSCPRunner.setProperty(SSHTransfer.CREATE_DIRECTORY, "false");
        putSCPRunner.setProperty(SSHTransfer.DATA_TIMEOUT, "30 sec");
        putSCPRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void testPutSCPFile() throws IOException {
        Map<String,String> attributes = new HashMap<>();
        attributes.put("filename", "testfile.txt");

        String testFile = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "hello.txt";
        putSCPRunner.enqueue(Paths.get(testFile), attributes);
        putSCPRunner.run();

        putSCPRunner.assertTransferCount(PutSCP.REL_SUCCESS, 1);
    }
}
