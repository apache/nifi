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

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.common.Factory;
import net.schmizz.sshj.sftp.Response;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPException;
import net.schmizz.sshj.userauth.method.AuthKeyboardInteractive;
import net.schmizz.sshj.userauth.method.AuthMethod;
import net.schmizz.sshj.userauth.method.AuthPassword;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyContext;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSFTPTransfer {

    private static final Logger logger = LoggerFactory.getLogger(TestSFTPTransfer.class);

    private SFTPTransfer createSftpTransfer(ProcessContext processContext, SFTPClient sftpClient) {
        final ComponentLog componentLog = mock(ComponentLog.class);
        return new SFTPTransfer(processContext, componentLog) {
            @Override
            protected SFTPClient getSFTPClient(FlowFile flowFile) {
                return sftpClient;
            }
        };
    }

    @Test
    public void testEnsureDirectoryExistsAlreadyExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3"));
    }

    @Test
    public void testEnsureDirectoryExistsFailedToStat() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);
        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.FAILURE, "Failure"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        try {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
            fail("Should fail");
        } catch (IOException e) {
            assertEquals("Failed to determine if remote directory exists at /dir1/dir2/dir3 due to 4: Failure", e.getMessage());
        }

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3"));
    }

    @Test
    public void testEnsureDirectoryExistsNotExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);
        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(sftpClient).stat(eq("/dir1/dir2")); // so, dir2 was checked
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir2 existed, so dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsParentNotExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);

        // stat for the dir1 was successful, simulating that dir1 exists, but no dir2 and dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));
        when(sftpClient.stat("/dir1/dir2")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(sftpClient).stat(eq("/dir1/dir2")); // dir2 was not found, too
        verify(sftpClient).stat(eq("/dir1")); // dir1 was found
        verify(sftpClient).mkdir(eq("/dir1/dir2")); // dir1 existed, so dir2 was created.
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // then dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsNotExistedFailedToCreate() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);

        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));
        // Failed to create dir3.
        doThrow(new SFTPException(Response.StatusCode.FAILURE, "Failed")).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        try {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
            fail("Should fail");
        } catch (IOException e) {
            assertEquals("Failed to create remote directory /dir1/dir2/dir3 due to 4: Failed", e.getMessage());
        }

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(sftpClient).stat(eq("/dir1/dir2")); // so, dir2 was checked
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir2 existed, so dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyNotExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyParentNotExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        final AtomicInteger mkdirCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            final int cnt = mkdirCount.getAndIncrement();
            if (cnt == 0) {
                // If the parent dir does not exist, no such file exception is thrown.
                throw new SFTPException(Response.StatusCode.NO_SUCH_FILE, "Failure");
            } else {
                logger.info("Created the dir successfully for the 2nd time");
            }
            return true;
        }).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        // dir3 was created blindly, but failed for the 1st time, and succeeded for the 2nd time.
        verify(sftpClient, times(2)).mkdir(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2")); // dir2 was created successfully.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyAlreadyExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        // If the dir existed, a failure exception is thrown, but should be swallowed.
        doThrow(new SFTPException(Response.StatusCode.FAILURE, "Failure")).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyFailed() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        doThrow(new SFTPException(Response.StatusCode.PERMISSION_DENIED, "Permission denied")).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        try {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
            fail("Should fail");
        } catch (IOException e) {
            assertEquals("Could not blindly create remote directory due to Permission denied", e.getMessage());
        }

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testRestrictSSHOptions() {
        Map<PropertyDescriptor, String> propertyDescriptorValues = new HashMap<>();

        DefaultConfig defaultConfig = new DefaultConfig();

        String allowedMac = defaultConfig.getMACFactories().stream().map(Factory.Named::getName).collect(Collectors.toList()).get(0);
        String allowedKeyAlgorithm = defaultConfig.getKeyAlgorithms().stream().map(Factory.Named::getName).collect(Collectors.toList()).get(0);
        String allowedKeyExchangeAlgorithm = defaultConfig.getKeyExchangeFactories().stream().map(Factory.Named::getName).collect(Collectors.toList()).get(0);
        String allowedCipher = defaultConfig.getCipherFactories().stream().map(Factory.Named::getName).collect(Collectors.toList()).get(0);

        propertyDescriptorValues.put(SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED, allowedMac);
        propertyDescriptorValues.put(SFTPTransfer.CIPHERS_ALLOWED, allowedCipher);
        propertyDescriptorValues.put(SFTPTransfer.KEY_ALGORITHMS_ALLOWED, allowedKeyAlgorithm);
        propertyDescriptorValues.put(SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED, allowedKeyExchangeAlgorithm);
        MockPropertyContext mockPropertyContext = new MockPropertyContext(propertyDescriptorValues);
        SFTPTransfer sftpTransfer = new SFTPTransfer(mockPropertyContext, new MockComponentLogger());

        sftpTransfer.updateConfigAlgorithms(defaultConfig);

        assertEquals(1, defaultConfig.getCipherFactories().size());
        assertEquals(1, defaultConfig.getKeyAlgorithms().size());
        assertEquals(1, defaultConfig.getKeyExchangeFactories().size());
        assertEquals(1, defaultConfig.getMACFactories().size());

        assertEquals(allowedCipher, defaultConfig.getCipherFactories().get(0).getName());
        assertEquals(allowedKeyAlgorithm, defaultConfig.getKeyAlgorithms().get(0).getName());
        assertEquals(allowedKeyExchangeAlgorithm, defaultConfig.getKeyExchangeFactories().get(0).getName());
        assertEquals(allowedMac, defaultConfig.getMACFactories().get(0).getName());
    }

    @Test
    public void testGetAuthMethodsPassword() {
        final String password = UUID.randomUUID().toString();
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.PASSWORD)).thenReturn(new MockPropertyValue(password));
        when(processContext.getProperty(SFTPTransfer.PRIVATE_KEY_PATH)).thenReturn(new MockPropertyValue(null));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);

        final SSHClient sshClient = new SSHClient();
        final List<AuthMethod> authMethods = sftpTransfer.getAuthMethods(sshClient, null);
        assertFalse("Authentication Methods not found", authMethods.isEmpty());

        final Optional<AuthMethod> authPassword = authMethods.stream().filter(authMethod -> authMethod instanceof AuthPassword).findFirst();
        assertTrue("Password Authentication not found", authPassword.isPresent());

        final Optional<AuthMethod> authKeyboardInteractive = authMethods.stream().filter(authMethod -> authMethod instanceof AuthKeyboardInteractive).findFirst();
        assertTrue("Keyboard Interactive Authentication not found", authKeyboardInteractive.isPresent());
    }

    @Test
    public void testGetAuthMethodsPrivateKeyLoadFailed() throws IOException {
        final File privateKeyFile = File.createTempFile(TestSFTPTransfer.class.getSimpleName(), ".key");
        privateKeyFile.deleteOnExit();

        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.PASSWORD)).thenReturn(new MockPropertyValue(null));
        when(processContext.getProperty(SFTPTransfer.PRIVATE_KEY_PATH)).thenReturn(new MockPropertyValue(privateKeyFile.getAbsolutePath()));
        when(processContext.getProperty(SFTPTransfer.PRIVATE_KEY_PASSPHRASE)).thenReturn(new MockPropertyValue(null));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);

        final SSHClient sshClient = new SSHClient();
        assertThrows(ProcessException.class, () -> sftpTransfer.getAuthMethods(sshClient, null));
    }
}
