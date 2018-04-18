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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jcraft.jsch.ChannelSftp.SSH_FX_FAILURE;
import static com.jcraft.jsch.ChannelSftp.SSH_FX_NO_SUCH_FILE;
import static com.jcraft.jsch.ChannelSftp.SSH_FX_PERMISSION_DENIED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSFTPTransfer {

    private static final Logger logger = LoggerFactory.getLogger(TestSFTPTransfer.class);

    private SFTPTransfer createSftpTransfer(ProcessContext processContext, ChannelSftp channel) {
        final ComponentLog componentLog = mock(ComponentLog.class);
        return new SFTPTransfer(processContext, componentLog) {
            @Override
            protected ChannelSftp getChannel(FlowFile flowFile) throws IOException {
                return channel;
            }
        };
    }

    @Test
    public void testEnsureDirectoryExistsAlreadyExisted() throws IOException, SftpException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final ChannelSftp channel = mock(ChannelSftp.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, channel);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(channel).stat(eq("/dir1/dir2/dir3"));
    }

    @Test
    public void testEnsureDirectoryExistsFailedToStat() throws IOException, SftpException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final ChannelSftp channel = mock(ChannelSftp.class);
        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(channel.stat("/dir1/dir2/dir3")).thenThrow(new SftpException(SSH_FX_FAILURE, "Failure"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, channel);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        try {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
            fail("Should fail");
        } catch (IOException e) {
            assertEquals("Failed to determine if remote directory exists at /dir1/dir2/dir3 due to 4: Failure", e.getMessage());
        }

        // Dir existence check should be done by stat
        verify(channel).stat(eq("/dir1/dir2/dir3"));
    }

    @Test
    public void testEnsureDirectoryExistsNotExisted() throws IOException, SftpException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final ChannelSftp channel = mock(ChannelSftp.class);
        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(channel.stat("/dir1/dir2/dir3")).thenThrow(new SftpException(SSH_FX_NO_SUCH_FILE, "No such file"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, channel);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(channel).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(channel).stat(eq("/dir1/dir2")); // so, dir2 was checked
        verify(channel).mkdir(eq("/dir1/dir2/dir3")); // dir2 existed, so dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsParentNotExisted() throws IOException, SftpException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final ChannelSftp channel = mock(ChannelSftp.class);
        // stat for the dir1 was successful, simulating that dir1 exists, but no dir2 and dir3.
        when(channel.stat("/dir1/dir2/dir3")).thenThrow(new SftpException(SSH_FX_NO_SUCH_FILE, "No such file"));
        when(channel.stat("/dir1/dir2")).thenThrow(new SftpException(SSH_FX_NO_SUCH_FILE, "No such file"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, channel);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(channel).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(channel).stat(eq("/dir1/dir2")); // dir2 was not found, too
        verify(channel).stat(eq("/dir1")); // dir1 was found
        verify(channel).mkdir(eq("/dir1/dir2")); // dir1 existed, so dir2 was created.
        verify(channel).mkdir(eq("/dir1/dir2/dir3")); // then dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsNotExistedFailedToCreate() throws IOException, SftpException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final ChannelSftp channel = mock(ChannelSftp.class);
        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(channel.stat("/dir1/dir2/dir3")).thenThrow(new SftpException(SSH_FX_NO_SUCH_FILE, "No such file"));
        // Failed to create dir3.
        doThrow(new SftpException(SSH_FX_FAILURE, "Failed")).when(channel).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, channel);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        try {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
            fail("Should fail");
        } catch (IOException e) {
            assertEquals("Failed to create remote directory /dir1/dir2/dir3 due to 4: Failed", e.getMessage());
        }

        // Dir existence check should be done by stat
        verify(channel).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(channel).stat(eq("/dir1/dir2")); // so, dir2 was checked
        verify(channel).mkdir(eq("/dir1/dir2/dir3")); // dir2 existed, so dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyNotExisted() throws IOException, SftpException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final ChannelSftp channel = mock(ChannelSftp.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, channel);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(channel, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(channel).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyParentNotExisted() throws IOException, SftpException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final ChannelSftp channel = mock(ChannelSftp.class);
        final AtomicInteger mkdirCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            final int cnt = mkdirCount.getAndIncrement();
            if (cnt == 0) {
                // If the parent dir does not exist, no such file exception is thrown.
                throw new SftpException(SSH_FX_NO_SUCH_FILE, "Failure");
            } else {
                logger.info("Created the dir successfully for the 2nd time");
            }
            return true;
        }).when(channel).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, channel);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(channel, times(0)).stat(eq("/dir1/dir2/dir3"));
        // dir3 was created blindly, but failed for the 1st time, and succeeded for the 2nd time.
        verify(channel, times(2)).mkdir(eq("/dir1/dir2/dir3"));
        verify(channel).mkdir(eq("/dir1/dir2")); // dir2 was created successfully.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyAlreadyExisted() throws IOException, SftpException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final ChannelSftp channel = mock(ChannelSftp.class);
        // If the dir existed, a failure exception is thrown, but should be swallowed.
        doThrow(new SftpException(SSH_FX_FAILURE, "Failure")).when(channel).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, channel);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(channel, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(channel).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyFailed() throws IOException, SftpException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final ChannelSftp channel = mock(ChannelSftp.class);
        doThrow(new SftpException(SSH_FX_PERMISSION_DENIED, "Permission denied")).when(channel).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, channel);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        try {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
            fail("Should fail");
        } catch (IOException e) {
            assertEquals("Could not blindly create remote directory due to Permission denied", e.getMessage());
        }

        // stat should not be called.
        verify(channel, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(channel).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

}
