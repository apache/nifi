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
package org.apache.nifi.services.smb;

import com.hierynomus.mserref.NtStatus;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SmbjClientServiceTest {

    @Mock
    Session session;

    @Mock
    DiskShare share;

    @Mock
    ComponentLog logger;

    @InjectMocks
    SmbjClientService underTest;

    @BeforeEach
    void beforeEach() {
        MockitoAnnotations.openMocks(this);

        when(session.connectShare(anyString())).thenReturn(share);
    }

    @Test
    void ensureDirectoryShouldCreateDirectoriesRecursively() throws Exception {
        when(share.fileExists("directory")).thenReturn(true);
        when(share.fileExists("path")).thenReturn(false);
        when(share.fileExists("to")).thenReturn(false);
        when(share.fileExists("create")).thenReturn(false);

        underTest.ensureDirectory("directory/path/to/create");

        verify(share).mkdir("directory/path");
        verify(share).mkdir("directory/path/to");
        verify(share).mkdir("directory/path/to/create");
    }

    @Test
    void listFilesShouldHandlePermissionErrors() {
        mockOpenDirectory("dir1", NtStatus.STATUS_ACCESS_DENIED);
        mockOpenDirectory("dir2", NtStatus.STATUS_BAD_NETWORK_NAME);
        mockOpenDirectory("dir3", NtStatus.STATUS_OTHER);

        assertEquals(0, underTest.listFiles("dir1").count());
        assertEquals(0, underTest.listFiles("dir2").count());
        assertThrows(SMBApiException.class, () -> underTest.listFiles("dir3").count());
    }

    private void mockOpenDirectory(String directoryName, NtStatus responseStatus) {
        when(share.openDirectory(eq(directoryName), any(), any(), any(), any(), any()))
                .thenThrow(new SMBApiException(responseStatus.getValue(), null, null));
    }
}