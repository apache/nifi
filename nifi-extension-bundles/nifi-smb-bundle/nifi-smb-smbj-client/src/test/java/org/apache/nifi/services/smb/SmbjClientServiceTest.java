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

import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SmbjClientServiceTest {

    @Mock
    Session session;

    @Mock
    DiskShare share;

    @InjectMocks
    SmbjClientService underTest;

    private AutoCloseable mockCloseable;

    @BeforeEach
    public void beforeEach() {
        MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    public void closeMock() throws Exception {
        if (mockCloseable != null) {
            mockCloseable.close();
        }
    }

    @Test
    public void shouldCreateDirectoriesRecursively() throws Exception {
        when(session.connectShare(anyString())).thenReturn(share);
        when(share.fileExists("directory")).thenReturn(true);
        when(share.fileExists("path")).thenReturn(false);
        when(share.fileExists("to")).thenReturn(false);
        when(share.fileExists("create")).thenReturn(false);

        underTest.ensureDirectory("directory/path/to/create");

        verify(share).mkdir("directory/path");
        verify(share).mkdir("directory/path/to");
        verify(share).mkdir("directory/path/to/create");

    }

}