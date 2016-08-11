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

package org.apache.nifi.processors.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.hadoop.util.SequenceFileReader;
import org.apache.nifi.util.MockProcessContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class GetHDFSSequenceFileTest {
    private AbstractHadoopProcessor.HdfsResources hdfsResources;
    private GetHDFSSequenceFile getHDFSSequenceFile;
    private Configuration configuration;
    private FileSystem fileSystem;
    private UserGroupInformation userGroupInformation;
    private boolean isTicketOld;
    private boolean reloginTried;

    @Before
    public void setup() throws IOException {
        configuration = mock(Configuration.class);
        fileSystem = mock(FileSystem.class);
        userGroupInformation = mock(UserGroupInformation.class);
        hdfsResources = new AbstractHadoopProcessor.HdfsResources(configuration, fileSystem, userGroupInformation);
        getHDFSSequenceFile = new TestableGetHDFSSequenceFile();
        getHDFSSequenceFile.kerberosProperties = mock(KerberosProperties.class);
        isTicketOld = false;
        reloginTried = false;
        init();
    }

    private void init() throws IOException {
        final MockProcessContext context = new MockProcessContext(getHDFSSequenceFile);
        getHDFSSequenceFile.init(mock(ProcessorInitializationContext.class));
        getHDFSSequenceFile.onScheduled(context);
    }

    private void getFlowFilesWithUgi() throws Exception {
        SequenceFileReader reader = mock(SequenceFileReader.class);
        Path file = mock(Path.class);
        getHDFSSequenceFile.getFlowFiles(configuration, fileSystem, reader, file);
        ArgumentCaptor<PrivilegedExceptionAction> privilegedExceptionActionArgumentCaptor = ArgumentCaptor.forClass(PrivilegedExceptionAction.class);
        verifyNoMoreInteractions(reader);
        verify(userGroupInformation).doAs(privilegedExceptionActionArgumentCaptor.capture());
        privilegedExceptionActionArgumentCaptor.getValue().run();
        verify(reader).readSequenceFile(file, configuration, fileSystem);
    }

    @Test
    public void getFlowFilesWithUgiAndNewTicketShouldCallDoAsAndNotRelogin() throws Exception {
        getFlowFilesWithUgi();
        assertFalse(reloginTried);
    }

    @Test
    public void getFlowFilesWithUgiAndOldTicketShouldCallDoAsAndRelogin() throws Exception {
        isTicketOld = true;
        getFlowFilesWithUgi();
        assertTrue(reloginTried);
    }

    @Test
    public void testGetFlowFilesNoUgiShouldntCallDoAs() throws Exception {
        hdfsResources = new AbstractHadoopProcessor.HdfsResources(configuration, fileSystem, null);
        init();
        SequenceFileReader reader = mock(SequenceFileReader.class);
        Path file = mock(Path.class);
        getHDFSSequenceFile.getFlowFiles(configuration, fileSystem, reader, file);
        verify(reader).readSequenceFile(file, configuration, fileSystem);
    }

    public class TestableGetHDFSSequenceFile extends GetHDFSSequenceFile {
        @Override
        HdfsResources resetHDFSResources(String configResources, ProcessContext context) throws IOException {
            return hdfsResources;
        }

        @Override
        public void onScheduled(ProcessContext context) throws IOException {
            abstractOnScheduled(context);
        }

        @Override
        protected KerberosProperties getKerberosProperties() {
            return kerberosProperties;
        }

        @Override
        protected boolean isTicketOld() {
            return isTicketOld;
        }

        @Override
        protected void tryKerberosRelogin(UserGroupInformation ugi) {
            reloginTried = true;
        }
    }
}
