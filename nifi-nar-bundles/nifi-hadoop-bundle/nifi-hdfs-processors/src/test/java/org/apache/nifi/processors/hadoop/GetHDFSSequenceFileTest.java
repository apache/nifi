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
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.ietf.jgss.GSSException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class GetHDFSSequenceFileTest {
    private HdfsResources hdfsResourcesLocal;
    private GetHDFSSequenceFile getHDFSSequenceFile;
    private Configuration configuration;
    private FileSystem fileSystem;
    private UserGroupInformation userGroupInformation;
    private boolean reloginTried;

    @BeforeEach
    public void setup() throws IOException {
        configuration = mock(Configuration.class);
        fileSystem = mock(FileSystem.class);
        userGroupInformation = mock(UserGroupInformation.class);
        hdfsResourcesLocal = new HdfsResources(configuration, fileSystem, userGroupInformation, null);
        getHDFSSequenceFile = new TestableGetHDFSSequenceFile(new KerberosProperties(null), userGroupInformation);
        reloginTried = false;
        init();
    }

    private void init() throws IOException {
        final MockProcessContext context = new MockProcessContext(getHDFSSequenceFile);
        ProcessorInitializationContext mockProcessorInitializationContext = mock(ProcessorInitializationContext.class);
        when(mockProcessorInitializationContext.getLogger()).thenReturn(new MockComponentLog("GetHDFSSequenceFileTest", getHDFSSequenceFile ));
        getHDFSSequenceFile.initialize(mockProcessorInitializationContext);
        getHDFSSequenceFile.init(mockProcessorInitializationContext);
        getHDFSSequenceFile.onScheduled(context);
    }

    @Test
    public void getFlowFilesWithUgiAndNewTicketShouldCallDoAsAndNotRelogin() throws Exception {
        SequenceFileReader reader = mock(SequenceFileReader.class);
        Path file = mock(Path.class);
        getHDFSSequenceFile.kerberosProperties = mock(KerberosProperties.class);
        getHDFSSequenceFile.getFlowFiles(configuration, fileSystem, reader, file);
        ArgumentCaptor<PrivilegedExceptionAction> privilegedExceptionActionArgumentCaptor = ArgumentCaptor.forClass(PrivilegedExceptionAction.class);
        verifyNoMoreInteractions(reader);
        verify(userGroupInformation).doAs(privilegedExceptionActionArgumentCaptor.capture());
        privilegedExceptionActionArgumentCaptor.getValue().run();
        verify(reader).readSequenceFile(file, configuration, fileSystem);
        assertFalse(reloginTried);
    }

    @Test
    public void testGetFlowFilesNoUgiShouldntCallDoAs() throws Exception {
        getHDFSSequenceFile = new TestableGetHDFSSequenceFile(new KerberosProperties(null), null);
        hdfsResourcesLocal = new HdfsResources(configuration, fileSystem, null, null);
        init();
        SequenceFileReader reader = mock(SequenceFileReader.class);
        Path file = mock(Path.class);
        getHDFSSequenceFile.getFlowFiles(configuration, fileSystem, reader, file);
        verify(reader).readSequenceFile(file, configuration, fileSystem);
    }

    @Test
    public void testGSSExceptionOnDoAs() throws Exception {
        NiFiProperties mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        GetHDFSSequenceFile testSubject = new TestableGetHDFSSequenceFile(getHDFSSequenceFile.kerberosProperties, userGroupInformation, true);
        TestRunner runner = TestRunners.newTestRunner(testSubject);
        runner.setProperty(GetHDFSSequenceFile.DIRECTORY, "path/does/not/exist");
        runner.run();
        // assert no flowfiles transferred to outgoing relationships
        runner.assertTransferCount(MoveHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(MoveHDFS.REL_FAILURE, 0);
    }

    public class TestableGetHDFSSequenceFile extends GetHDFSSequenceFile {

        UserGroupInformation userGroupInformation;
        private KerberosProperties kerberosProperties;


        public TestableGetHDFSSequenceFile(KerberosProperties kerberosProperties, UserGroupInformation ugi) throws IOException {
            this(kerberosProperties, ugi, false);
        }

        public TestableGetHDFSSequenceFile(KerberosProperties kerberosProperties, UserGroupInformation ugi, boolean failOnDoAs) throws IOException {
            this.kerberosProperties = kerberosProperties;
            this.userGroupInformation = ugi;
            if(failOnDoAs && userGroupInformation != null) {
                try {
                    when(userGroupInformation.doAs(any(PrivilegedExceptionAction.class))).thenThrow(new IOException(new GSSException(13)));
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }


        @Override
        HdfsResources resetHDFSResources(final List<String> resourceLocations, ProcessContext context) throws IOException {
            return hdfsResourcesLocal;
        }

        @Override
        public void onScheduled(ProcessContext context) throws IOException {
            abstractOnScheduled(context);
        }

        @Override
        protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
            return kerberosProperties;
        }

        protected UserGroupInformation getUserGroupInformation() {
            return userGroupInformation;
        }
    }
}
