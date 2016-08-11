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

import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFetchHDFS {

    private TestRunner runner;
    private TestableFetchHDFS proc;
    private NiFiProperties mockNiFiProperties;
    private KerberosProperties kerberosProperties;

    @Before
    public void setup() {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = KerberosProperties.create(mockNiFiProperties);

        proc = new TestableFetchHDFS(kerberosProperties);
        runner = TestRunners.newTestRunner(proc);
    }

    @Test
    public void testFetchStaticFileThatExists() throws IOException {
        final String file = "src/test/resources/testdata/randombytes-1";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue(new String("trigger flow file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testFetchStaticFileThatDoesNotExist() throws IOException {
        final String file = "src/test/resources/testdata/doesnotexist";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue(new String("trigger flow file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_FAILURE, 1);
    }

    @Test
    public void testFetchFileThatExistsFromIncomingFlowFile() throws IOException {
        final String file = "src/test/resources/testdata/randombytes-1";
        runner.setProperty(FetchHDFS.FILENAME, "${my.file}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("my.file", file);

        runner.enqueue(new String("trigger flow file"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testFilenameWithValidEL() throws IOException {
        final String file = "src/test/resources/testdata/${literal('randombytes-1')}";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue(new String("trigger flow file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testFilenameWithInvalidEL() throws IOException {
        final String file = "src/test/resources/testdata/${literal('randombytes-1'):foo()}";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.assertNotValid();
    }

    @Test
    public void testFilenameWithUnrecognizedEL() throws IOException {
        final String file = "data_${literal('testing'):substring(0,4)%7D";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue(new String("trigger flow file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_FAILURE, 1);
    }

    private static class TestableFetchHDFS extends FetchHDFS {
        private final KerberosProperties testKerberosProps;

        public TestableFetchHDFS(KerberosProperties testKerberosProps) {
            this.testKerberosProps = testKerberosProps;
        }

        @Override
        protected KerberosProperties getKerberosProperties() {
            return testKerberosProps;
        }


    }
}
