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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MoveHDFSTest {

    private static final String OUTPUT_DIRECTORY = "target/test-data-output";
    private static final String TEST_DATA_DIRECTORY = "src/test/resources/testdata";
    private static final String INPUT_DIRECTORY = "target/test-data-input";
    private NiFiProperties mockNiFiProperties;
    private KerberosProperties kerberosProperties;

    @BeforeClass
    public static void setUpSuite() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void setup() {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = new KerberosProperties(null);
    }

    @After
    public void teardown() {
        File inputDirectory = new File(INPUT_DIRECTORY);
        File outputDirectory = new File(OUTPUT_DIRECTORY);
        if (inputDirectory.exists()) {
            Assert.assertTrue("Could not delete input directory: " + inputDirectory, FileUtils.deleteQuietly(inputDirectory));
        }
        if (outputDirectory.exists()) {
            Assert.assertTrue("Could not delete output directory: " + outputDirectory, FileUtils.deleteQuietly(outputDirectory));
        }
    }

    @Test
    public void testOutputDirectoryValidator() {
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        Collection<ValidationResult> results;
        ProcessContext pc;

        results = new HashSet<>();
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, "/source");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("Output Directory is required"));
        }
    }

    @Test
    public void testBothInputAndOutputDirectoriesAreValid() {
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        Collection<ValidationResult> results;
        ProcessContext pc;

        results = new HashSet<>();
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testOnScheduledShouldRunCleanly() throws IOException {
        FileUtils.copyDirectory(new File(TEST_DATA_DIRECTORY), new File(INPUT_DIRECTORY));
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
        runner.enqueue(new byte[0]);
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
        runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
        Assert.assertEquals(7, flowFiles.size());
    }

    @Test
    public void testDotFileFilterIgnore() throws IOException {
        FileUtils.copyDirectory(new File(TEST_DATA_DIRECTORY), new File(INPUT_DIRECTORY));
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.IGNORE_DOTTED_FILES, "true");
        runner.enqueue(new byte[0]);
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
        runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
        Assert.assertEquals(7, flowFiles.size());
        Assert.assertTrue(new File(INPUT_DIRECTORY, ".dotfile").exists());
    }

    @Test
    public void testDotFileFilterInclude() throws IOException {
        FileUtils.copyDirectory(new File(TEST_DATA_DIRECTORY), new File(INPUT_DIRECTORY));
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.IGNORE_DOTTED_FILES, "false");
        runner.enqueue(new byte[0]);
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
        runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
        Assert.assertEquals(8, flowFiles.size());
    }

    @Test
    public void testFileFilterRegex() throws IOException {
        FileUtils.copyDirectory(new File(TEST_DATA_DIRECTORY), new File(INPUT_DIRECTORY));
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.FILE_FILTER_REGEX, ".*\\.gz");
        runner.enqueue(new byte[0]);
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
        runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());
    }

    @Test
    public void testSingleFileAsInputCopy() throws IOException {
        FileUtils.copyDirectory(new File(TEST_DATA_DIRECTORY), new File(INPUT_DIRECTORY));
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY + "/randombytes-1");
        runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.OPERATION, "copy");
        runner.enqueue(new byte[0]);
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
        runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());
        Assert.assertTrue(new File(INPUT_DIRECTORY, "randombytes-1").exists());
        Assert.assertTrue(new File(OUTPUT_DIRECTORY, "randombytes-1").exists());
    }

    @Test
    public void testSingleFileAsInputMove() throws IOException {
        FileUtils.copyDirectory(new File(TEST_DATA_DIRECTORY), new File(INPUT_DIRECTORY));
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY + "/randombytes-1");
        runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
        runner.enqueue(new byte[0]);
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
        runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());
        Assert.assertFalse(new File(INPUT_DIRECTORY, "randombytes-1").exists());
        Assert.assertTrue(new File(OUTPUT_DIRECTORY, "randombytes-1").exists());
    }

    @Test
    public void testDirectoryWithSubDirectoryAsInputMove() throws IOException {
        FileUtils.copyDirectory(new File(TEST_DATA_DIRECTORY), new File(INPUT_DIRECTORY));
        File subdir = new File(INPUT_DIRECTORY, "subdir");
        FileUtils.copyDirectory(new File(TEST_DATA_DIRECTORY), subdir);
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
        runner.enqueue(new byte[0]);
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
        runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
        Assert.assertEquals(7, flowFiles.size());
        Assert.assertTrue(new File(INPUT_DIRECTORY).exists());
        Assert.assertTrue(subdir.exists());
    }

    @Test
    public void testEmptyInputDirectory() throws IOException {
        MoveHDFS proc = new TestableMoveHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        Files.createDirectories(Paths.get(INPUT_DIRECTORY));
        runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
        runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
        runner.enqueue(new byte[0]);
        Assert.assertEquals(0, Files.list(Paths.get(INPUT_DIRECTORY)).count());
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(MoveHDFS.REL_SUCCESS);
        runner.assertAllFlowFilesTransferred(MoveHDFS.REL_SUCCESS);
        Assert.assertEquals(0, flowFiles.size());
    }

    @Test
    public void testPutWhenAlreadyExistingShouldFailWhenFAIL_RESOLUTION() throws IOException {
        testPutWhenAlreadyExisting(MoveHDFS.FAIL_RESOLUTION, MoveHDFS.REL_FAILURE, "randombytes-1");
    }

    @Test
    public void testPutWhenAlreadyExistingShouldIgnoreWhenIGNORE_RESOLUTION() throws IOException {
        testPutWhenAlreadyExisting(MoveHDFS.IGNORE_RESOLUTION, MoveHDFS.REL_SUCCESS, "randombytes-1");
    }

    @Test
    public void testPutWhenAlreadyExistingShouldReplaceWhenREPLACE_RESOLUTION() throws IOException {
        testPutWhenAlreadyExisting(MoveHDFS.REPLACE_RESOLUTION, MoveHDFS.REL_SUCCESS, "randombytes-2");
    }

    private void testPutWhenAlreadyExisting(String conflictResolution, Relationship expectedDestination, String expectedContent) throws IOException {
      // GIVEN
      Files.createDirectories(Paths.get(INPUT_DIRECTORY));
      Files.createDirectories(Paths.get(OUTPUT_DIRECTORY));
      Files.copy(Paths.get(TEST_DATA_DIRECTORY, "randombytes-2"), Paths.get(INPUT_DIRECTORY, "randombytes-1"));
      Files.copy(Paths.get(TEST_DATA_DIRECTORY, "randombytes-1"), Paths.get(OUTPUT_DIRECTORY, "randombytes-1"));

      MoveHDFS processor = new MoveHDFS();

      TestRunner runner = TestRunners.newTestRunner(processor);
      runner.setProperty(MoveHDFS.INPUT_DIRECTORY_OR_FILE, INPUT_DIRECTORY);
      runner.setProperty(MoveHDFS.OUTPUT_DIRECTORY, OUTPUT_DIRECTORY);
      runner.setProperty(MoveHDFS.CONFLICT_RESOLUTION, conflictResolution);

      byte[] expected = Files.readAllBytes(Paths.get(TEST_DATA_DIRECTORY, expectedContent));

      // WHEN
      runner.enqueue(new byte[0]);
      runner.run();

      // THEN
      runner.assertAllFlowFilesTransferred(expectedDestination);

      List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(expectedDestination);
      Assert.assertEquals(1, flowFiles.size());

      byte[] actual = Files.readAllBytes(Paths.get(OUTPUT_DIRECTORY, "randombytes-1"));

      assertArrayEquals(expected, actual);
    }

    private static class TestableMoveHDFS extends MoveHDFS {

        private KerberosProperties testKerberosProperties;

        public TestableMoveHDFS(KerberosProperties testKerberosProperties) {
            this.testKerberosProperties = testKerberosProperties;
        }

        @Override
        protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
            return testKerberosProperties;
        }
    }
}
