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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestPutFile {

    public static final String TARGET_DIRECTORY = "target/put-file";
    private File targetDir;

    @Before
    public void prepDestDirectory() throws IOException {
        targetDir = new File(TARGET_DIRECTORY);
        if (!targetDir.exists()) {
            Files.createDirectories(targetDir.toPath());
            return;
        }

        targetDir.setReadable(true);

        deleteDirectoryContent(targetDir);
    }

    private void deleteDirectoryContent(File directory) throws IOException {
        for (final File file : directory.listFiles()) {
            if (file.isDirectory()) {
                deleteDirectoryContent(file);
            }
            Files.delete(file.toPath());
        }
    }

    @Test
    public void testCreateDirectory() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        String newDir = targetDir.getAbsolutePath()+"/new-folder";
        runner.setProperty(PutFile.DIRECTORY, newDir);
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        Path targetPath = Paths.get(TARGET_DIRECTORY + "/new-folder/targetFile.txt");
        byte[] content = Files.readAllBytes(targetPath);
        assertEquals("Hello world!!", new String(content));
    }

    @Test
    public void testReplaceConflictResolution() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        Path targetPath = Paths.get(TARGET_DIRECTORY+"/targetFile.txt");
        byte[] content = Files.readAllBytes(targetPath);
        assertEquals("Hello world!!", new String(content));

        //Second file
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Another file".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);
        File dir = new File(TARGET_DIRECTORY);
        assertEquals(1, dir.list().length);
        targetPath = Paths.get(TARGET_DIRECTORY+"/targetFile.txt");
        content = Files.readAllBytes(targetPath);
        assertEquals("Another file", new String(content));
    }

    @Test
    public void testIgnoreConflictResolution() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.IGNORE_RESOLUTION);

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        Path targetPath = Paths.get(TARGET_DIRECTORY+"/targetFile.txt");
        byte[] content = Files.readAllBytes(targetPath);
        assertEquals("Hello world!!", new String(content));

        //Second file
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Another file".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);
        File dir = new File(TARGET_DIRECTORY);
        assertEquals(1, dir.list().length);
        targetPath = Paths.get(TARGET_DIRECTORY+"/targetFile.txt");
        content = Files.readAllBytes(targetPath);
        assertEquals("Hello world!!", new String(content));
    }

    @Test
    public void testFailConflictResolution() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.FAIL_RESOLUTION);

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        Path targetPath = Paths.get(TARGET_DIRECTORY+"/targetFile.txt");
        byte[] content = Files.readAllBytes(targetPath);
        assertEquals("Hello world!!", new String(content));

        //Second file
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Another file".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(PutFile.REL_SUCCESS, 1);
        runner.assertTransferCount(PutFile.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);
    }

    @Test
    public void testMaxFileLimitReach() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);
        runner.setProperty(PutFile.MAX_DESTINATION_FILES, "1");

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        Path targetPath = Paths.get(TARGET_DIRECTORY+"/targetFile.txt");
        byte[] content = Files.readAllBytes(targetPath);
        assertEquals("Hello world!!", new String(content));

        //Second file
        attributes.put(CoreAttributes.FILENAME.key(), "secondFile.txt");
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(PutFile.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);
    }

    @Test
    public void testReplaceAndMaxFileLimitReach() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);
        runner.setProperty(PutFile.MAX_DESTINATION_FILES, "1");

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        Path targetPath = Paths.get(TARGET_DIRECTORY+"/targetFile.txt");
        byte[] content = Files.readAllBytes(targetPath);
        assertEquals("Hello world!!", new String(content));

        //Second file
        attributes.put(CoreAttributes.FILENAME.key(), "targetFile.txt");
        runner.enqueue("Another file".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);
        File dir = new File(TARGET_DIRECTORY);
        assertEquals(1, dir.list().length);
        targetPath = Paths.get(TARGET_DIRECTORY+"/targetFile.txt");
        content = Files.readAllBytes(targetPath);
        assertEquals("Another file", new String(content));
    }

}
