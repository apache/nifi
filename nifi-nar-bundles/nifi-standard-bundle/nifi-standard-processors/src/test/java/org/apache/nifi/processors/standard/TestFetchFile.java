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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestFetchFile {

    @Before
    public void prepDestDirectory() throws IOException {
        final File targetDir = new File("target/move-target");
        if (!targetDir.exists()) {
            Files.createDirectories(targetDir.toPath());
            return;
        }

        targetDir.setReadable(true);

        for (final File file : targetDir.listFiles()) {
            Files.delete(file.toPath());
        }
    }

    @Test
    public void notFound() throws IOException {
        final File sourceFile = new File("notFound");

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_NONE.getValue());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_NOT_FOUND, 1);
    }

    @Test
    public void testSimpleSuccess() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_NONE.getValue());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).get(0).assertContentEquals(content);

        assertTrue(sourceFile.exists());
    }

    @Test
    public void testDeleteOnComplete() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_DELETE.getValue());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).get(0).assertContentEquals(content);

        assertFalse(sourceFile.exists());
    }

    @Test
    public void testMoveOnCompleteWithTargetDirExisting() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, "target/move-target");
        runner.assertValid();

        final File destDir = new File("target/move-target");
        destDir.mkdirs();
        assertTrue(destDir.exists());

        final File destFile = new File(destDir, sourceFile.getName());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).get(0).assertContentEquals(content);

        assertFalse(sourceFile.exists());
        assertTrue(destFile.exists());
    }

    @Test
    public void testMoveOnCompleteWithTargetDirMissing() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, "target/move-target");
        runner.assertValid();

        final File destDir = new File("target/move-target");
        if (destDir.exists()) {
            destDir.delete();
        }
        assertFalse(destDir.exists());

        final File destFile = new File(destDir, sourceFile.getName());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).get(0).assertContentEquals(content);

        assertFalse(sourceFile.exists());
        assertTrue(destFile.exists());
    }

    @Test
    public void testMoveOnCompleteWithTargetExistsButNotWritable() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, "target/move-target");
        runner.assertValid();

        final File destDir = new File("target/move-target");
        if (!destDir.exists()) {
            destDir.mkdirs();
        }
        destDir.setWritable(false);

        assertTrue(destDir.exists());
        assertFalse(destDir.canWrite());

        final File destFile = new File(destDir, sourceFile.getName());

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_FAILURE).get(0).assertContentEquals("");

        assertTrue(sourceFile.exists());
        assertFalse(destFile.exists());
    }

    @Test
    public void testMoveOnCompleteWithParentOfTargetDirNotAccessible() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final String moveTargetParent = "target/fetch-file";
        final String moveTarget = moveTargetParent + "/move-target";

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, moveTarget);
        runner.assertValid();

        // Make the parent of move-target non-writable and non-readable
        final File moveTargetParentDir = new File(moveTargetParent);
        moveTargetParentDir.mkdirs();
        moveTargetParentDir.setReadable(false);
        moveTargetParentDir.setWritable(false);
        try {
            runner.enqueue(new byte[0]);
            runner.run();
            runner.assertAllFlowFilesTransferred(FetchFile.REL_FAILURE, 1);
            runner.getFlowFilesForRelationship(FetchFile.REL_FAILURE).get(0).assertContentEquals("");

            assertTrue(sourceFile.exists());
        } finally {
            moveTargetParentDir.setReadable(true);
            moveTargetParentDir.setWritable(true);
        }
    }

    @Test
    public void testMoveAndReplace() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, "target/move-target");
        runner.setProperty(FetchFile.CONFLICT_STRATEGY, FetchFile.CONFLICT_REPLACE.getValue());
        runner.assertValid();

        final File destDir = new File("target/move-target");
        final File destFile = new File(destDir, sourceFile.getName());
        Files.write(destFile.toPath(), "Good-bye".getBytes(), StandardOpenOption.CREATE);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).get(0).assertContentEquals(content);

        final byte[] replacedContent = Files.readAllBytes(destFile.toPath());
        assertTrue(Arrays.equals(content, replacedContent));
        assertFalse(sourceFile.exists());
        assertTrue(destFile.exists());
    }

    @Test
    public void testMoveAndKeep() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, "target/move-target");
        runner.setProperty(FetchFile.CONFLICT_STRATEGY, FetchFile.CONFLICT_KEEP_INTACT.getValue());
        runner.assertValid();

        final File destDir = new File("target/move-target");
        final File destFile = new File(destDir, sourceFile.getName());

        final byte[] goodBye = "Good-bye".getBytes();
        Files.write(destFile.toPath(), goodBye);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(FetchFile.REL_SUCCESS).get(0).assertContentEquals(content);

        final byte[] replacedContent = Files.readAllBytes(destFile.toPath());
        assertTrue(Arrays.equals(goodBye, replacedContent));
        assertFalse(sourceFile.exists());
        assertTrue(destFile.exists());
    }

    @Test
    public void testMoveAndFail() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, "target/move-target");
        runner.setProperty(FetchFile.CONFLICT_STRATEGY, FetchFile.CONFLICT_FAIL.getValue());
        runner.assertValid();

        final File destDir = new File("target/move-target");
        final File destFile = new File(destDir, sourceFile.getName());

        final byte[] goodBye = "Good-bye".getBytes();
        Files.write(destFile.toPath(), goodBye);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_FAILURE, 1);

        final byte[] replacedContent = Files.readAllBytes(destFile.toPath());
        assertTrue(Arrays.equals(goodBye, replacedContent));
        assertTrue(sourceFile.exists());
        assertTrue(destFile.exists());
    }


    @Test
    public void testMoveAndRename() throws IOException {
        final File sourceFile = new File("target/1.txt");
        final byte[] content = "Hello, World!".getBytes();
        Files.write(sourceFile.toPath(), content, StandardOpenOption.CREATE);

        final TestRunner runner = TestRunners.newTestRunner(new FetchFile());
        runner.setProperty(FetchFile.FILENAME, sourceFile.getAbsolutePath());
        runner.setProperty(FetchFile.COMPLETION_STRATEGY, FetchFile.COMPLETION_MOVE.getValue());
        runner.assertNotValid();
        runner.setProperty(FetchFile.MOVE_DESTINATION_DIR, "target/move-target");
        runner.setProperty(FetchFile.CONFLICT_STRATEGY, FetchFile.CONFLICT_RENAME.getValue());
        runner.assertValid();

        final File destDir = new File("target/move-target");
        final File destFile = new File(destDir, sourceFile.getName());

        final byte[] goodBye = "Good-bye".getBytes();
        Files.write(destFile.toPath(), goodBye);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);

        final byte[] replacedContent = Files.readAllBytes(destFile.toPath());
        assertTrue(Arrays.equals(goodBye, replacedContent));
        assertFalse(sourceFile.exists());
        assertTrue(destFile.exists());

        assertEquals(2, destDir.list().length);
    }
}
