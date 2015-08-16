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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestUnpackContent {

    private static final Path dataPath = Paths.get("src/test/resources/TestUnpackContent");

    @Test
    public void testTar() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.TAR_FORMAT);

        runner.enqueue(dataPath.resolve("data.tar"));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testZip() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.ZIP_FORMAT);
        runner.enqueue(dataPath.resolve("data.zip"));

        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testFlowFileStreamV3() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.FLOWFILE_STREAM_FORMAT_V3);
        runner.enqueue(dataPath.resolve("data.flowfilev3"));

        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testFlowFileStreamV2() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.FLOWFILE_STREAM_FORMAT_V2);
        runner.enqueue(dataPath.resolve("data.flowfilev2"));

        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testTarThenMerge() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.TAR_FORMAT);

        unpackRunner.enqueue(dataPath.resolve("data.tar"));
        unpackRunner.run();

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            assertEquals(flowFile.getAttribute(UnpackContent.SEGMENT_ORIGINAL_FILENAME), "data");
        }

        final TestRunner mergeRunner = TestRunners.newTestRunner(new MergeContent());
        mergeRunner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_TAR);
        mergeRunner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        mergeRunner.setProperty(MergeContent.KEEP_PATH, "true");
        mergeRunner.enqueue(unpacked.toArray(new MockFlowFile[0]));
        mergeRunner.run();

        mergeRunner.assertTransferCount(MergeContent.REL_MERGED, 1);
        mergeRunner.assertTransferCount(MergeContent.REL_ORIGINAL, 2);
        mergeRunner.assertTransferCount(MergeContent.REL_FAILURE, 0);

        final List<MockFlowFile> packed = mergeRunner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        for (final MockFlowFile flowFile : packed) {
            flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "data.tar");
        }
    }

    @Test
    public void testZipThenMerge() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.ZIP_FORMAT);

        unpackRunner.enqueue(dataPath.resolve("data.zip"));
        unpackRunner.run();

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            assertEquals(flowFile.getAttribute(UnpackContent.SEGMENT_ORIGINAL_FILENAME), "data");
        }

        final TestRunner mergeRunner = TestRunners.newTestRunner(new MergeContent());
        mergeRunner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_ZIP);
        mergeRunner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        mergeRunner.setProperty(MergeContent.KEEP_PATH, "true");
        mergeRunner.enqueue(unpacked.toArray(new MockFlowFile[0]));
        mergeRunner.run();

        mergeRunner.assertTransferCount(MergeContent.REL_MERGED, 1);
        mergeRunner.assertTransferCount(MergeContent.REL_ORIGINAL, 2);
        mergeRunner.assertTransferCount(MergeContent.REL_FAILURE, 0);

        final List<MockFlowFile> packed = mergeRunner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        for (final MockFlowFile flowFile : packed) {
            flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "data.zip");
        }
    }

    @Test
    public void testZipHandlesBadData() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.ZIP_FORMAT);

        unpackRunner.enqueue(dataPath.resolve("data.tar"));
        unpackRunner.run();

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 1);
    }

    @Test
    public void testTarHandlesBadData() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.TAR_FORMAT);

        unpackRunner.enqueue(dataPath.resolve("data.zip"));
        unpackRunner.run();

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 1);
    }
}
