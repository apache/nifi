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

import static org.apache.nifi.processors.standard.SplitContent.FRAGMENT_COUNT;
import static org.apache.nifi.processors.standard.SplitContent.FRAGMENT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestUnpackContent {

    private static final Path dataPath = Paths.get("src/test/resources/TestUnpackContent");

    @Test
    public void testTar() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        final TestRunner autoUnpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.TAR_FORMAT.toString());
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT.toString());
        unpackRunner.enqueue(dataPath.resolve("data.tar"));
        unpackRunner.enqueue(dataPath.resolve("data.tar"));
        Map<String, String> attributes = new HashMap<>(1);
        Map<String, String> attributes2 = new HashMap<>(1);
        attributes.put("mime.type", UnpackContent.PackageFormat.TAR_FORMAT.getMimeType());
        attributes2.put("mime.type", UnpackContent.PackageFormat.X_TAR_FORMAT.getMimeType());
        autoUnpackRunner.enqueue(dataPath.resolve("data.tar"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("data.tar"), attributes2);
        unpackRunner.run(2);
        autoUnpackRunner.run(2);

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testTarWithFilter() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        final TestRunner autoUnpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.TAR_FORMAT.toString());
        unpackRunner.setProperty(UnpackContent.FILE_FILTER, "^folder/date.txt$");
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT.toString());
        autoUnpackRunner.setProperty(UnpackContent.FILE_FILTER, "^folder/cal.txt$");
        unpackRunner.enqueue(dataPath.resolve("data.tar"));
        unpackRunner.enqueue(dataPath.resolve("data.tar"));
        Map<String, String> attributes = new HashMap<>(1);
        Map<String, String> attributes2 = new HashMap<>(1);
        attributes.put("mime.type", "application/x-tar");
        attributes2.put("mime.type", "application/tar");
        autoUnpackRunner.enqueue(dataPath.resolve("data.tar"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("data.tar"), attributes2);
        unpackRunner.run(2);
        autoUnpackRunner.run(2);

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "1");
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "1");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        List<MockFlowFile> unpacked = unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));
            assertEquals("date.txt", filename);
            flowFile.assertContentEquals(path.toFile());
        }
        unpacked = autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));
            assertEquals("cal.txt", filename);
            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testZip() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        final TestRunner autoUnpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT.toString());
        unpackRunner.enqueue(dataPath.resolve("data.zip"));
        unpackRunner.enqueue(dataPath.resolve("data.zip"));
        Map<String, String> attributes = new HashMap<>(1);
        attributes.put("mime.type", "application/zip");
        autoUnpackRunner.enqueue(dataPath.resolve("data.zip"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("data.zip"), attributes);
        unpackRunner.run(2);
        autoUnpackRunner.run(2);

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }
    @Test
    public void testInvalidZip() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        final TestRunner autoUnpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT.toString());
        unpackRunner.enqueue(dataPath.resolve("invalid_data.zip"));
        unpackRunner.enqueue(dataPath.resolve("invalid_data.zip"));
        Map<String, String> attributes = new HashMap<>(1);
        attributes.put("mime.type", "application/zip");
        autoUnpackRunner.enqueue(dataPath.resolve("invalid_data.zip"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("invalid_data.zip"), attributes);
        unpackRunner.run(2);
        autoUnpackRunner.run(2);

        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 2);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 2);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);

        final List<MockFlowFile> unpacked = unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_FAILURE);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
           // final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testZipWithFilter() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        final TestRunner autoUnpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.FILE_FILTER, "^folder/date.txt$");
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT.toString());
        autoUnpackRunner.setProperty(UnpackContent.FILE_FILTER, "^folder/cal.txt$");
        unpackRunner.enqueue(dataPath.resolve("data.zip"));
        unpackRunner.enqueue(dataPath.resolve("data.zip"));
        Map<String, String> attributes = new HashMap<>(1);
        attributes.put("mime.type", "application/zip");
        autoUnpackRunner.enqueue(dataPath.resolve("data.zip"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("data.zip"), attributes);
        unpackRunner.run(2);
        autoUnpackRunner.run(2);

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "1");
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "1");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        List<MockFlowFile> unpacked = unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));
            assertEquals("date.txt", filename);
            flowFile.assertContentEquals(path.toFile());
        }
        unpacked = autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));
            assertEquals("cal.txt", filename);
            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testFlowFileStreamV3() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.FLOWFILE_STREAM_FORMAT_V3.toString());
        runner.enqueue(dataPath.resolve("data.flowfilev3"));
        runner.enqueue(dataPath.resolve("data.flowfilev3"));

        runner.run(2);

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
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
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.FLOWFILE_STREAM_FORMAT_V2.toString());
        runner.enqueue(dataPath.resolve("data.flowfilev2"));
        runner.enqueue(dataPath.resolve("data.flowfilev2"));

        runner.run(2);

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
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
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.TAR_FORMAT.toString());

        unpackRunner.enqueue(dataPath.resolve("data.tar"));
        unpackRunner.run();

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
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
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());

        unpackRunner.enqueue(dataPath.resolve("data.zip"));
        unpackRunner.run();

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        final MockFlowFile originalFlowFile = unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0);
        originalFlowFile.assertAttributeExists(FRAGMENT_ID);
        originalFlowFile.assertAttributeEquals(FRAGMENT_COUNT, "2");
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
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());

        unpackRunner.enqueue(dataPath.resolve("data.tar"));
        unpackRunner.run();

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 1);
    }

    @Test
    public void testTarHandlesBadData() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.TAR_FORMAT.toString());

        unpackRunner.enqueue(dataPath.resolve("data.zip"));
        unpackRunner.run();

        unpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);
        unpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 1);
    }

    /*
     * This test checks for thread safety problems when PackageFormat.AUTO_DETECT_FORMAT is used.
     * It won't always fail if there is a issue with the code, but it will fail often enough to eventually be noticed.
     * If this test fails at all, then it needs to be investigated.
     */
    @Test
    public void testThreadSafetyUsingAutoDetect() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT.toString());

        Map<String, String> attrsTar = new HashMap<>(1);
        Map<String, String> attrsFFv3 = new HashMap<>(1);
        attrsTar.put("mime.type", UnpackContent.PackageFormat.TAR_FORMAT.getMimeType());
        attrsFFv3.put("mime.type", UnpackContent.PackageFormat.FLOWFILE_STREAM_FORMAT_V3.getMimeType());

        int numThreads = 50;
        runner.setThreadCount(numThreads);

        for (int i=0; i<numThreads; i++) {
            if (i%2 == 0) {
                runner.enqueue(dataPath.resolve("data.tar"), attrsTar);
            } else {
                runner.enqueue(dataPath.resolve("data.flowfilev3"), attrsFFv3);
            }
        }

        runner.run(numThreads);

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, numThreads*2);
    }
}
