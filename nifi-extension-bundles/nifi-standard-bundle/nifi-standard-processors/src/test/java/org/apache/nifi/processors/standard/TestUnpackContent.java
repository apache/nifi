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

import java.nio.charset.StandardCharsets;
import net.lingala.zip4j.io.outputstream.ZipOutputStream;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.EncryptionMethod;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.processors.standard.SplitContent.FRAGMENT_COUNT;
import static org.apache.nifi.processors.standard.SplitContent.FRAGMENT_ID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUnpackContent {

    private static final String FIRST_FRAGMENT_INDEX = "1";

    private static final Path dataPath = Paths.get("src/test/resources/TestUnpackContent");

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");

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
            assertTrue(flowFile.getAttributes().keySet().containsAll(List.of(UnpackContent.FRAGMENT_ID, UnpackContent.FRAGMENT_INDEX,
                    UnpackContent.FRAGMENT_COUNT, UnpackContent.SEGMENT_ORIGINAL_FILENAME, UnpackContent.FILE_SIZE_ATTRIBUTE)));

            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);

            assertEquals("rw-r--r--", flowFile.getAttribute(UnpackContent.FILE_PERMISSIONS_ATTRIBUTE));
            assertEquals("jmcarey", flowFile.getAttribute(UnpackContent.FILE_OWNER_ATTRIBUTE));
            assertEquals("mkpasswd", flowFile.getAttribute(UnpackContent.FILE_GROUP_ATTRIBUTE));

            String modifiedTimeAsString = flowFile.getAttribute("file.lastModifiedTime");
            assertDoesNotThrow(() -> TIMESTAMP_FORMATTER.parse(modifiedTimeAsString));
            String creationTimeAsString = flowFile.getAttribute("file.creationTime");
            assertDoesNotThrow(() -> TIMESTAMP_FORMATTER.parse(creationTimeAsString));

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
        unpackRunner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "true"); //just forces this to be exercised
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

        final List<String> expectedAttributeNames = List.of(
                CoreAttributes.FILENAME.key(),
                CoreAttributes.PATH.key(),
                UnpackContent.FRAGMENT_ID,
                UnpackContent.FRAGMENT_INDEX,
                UnpackContent.FRAGMENT_COUNT,
                UnpackContent.SEGMENT_ORIGINAL_FILENAME,
                UnpackContent.FILE_SIZE_ATTRIBUTE,
                UnpackContent.FILE_CREATION_TIME_ATTRIBUTE,
                UnpackContent.FILE_LAST_MODIFIED_TIME_ATTRIBUTE,
                UnpackContent.FILE_PERMISSIONS_ATTRIBUTE
        );

        for (final MockFlowFile flowFile : unpacked) {
            for (final String expectedAttributeName : expectedAttributeNames) {
                flowFile.assertAttributeExists(expectedAttributeName);
            }
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
        unpackRunner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");
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
            final Path path = dataPath.resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }
    @Test
    public void testZipEncodingField() {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());
        unpackRunner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "invalid-encoding");
        unpackRunner.assertNotValid();
        unpackRunner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "IBM437");
        unpackRunner.assertValid();
        unpackRunner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "Cp437");
        unpackRunner.assertValid();
        unpackRunner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, StandardCharsets.ISO_8859_1.name());
        unpackRunner.assertValid();
        unpackRunner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, StandardCharsets.UTF_8.name());
        unpackRunner.assertValid();

    }
    @Test
    public void testZipWithCp437Encoding() throws IOException {
        String zipFilename = "windows-with-cp437.zip";
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        final TestRunner autoUnpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());
        unpackRunner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "Cp437");
        unpackRunner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "true"); // just forces this to be exercised

        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT.toString());
        autoUnpackRunner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "Cp437");

        unpackRunner.enqueue(dataPath.resolve(zipFilename));
        unpackRunner.enqueue(dataPath.resolve(zipFilename));

        Map<String, String> attributes = new HashMap<>(1);
        attributes.put("mime.type", "application/zip");
        autoUnpackRunner.enqueue(dataPath.resolve(zipFilename), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve(zipFilename), attributes);
        unpackRunner.run(2);
        autoUnpackRunner.run(2);

        unpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked =
            unpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            // In this test case only check for presence of `?` in filename and path for failure, since the zip was created on Windows,
            // it will always output `?` if Cp437 encoding is not used during unpacking. The zip file also contains file and folder
            // without special characters.
            // As a result of these conditions, this test does not check for valid special character presence.
            assertTrue(StringUtils.containsNone(filename, "?"), "filename contains '?': " + filename);
            final String path = flowFile.getAttribute(CoreAttributes.PATH.key());
            assertTrue(StringUtils.containsNone(path, "?"), "path contains '?': " + path);
        }
    }
    @Test
    public void testEncryptedZipWithCp437Encoding() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());
        runner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");
        runner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "Cp437");
        final String password = String.class.getSimpleName();
        runner.setProperty(UnpackContent.PASSWORD, password);

        final char[] streamPassword = password.toCharArray();
        final String contents = TestRunner.class.getCanonicalName();
        String specialChar = "\u00E4";
        String pathInZip = "path_with_special_%s_char/".formatted(specialChar);
        String filename = "filename_with_special_char%s.txt".formatted(specialChar);
        final byte[] zipEncrypted = createZipEncryptedCp437(EncryptionMethod.AES, streamPassword, contents, pathInZip.concat(filename));
        runner.enqueue(zipEncrypted);
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 1);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);

        final List<MockFlowFile> unpacked =
            runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String outputFilename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            assertTrue(StringUtils.containsNone(outputFilename, "?"), "filename contains '?': " + outputFilename);
            assertTrue(StringUtils.contains(outputFilename, specialChar), "filename missing '%s': %s".formatted(specialChar, outputFilename));
            final String path = flowFile.getAttribute(CoreAttributes.PATH.key());
            assertTrue(StringUtils.containsNone(path, "?"), "path contains '?': " + path);
            assertTrue(StringUtils.contains(path, specialChar), "path missing '%s': %s".formatted(specialChar, path));
        }
    }

    @Test
    public void testZipEncryptionZipStandard() throws IOException {
        runZipEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
    }

    @Test
    public void testZipEncryptionAes() throws IOException {
        runZipEncryptionMethod(EncryptionMethod.AES);
    }

    @Test
    public void testZipEncryptionNoPasswordConfigured() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());

        final String password = String.class.getSimpleName();
        final char[] streamPassword = password.toCharArray();
        final String contents = TestRunner.class.getCanonicalName();

        final byte[] zipEncrypted = createZipEncrypted(EncryptionMethod.AES, streamPassword, contents);
        runner.enqueue(zipEncrypted);
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_FAILURE, 1);
    }

    @Test
    public void testZipWithFilter() throws IOException {
        final TestRunner unpackRunner = TestRunners.newTestRunner(new UnpackContent());
        final TestRunner autoUnpackRunner = TestRunners.newTestRunner(new UnpackContent());
        unpackRunner.setProperty(UnpackContent.FILE_FILTER, "^folder/date.txt$");
        unpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());
        unpackRunner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");

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
        unpackRunner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");

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
        unpackRunner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");

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

        for (int i = 0; i < numThreads; i++) {
            if (i % 2 == 0) {
                runner.enqueue(dataPath.resolve("data.tar"), attrsTar);
            } else {
                runner.enqueue(dataPath.resolve("data.flowfilev3"), attrsFFv3);
            }
        }

        runner.run(numThreads);

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, numThreads * 2);
    }

    private void runZipEncryptionMethod(final EncryptionMethod encryptionMethod) throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT.toString());
        runner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");
        final String password = String.class.getSimpleName();
        runner.setProperty(UnpackContent.PASSWORD, password);

        final char[] streamPassword = password.toCharArray();
        final String contents = TestRunner.class.getCanonicalName();

        final byte[] zipEncrypted = createZipEncrypted(encryptionMethod, streamPassword, contents);
        runner.enqueue(zipEncrypted);
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 1);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);

        final MockFlowFile unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS).iterator().next();
        unpacked.assertAttributeEquals(UnpackContent.FILE_ENCRYPTION_METHOD_ATTRIBUTE, encryptionMethod.toString());
        unpacked.assertAttributeEquals(UnpackContent.FRAGMENT_INDEX, FIRST_FRAGMENT_INDEX);

        final byte[] unpackedBytes = runner.getContentAsByteArray(unpacked);
        final String unpackedContents = new String(unpackedBytes);
        assertEquals(contents, unpackedContents, "Unpacked Contents not matched");
    }

    private byte[] createZipEncrypted(final EncryptionMethod encryptionMethod, final char[] password, final String contents) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream, password);

        final String name = UUID.randomUUID().toString();

        final ZipParameters zipParameters = new ZipParameters();
        zipParameters.setEncryptionMethod(encryptionMethod);
        zipParameters.setEncryptFiles(true);
        zipParameters.setFileNameInZip(name);
        zipOutputStream.putNextEntry(zipParameters);
        zipOutputStream.write(contents.getBytes());
        zipOutputStream.closeEntry();
        zipOutputStream.close();

        return outputStream.toByteArray();
    }

    private byte[] createZipEncryptedCp437(final EncryptionMethod encryptionMethod, final char[] password, final String contents, String filename) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream, password, Charsets.toCharset("Cp437"));

        final ZipParameters zipParameters = new ZipParameters();
        zipParameters.setEncryptionMethod(encryptionMethod);
        zipParameters.setEncryptFiles(true);
        zipParameters.setFileNameInZip(filename);
        zipOutputStream.putNextEntry(zipParameters);
        zipOutputStream.write(contents.getBytes());
        zipOutputStream.closeEntry();
        zipOutputStream.close();

        return outputStream.toByteArray();
    }
}
