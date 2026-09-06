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

import net.lingala.zip4j.io.outputstream.ZipOutputStream;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.EncryptionMethod;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;

import static org.apache.nifi.processors.standard.SplitContent.FRAGMENT_COUNT;
import static org.apache.nifi.processors.standard.SplitContent.FRAGMENT_ID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUnpackContent {

    private static final String FIRST_FRAGMENT_INDEX = "1";

    private static final String EXISTING_FRAGMENT_ID = "existing-fragment-id";

    private static final String EXISTING_SEGMENT_FILENAME = "original-archive";

    private static final Path dataPath = Paths.get("src/test/resources/TestUnpackContent");

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");

    private final TestRunner runner = TestRunners.newTestRunner(new UnpackContent());
    private final TestRunner autoUnpackRunner = TestRunners.newTestRunner(new UnpackContent());

    @Test
    public void testTar() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.TAR_FORMAT);
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT);
        runner.enqueue(dataPath.resolve("data.tar"));
        runner.enqueue(dataPath.resolve("data.tar"));
        Map<String, String> attributes = new HashMap<>(1);
        Map<String, String> attributes2 = new HashMap<>(1);
        attributes.put("mime.type", UnpackContent.PackageFormat.TAR_FORMAT.getMimeType());
        attributes2.put("mime.type", "application/tar");
        autoUnpackRunner.enqueue(dataPath.resolve("data.tar"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("data.tar"), attributes2);
        runner.run(2);
        autoUnpackRunner.run(2);

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);

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
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.TAR_FORMAT);
        runner.setProperty(UnpackContent.FILE_FILTER, "^folder/date.txt$");
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT);
        autoUnpackRunner.setProperty(UnpackContent.FILE_FILTER, "^folder/cal.txt$");
        runner.enqueue(dataPath.resolve("data.tar"));
        runner.enqueue(dataPath.resolve("data.tar"));
        Map<String, String> attributes = new HashMap<>(1);
        Map<String, String> attributes2 = new HashMap<>(1);
        attributes.put("mime.type", "application/x-tar");
        attributes2.put("mime.type", "application/tar");
        autoUnpackRunner.enqueue(dataPath.resolve("data.tar"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("data.tar"), attributes2);
        runner.run(2);
        autoUnpackRunner.run(2);

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "1");
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "1");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
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
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "true"); //just forces this to be exercised
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT);
        runner.enqueue(dataPath.resolve("data.zip"));
        runner.enqueue(dataPath.resolve("data.zip"));
        Map<String, String> attributes = new HashMap<>(1);
        attributes.put("mime.type", "application/zip");
        autoUnpackRunner.enqueue(dataPath.resolve("data.zip"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("data.zip"), attributes);
        runner.run(2);
        autoUnpackRunner.run(2);

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);

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
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT);
        runner.enqueue(dataPath.resolve("invalid_data.zip"));
        runner.enqueue(dataPath.resolve("invalid_data.zip"));
        Map<String, String> attributes = new HashMap<>(1);
        attributes.put("mime.type", "application/zip");
        autoUnpackRunner.enqueue(dataPath.resolve("invalid_data.zip"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("invalid_data.zip"), attributes);
        runner.run(2);
        autoUnpackRunner.run(2);

        runner.assertTransferCount(UnpackContent.REL_FAILURE, 2);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 2);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_FAILURE);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final Path path = dataPath.resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testZipInvalidCrcRoutesToFailure() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);

        final byte[] zipBytes = createStoredZip(true, Map.entry("corrupt.txt", "payload-for-crc-mismatch"));
        runner.enqueue(zipBytes);
        runner.run();

        assertOriginalRoutedToFailureOnly(zipBytes);
        assertCrcMismatchLoggedWithoutStackTrace();
    }

    @Test
    public void testZipStoredCrcValidRoutesToSuccess() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);

        final byte[] payload = "payload-for-crc-match".getBytes(StandardCharsets.UTF_8);
        runner.enqueue(createStoredZip(false, Map.entry("ok.txt", "payload-for-crc-match")));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);
        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 1);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS).getFirst().assertContentEquals(payload);
    }

    @Test
    public void testZipDeflatedInvalidCrcRoutesToFailure() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);

        final byte[] zipBytes = createDeflatedZip(true, Map.entry("corrupt.txt", "deflated-payload-for-crc-mismatch"));
        runner.enqueue(zipBytes);
        runner.run();

        assertOriginalRoutedToFailureOnly(zipBytes);
        assertCrcMismatchLoggedWithoutStackTrace();
    }

    @Test
    public void testZipDeflatedCrcValidRoutesToSuccess() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);

        final String contents = "deflated-payload-for-crc-match";
        runner.enqueue(createDeflatedZip(false, Map.entry("ok.txt", contents)));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);
        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 1);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS).getFirst()
                .assertContentEquals(contents.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testZipDeflatedInvalidCrcOnSecondEntryRoutesToFailure() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);

        final byte[] zipBytes = createDeflatedZip(true,
                Map.entry("first.txt", "first-entry-valid-crc"),
                Map.entry("second.txt", "second-entry-invalid-crc"));
        runner.enqueue(zipBytes);
        runner.run();

        assertOriginalRoutedToFailureOnly(zipBytes);
        assertCrcMismatchLoggedWithoutStackTrace();
    }

    @Test
    public void testZipStoredInvalidCrcOnSecondEntryRoutesToFailure() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);

        final byte[] zipBytes = createStoredZip(true,
                Map.entry("first.txt", "first-stored-valid-crc"),
                Map.entry("second.txt", "second-stored-invalid-crc"));
        runner.enqueue(zipBytes);
        runner.run();

        assertOriginalRoutedToFailureOnly(zipBytes);
        assertCrcMismatchLoggedWithoutStackTrace();
    }

    @Test
    public void testZipFileFilterExtractsMatchingEntryWhenCrcIsValid() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.FILE_FILTER, "^keep.txt$");

        runner.enqueue(createDeflatedZip(false,
                Map.entry("keep.txt", "keep-me"),
                Map.entry("skip.txt", "skip-me")));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);
        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 1);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS).getFirst()
                .assertContentEquals("keep-me".getBytes(StandardCharsets.UTF_8));
        runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS).getFirst()
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "keep.txt");
    }

    @Test
    public void testZipFileFilterStillFailsWhenSkippedEntryHasInvalidCrc() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.FILE_FILTER, "^keep.txt$");

        final byte[] zipBytes = createDeflatedZip(true,
                Map.entry("keep.txt", "keep-me"),
                Map.entry("skip.txt", "corrupt-skipped-entry"));
        runner.enqueue(zipBytes);
        runner.run();

        assertOriginalRoutedToFailureOnly(zipBytes);
        assertCrcMismatchLoggedWithoutStackTrace();
    }

    @Test
    public void testZipFileFilterFailsWhenOnlySkippedEntryHasInvalidCrc() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.FILE_FILTER, "^keep.txt$");

        final byte[] zipBytes = createStoredZip(true, Map.entry("skip.txt", "not-extracted-and-corrupt"));
        runner.enqueue(zipBytes);
        runner.run();

        assertOriginalRoutedToFailureOnly(zipBytes);
        assertCrcMismatchLoggedWithoutStackTrace();
    }

    @Test
    public void testZipDirectoryEntryAndFileWithValidCrcRoutesToSuccess() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);

        runner.enqueue(createDeflatedZip(false,
                Map.entry("folder/", ""),
                Map.entry("folder/file.txt", "nested-file")));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);
        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 1);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS).getFirst()
                .assertContentEquals("nested-file".getBytes(StandardCharsets.UTF_8));
        runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS).getFirst()
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "file.txt");
    }

    @Test
    public void testZipEncodingField() {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "invalid-encoding");
        runner.assertNotValid();
        runner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "IBM437");
        runner.assertValid();
        runner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "Cp437");
        runner.assertValid();
        runner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, StandardCharsets.ISO_8859_1.name());
        runner.assertValid();
        runner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, StandardCharsets.UTF_8.name());
        runner.assertValid();

    }
    @Test
    public void testZipWithCp437Encoding() throws IOException {
        String zipFilename = "windows-with-cp437.zip";
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "Cp437");
        runner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "true"); // just forces this to be exercised

        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT);
        autoUnpackRunner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "Cp437");

        runner.enqueue(dataPath.resolve(zipFilename));
        runner.enqueue(dataPath.resolve(zipFilename));

        Map<String, String> attributes = new HashMap<>(1);
        attributes.put("mime.type", "application/zip");
        autoUnpackRunner.enqueue(dataPath.resolve(zipFilename), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve(zipFilename), attributes);
        runner.run(2);
        autoUnpackRunner.run(2);

        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked =
            runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
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
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        autoUnpackRunner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");
        autoUnpackRunner.setProperty(UnpackContent.ZIP_FILENAME_CHARSET, "Cp437");
        final String password = String.class.getSimpleName();
        autoUnpackRunner.setProperty(UnpackContent.PASSWORD, password);

        final char[] streamPassword = password.toCharArray();
        final String contents = TestRunner.class.getCanonicalName();
        String specialChar = "\u00E4";
        String pathInZip = "path_with_special_%s_char/".formatted(specialChar);
        String filename = "filename_with_special_char%s.txt".formatted(specialChar);
        final byte[] zipEncrypted = createZipEncryptedCp437(EncryptionMethod.AES, streamPassword, contents, pathInZip.concat(filename));
        autoUnpackRunner.enqueue(zipEncrypted);
        autoUnpackRunner.run();

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 1);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);

        final List<MockFlowFile> unpacked =
            autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String outputFilename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            assertTrue(StringUtils.containsNone(outputFilename, "?"), "filename contains '?': " + outputFilename);
            assertTrue(Strings.CS.contains(outputFilename, specialChar), "filename missing '%s': %s".formatted(specialChar, outputFilename));
            final String path = flowFile.getAttribute(CoreAttributes.PATH.key());
            assertTrue(StringUtils.containsNone(path, "?"), "path contains '?': " + path);
            assertTrue(Strings.CS.contains(path, specialChar), "path missing '%s': %s".formatted(specialChar, path));
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
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);

        final String password = String.class.getSimpleName();
        final char[] streamPassword = password.toCharArray();
        final String contents = TestRunner.class.getCanonicalName();

        final byte[] zipEncrypted = createZipEncrypted(EncryptionMethod.AES, streamPassword, contents);
        autoUnpackRunner.enqueue(zipEncrypted);
        autoUnpackRunner.run();

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 1);
    }

    @Test
    public void testZipWithFilter() throws IOException {
        runner.setProperty(UnpackContent.FILE_FILTER, "^folder/date.txt$");
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");

        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT);
        autoUnpackRunner.setProperty(UnpackContent.FILE_FILTER, "^folder/cal.txt$");
        runner.enqueue(dataPath.resolve("data.zip"));
        runner.enqueue(dataPath.resolve("data.zip"));
        Map<String, String> attributes = new HashMap<>(1);
        attributes.put("mime.type", "application/zip");
        autoUnpackRunner.enqueue(dataPath.resolve("data.zip"), attributes);
        autoUnpackRunner.enqueue(dataPath.resolve("data.zip"), attributes);
        runner.run(2);
        autoUnpackRunner.run(2);

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "1");
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "1");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
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
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.FLOWFILE_STREAM_FORMAT_V3);
        autoUnpackRunner.enqueue(dataPath.resolve("data.flowfilev3"));
        autoUnpackRunner.enqueue(dataPath.resolve("data.flowfilev3"));

        autoUnpackRunner.run(2);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
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
        autoUnpackRunner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.FLOWFILE_STREAM_FORMAT_V2);
        autoUnpackRunner.enqueue(dataPath.resolve("data.flowfilev2"));
        autoUnpackRunner.enqueue(dataPath.resolve("data.flowfilev2"));

        autoUnpackRunner.run(2);

        autoUnpackRunner.assertTransferCount(UnpackContent.REL_SUCCESS, 4);
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_ORIGINAL, 2);
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).get(1).assertAttributeEquals(FRAGMENT_COUNT, "2");
        autoUnpackRunner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = autoUnpackRunner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final String folder = flowFile.getAttribute(CoreAttributes.PATH.key());
            final Path path = dataPath.resolve(folder).resolve(filename);
            assertTrue(Files.exists(path));

            flowFile.assertContentEquals(path.toFile());
        }
    }

    @Test
    public void testFlowFileStreamAssignsFragmentAttributesWhenAbsent() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.FLOWFILE_STREAM_FORMAT_V3);
        runner.enqueue(dataPath.resolve("data.flowfilev3"));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        final String fragmentId = unpacked.getFirst().getAttribute(UnpackContent.FRAGMENT_ID);
        assertNotNull(fragmentId);
        for (final MockFlowFile flowFile : unpacked) {
            flowFile.assertAttributeEquals(UnpackContent.FRAGMENT_ID, fragmentId);
            flowFile.assertAttributeEquals(UnpackContent.FRAGMENT_COUNT, "2");
            flowFile.assertAttributeEquals(UnpackContent.SEGMENT_ORIGINAL_FILENAME, "data.flowfilev3");
        }

        unpacked.get(0).assertAttributeEquals(UnpackContent.FRAGMENT_INDEX, FIRST_FRAGMENT_INDEX);
        unpacked.get(1).assertAttributeEquals(UnpackContent.FRAGMENT_INDEX, "2");
    }

    @Test
    public void testFlowFileStreamPreservesExistingFragmentAttributes() {
        final TestRunner mergeRunner = TestRunners.newTestRunner(new MergeContent());
        mergeRunner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MergeFormat.FLOWFILE_STREAM_V3);
        mergeRunner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MergeStrategy.BIN_PACK);
        mergeRunner.setProperty(MergeContent.MIN_ENTRIES, "2");
        mergeRunner.setProperty(MergeContent.MAX_ENTRIES, "2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(UnpackContent.FRAGMENT_ID, EXISTING_FRAGMENT_ID);
        attributes.put(UnpackContent.FRAGMENT_COUNT, "2");
        attributes.put(UnpackContent.SEGMENT_ORIGINAL_FILENAME, EXISTING_SEGMENT_FILENAME);
        attributes.put(UnpackContent.FRAGMENT_INDEX, FIRST_FRAGMENT_INDEX);
        mergeRunner.enqueue("Hello ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(UnpackContent.FRAGMENT_INDEX, "2");
        mergeRunner.enqueue("World".getBytes(StandardCharsets.UTF_8), attributes);
        mergeRunner.run();

        mergeRunner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile packaged = mergeRunner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();

        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.FLOWFILE_STREAM_FORMAT_V3);
        runner.enqueue(packaged);
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            flowFile.assertAttributeEquals(UnpackContent.FRAGMENT_ID, EXISTING_FRAGMENT_ID);
            flowFile.assertAttributeEquals(UnpackContent.FRAGMENT_COUNT, "2");
            flowFile.assertAttributeEquals(UnpackContent.SEGMENT_ORIGINAL_FILENAME, EXISTING_SEGMENT_FILENAME);
        }

        unpacked.get(0).assertAttributeEquals(UnpackContent.FRAGMENT_INDEX, FIRST_FRAGMENT_INDEX);
        unpacked.get(1).assertAttributeEquals(UnpackContent.FRAGMENT_INDEX, "2");
    }

    @Test
    public void testTarThenMerge() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.TAR_FORMAT);

        runner.enqueue(dataPath.resolve("data.tar"));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).getFirst().assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            assertEquals("data", flowFile.getAttribute(UnpackContent.SEGMENT_ORIGINAL_FILENAME));
        }

        final TestRunner mergeRunner = TestRunners.newTestRunner(new MergeContent());
        mergeRunner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MergeFormat.TAR);
        mergeRunner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MergeStrategy.DEFRAGMENT);
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
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");

        runner.enqueue(dataPath.resolve("data.zip"));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 2);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 1);
        final MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(UnpackContent.REL_ORIGINAL).getFirst();
        originalFlowFile.assertAttributeExists(FRAGMENT_ID);
        originalFlowFile.assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 0);

        final List<MockFlowFile> unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS);
        for (final MockFlowFile flowFile : unpacked) {
            assertEquals("data", flowFile.getAttribute(UnpackContent.SEGMENT_ORIGINAL_FILENAME));
        }

        final TestRunner mergeRunner = TestRunners.newTestRunner(new MergeContent());
        mergeRunner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MergeFormat.ZIP);
        mergeRunner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MergeStrategy.DEFRAGMENT);
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
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
        runner.setProperty(UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR, "false");

        runner.enqueue(dataPath.resolve("data.tar"));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 1);
    }

    @Test
    public void testTarHandlesBadData() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.TAR_FORMAT);

        runner.enqueue(dataPath.resolve("data.zip"));
        runner.run();

        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 1);
    }

    /*
     * This test checks for thread safety problems when PackageFormat.AUTO_DETECT_FORMAT is used.
     * It won't always fail if there is a issue with the code, but it will fail often enough to eventually be noticed.
     * If this test fails at all, then it needs to be investigated.
     */
    @Test
    public void testThreadSafetyUsingAutoDetect() throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.AUTO_DETECT_FORMAT);

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

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.of(
                "allow-stored-entries-wdd", UnpackContent.ALLOW_STORED_ENTRIES_WITH_DATA_DESCRIPTOR.getName()
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private void runZipEncryptionMethod(final EncryptionMethod encryptionMethod) throws IOException {
        runner.setProperty(UnpackContent.PACKAGING_FORMAT, UnpackContent.PackageFormat.ZIP_FORMAT);
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

        final MockFlowFile unpacked = runner.getFlowFilesForRelationship(UnpackContent.REL_SUCCESS).getFirst();
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

    private void assertOriginalRoutedToFailureOnly(final byte[] originalZip) throws IOException {
        runner.assertTransferCount(UnpackContent.REL_FAILURE, 1);
        runner.assertTransferCount(UnpackContent.REL_SUCCESS, 0);
        runner.assertTransferCount(UnpackContent.REL_ORIGINAL, 0);
        runner.getFlowFilesForRelationship(UnpackContent.REL_FAILURE).getFirst().assertContentEquals(originalZip);
    }

    private void assertCrcMismatchLoggedWithoutStackTrace() {
        final List<LogMessage> errors = runner.getLogger().getErrorMessages();
        assertFalse(errors.isEmpty(), "Expected an error log for CRC mismatch");
        final LogMessage error = errors.getFirst();
        final String details = error.getMsg() + Arrays.toString(error.getArgs());
        assertTrue(details.contains("CRC mismatch"), "Error log should describe the CRC mismatch: " + details);
        assertNull(error.getThrowable(), "CRC mismatch should be logged without a stack trace");
    }

    /**
     * Builds a STORED zip in memory. When {@code invertLastEntryCrc} is true, the CRC-32 of the last
     * entry in the local file header and central directory is bitwise-inverted so the archive is
     * well-formed but the checksum no longer matches the entry bytes.
     */
    @SafeVarargs
    private static byte[] createStoredZip(final boolean invertLastEntryCrc, final Map.Entry<String, String>... entries) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (java.util.zip.ZipOutputStream zipOutputStream = new java.util.zip.ZipOutputStream(outputStream)) {
            zipOutputStream.setMethod(java.util.zip.ZipOutputStream.STORED);
            for (final Map.Entry<String, String> entry : entries) {
                final byte[] payload = entry.getValue().getBytes(StandardCharsets.UTF_8);
                final CRC32 crc32 = new CRC32();
                crc32.update(payload);
                final ZipEntry zipEntry = new ZipEntry(entry.getKey());
                zipEntry.setMethod(ZipEntry.STORED);
                zipEntry.setCrc(crc32.getValue());
                zipEntry.setSize(payload.length);
                zipEntry.setCompressedSize(payload.length);
                zipOutputStream.putNextEntry(zipEntry);
                zipOutputStream.write(payload);
                zipOutputStream.closeEntry();
            }
        }

        final byte[] zipBytes = outputStream.toByteArray();
        if (!invertLastEntryCrc) {
            return zipBytes;
        }

        final byte[] localFileHeaderSignature = {0x50, 0x4b, 0x03, 0x04};
        final byte[] centralDirectorySignature = {0x50, 0x4b, 0x01, 0x02};
        final int centralDirectoryIndex = indexOf(zipBytes, centralDirectorySignature);
        assertTrue(centralDirectoryIndex > 0, "ZIP central directory signature not found");
        corruptCrcAt(zipBytes, lastIndexOf(zipBytes, localFileHeaderSignature, centralDirectoryIndex) + 14);
        corruptCrcAt(zipBytes, lastIndexOf(zipBytes, centralDirectorySignature, zipBytes.length) + 16);
        return zipBytes;
    }

    /**
     * Builds a DEFLATED zip (Java default: data descriptor after the compressed bytes). When
     * {@code invertLastEntryCrc} is true, the CRC in that last entry's data descriptor and central
     * directory is inverted.
     */
    @SafeVarargs
    private static byte[] createDeflatedZip(final boolean invertLastEntryCrc, final Map.Entry<String, String>... entries) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (java.util.zip.ZipOutputStream zipOutputStream = new java.util.zip.ZipOutputStream(outputStream)) {
            for (final Map.Entry<String, String> entry : entries) {
                final ZipEntry zipEntry = new ZipEntry(entry.getKey());
                zipEntry.setMethod(ZipEntry.DEFLATED);
                zipOutputStream.putNextEntry(zipEntry);
                zipOutputStream.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                zipOutputStream.closeEntry();
            }
        }

        final byte[] zipBytes = outputStream.toByteArray();
        if (!invertLastEntryCrc) {
            return zipBytes;
        }

        final byte[] centralDirectorySignature = {0x50, 0x4b, 0x01, 0x02};
        final byte[] dataDescriptorSignature = {0x50, 0x4b, 0x07, 0x08};
        final int centralDirectoryIndex = indexOf(zipBytes, centralDirectorySignature);
        assertTrue(centralDirectoryIndex > 0, "ZIP central directory signature not found");
        final int dataDescriptorIndex = lastIndexOf(zipBytes, dataDescriptorSignature, centralDirectoryIndex);
        assertTrue(dataDescriptorIndex >= 0, "ZIP data descriptor signature not found");
        corruptCrcAt(zipBytes, dataDescriptorIndex + 4);
        corruptCrcAt(zipBytes, lastIndexOf(zipBytes, centralDirectorySignature, zipBytes.length) + 16);
        return zipBytes;
    }

    private static void corruptCrcAt(final byte[] zipBytes, final int crcIndex) {
        final int storedCrc = ByteBuffer.wrap(zipBytes, crcIndex, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        ByteBuffer.wrap(zipBytes, crcIndex, 4).order(ByteOrder.LITTLE_ENDIAN).putInt(storedCrc ^ 0xffffffff);
    }

    private static int indexOf(final byte[] haystack, final byte[] needle) {
        return indexOf(haystack, needle, 0, haystack.length);
    }

    private static int lastIndexOf(final byte[] haystack, final byte[] needle, final int endExclusive) {
        outer:
        for (int i = endExclusive - needle.length; i >= 0; i--) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static int indexOf(final byte[] haystack, final byte[] needle, final int start, final int endExclusive) {
        outer:
        for (int i = start; i <= endExclusive - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }
}
