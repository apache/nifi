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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestMergeContent {

    @BeforeClass
    public static void setup() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.standard", "DEBUG");
    }

    @Test
    public void testSimpleBinaryConcat() throws IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);

        createFlowFiles(runner);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        bundle.assertContentEquals("Hello, World!".getBytes("UTF-8"));
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
    }

    @Test
    public void testMimeTypeIsOctetStreamIfConflictingWithBinaryConcat() throws IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);

        createFlowFiles(runner);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/zip");
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 4);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        bundle.assertContentEquals("Hello, World!".getBytes("UTF-8"));
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
    }

    @Test
    public void testOldestBinIsExpired() throws IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 day");
        runner.setProperty(MergeContent.MAX_BIN_COUNT, "50");
        runner.setProperty(MergeContent.MIN_ENTRIES, "10");
        runner.setProperty(MergeContent.MAX_ENTRIES, "10");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.CORRELATION_ATTRIBUTE_NAME, "correlationId");

        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < 49; i++) {
            attrs.put("correlationId", String.valueOf(i));

            for (int j = 0; j < 5; j++) {
                runner.enqueue(new byte[0], attrs);
            }
        }

        runner.run();

        runner.assertQueueNotEmpty(); // sessions rolled back.
        runner.assertTransferCount(MergeContent.REL_MERGED, 0);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 0);

        attrs.remove("correlationId");
        runner.enqueue(new byte[0], attrs);

        runner.clearTransferState();
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 5);
    }

    @Test
    public void testSimpleBinaryConcatWaitsForMin() throws IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.MIN_SIZE, "20 KB");

        createFlowFiles(runner);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 0);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 0);
    }

    @Test
    public void testZip() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_ZIP);

        createFlowFiles(runner);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        try (final InputStream rawIn = new ByteArrayInputStream(runner.getContentAsByteArray(bundle)); final ZipInputStream in = new ZipInputStream(rawIn)) {
            Assert.assertNotNull(in.getNextEntry());
            final byte[] part1 = IOUtils.toByteArray(in);
            Assert.assertTrue(Arrays.equals("Hello".getBytes("UTF-8"), part1));

            in.getNextEntry();
            final byte[] part2 = IOUtils.toByteArray(in);
            Assert.assertTrue(Arrays.equals(", ".getBytes("UTF-8"), part2));

            in.getNextEntry();
            final byte[] part3 = IOUtils.toByteArray(in);
            Assert.assertTrue(Arrays.equals("World!".getBytes("UTF-8"), part3));
        }
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/zip");
    }

    @Test
    public void testTar() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_TAR);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");

        attributes.put(CoreAttributes.FILENAME.key(), "AShortFileName");
        runner.enqueue("Hello".getBytes("UTF-8"), attributes);
        attributes.put(CoreAttributes.FILENAME.key(), "ALongerrrFileName");
        runner.enqueue(", ".getBytes("UTF-8"), attributes);
        attributes.put(CoreAttributes.FILENAME.key(), "AReallyLongggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggFileName");
        runner.enqueue("World!".getBytes("UTF-8"), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        try (final InputStream rawIn = new ByteArrayInputStream(runner.getContentAsByteArray(bundle)); final TarArchiveInputStream in = new TarArchiveInputStream(rawIn)) {
            ArchiveEntry entry = in.getNextEntry();
            Assert.assertNotNull(entry);
            assertEquals("AShortFileName", entry.getName());
            final byte[] part1 = IOUtils.toByteArray(in);
            Assert.assertTrue(Arrays.equals("Hello".getBytes("UTF-8"), part1));

            entry = in.getNextEntry();
            assertEquals("ALongerrrFileName", entry.getName());
            final byte[] part2 = IOUtils.toByteArray(in);
            Assert.assertTrue(Arrays.equals(", ".getBytes("UTF-8"), part2));

            entry = in.getNextEntry();
            assertEquals("AReallyLongggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggFileName", entry.getName());
            final byte[] part3 = IOUtils.toByteArray(in);
            Assert.assertTrue(Arrays.equals("World!".getBytes("UTF-8"), part3));
        }
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/tar");
    }

    @Test
    public void testFlowFileStream() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MIN_ENTRIES, "2");
        runner.setProperty(MergeContent.MAX_ENTRIES, "2");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_FLOWFILE_STREAM_V3);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("path", "folder");
        runner.enqueue(Paths.get("src/test/resources/TestUnpackContent/folder/cal.txt"), attributes);
        runner.enqueue(Paths.get("src/test/resources/TestUnpackContent/folder/date.txt"), attributes);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 2);

        final MockFlowFile merged = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        merged.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/flowfile-v3");
    }

    @Test
    public void testDefragment() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("A Man ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes("UTF-8"), attributes);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes("UTF-8"));
    }

    @Test
    public void testDefragmentWithTooFewFragments() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "2 secs");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "5");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("A Man ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes("UTF-8"), attributes);

        runner.run(1, false);

        while (true) {
            try {
                Thread.sleep(3000L);
                break;
            } catch (final InterruptedException ie) {
            }
        }
        runner.run(1);

        runner.assertTransferCount(MergeContent.REL_FAILURE, 4);
    }

    @Test
    public void testDefragmentOutOfOrder() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("A Man ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes("UTF-8"), attributes);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes("UTF-8"));
    }

    @Ignore("this test appears to be faulty")
    @Test
    public void testDefragmentMultipleMingledSegments() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");

        final Map<String, String> secondAttrs = new HashMap<>();
        secondAttrs.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "TWO");
        secondAttrs.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "3");

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");
        runner.enqueue("A Man ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes("UTF-8"), attributes);
        secondAttrs.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");
        runner.enqueue("No x ".getBytes("UTF-8"), secondAttrs);
        secondAttrs.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("in ".getBytes("UTF-8"), secondAttrs);
        secondAttrs.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("Nixon".getBytes("UTF-8"), secondAttrs);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes("UTF-8"), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes("UTF-8"), attributes);

        runner.run(2);

        runner.assertTransferCount(MergeContent.REL_MERGED, 2);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes("UTF-8"));
        final MockFlowFile assembledTwo = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(1);
        assembledTwo.assertContentEquals("No x in Nixon".getBytes("UTF-8"));
    }

    @Test
    public void testDefragmentOldStyleAttributes() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("segment.identifier", "1");
        attributes.put("segment.count", "4");
        attributes.put("segment.index", "1");
        attributes.put("segment.original.filename", "originalfilename");

        runner.enqueue("A Man ".getBytes("UTF-8"), attributes);
        attributes.put("segment.index", "2");
        runner.enqueue("A Plan ".getBytes("UTF-8"), attributes);
        attributes.put("segment.index", "3");
        runner.enqueue("A Canal ".getBytes("UTF-8"), attributes);
        attributes.put("segment.index", "4");
        runner.enqueue("Panama".getBytes("UTF-8"), attributes);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes("UTF-8"));
        assembled.assertAttributeEquals(CoreAttributes.FILENAME.key(), "originalfilename");
    }

    @Test
    public void testDefragmentMultipleOnTriggers() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");
        runner.enqueue("A Man ".getBytes("UTF-8"), attributes);
        runner.run();

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes("UTF-8"), attributes);
        runner.run();

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes("UTF-8"), attributes);
        runner.run();

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes("UTF-8"), attributes);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes("UTF-8"));
    }

    @Ignore("This test appears to be a fail...is retuning 1 instead of 2...needs work")
    @Test
    public void testMergeBasedOnCorrelation() throws IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_BIN_PACK);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");
        runner.setProperty(MergeContent.CORRELATION_ATTRIBUTE_NAME, "attr");
        runner.setProperty(MergeContent.MAX_ENTRIES, "3");
        runner.setProperty(MergeContent.MIN_ENTRIES, "1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "b");
        runner.enqueue("A Man ".getBytes("UTF-8"), attributes);
        runner.enqueue("A Plan ".getBytes("UTF-8"), attributes);

        attributes.put("attr", "c");
        runner.enqueue("A Canal ".getBytes("UTF-8"), attributes);

        attributes.put("attr", "b");
        runner.enqueue("Panama".getBytes("UTF-8"), attributes);

        runner.run(2);

        runner.assertTransferCount(MergeContent.REL_MERGED, 2);

        final List<MockFlowFile> mergedFiles = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        final MockFlowFile merged1 = mergedFiles.get(0);
        final MockFlowFile merged2 = mergedFiles.get(1);

        final String attr1 = merged1.getAttribute("attr");
        final String attr2 = merged2.getAttribute("attr");

        if ("c".equals(attr1)) {
            Assert.assertEquals("b", attr2);
            merged1.assertContentEquals("A Canal ", "UTF-8");
            merged2.assertContentEquals("A Man A Plan Panama", "UTF-8");
        } else {
            assertEquals("b", attr1);
            assertEquals("c", attr2);
            merged1.assertContentEquals("A Man A Plan Panama", "UTF-8");
            merged2.assertContentEquals("A Canal ", "UTF-8");
        }
    }

    @Test
    public void testMaxBinAge() throws InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_BIN_PACK);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "2 sec");
        runner.setProperty(MergeContent.CORRELATION_ATTRIBUTE_NAME, "attr");
        runner.setProperty(MergeContent.MAX_ENTRIES, "500");
        runner.setProperty(MergeContent.MIN_ENTRIES, "500");

        for (int i = 0; i < 50; i++) {
            runner.enqueue(new byte[0]);
        }

        runner.run(5, false);

        runner.assertTransferCount(MergeContent.REL_MERGED, 0);
        runner.clearTransferState();

        Thread.sleep(3000L);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
    }

    @Test
    public void testUniqueAttributes() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.ATTRIBUTE_STRATEGY, MergeContent.ATTRIBUTE_STRATEGY_ALL_UNIQUE);
        runner.setProperty(MergeContent.MAX_SIZE, "2 B");
        runner.setProperty(MergeContent.MIN_SIZE, "2 B");

        final Map<String, String> attr1 = new HashMap<>();
        attr1.put("abc", "xyz");
        attr1.put("xyz", "123");
        attr1.put("hello", "good-bye");

        final Map<String, String> attr2 = new HashMap<>();
        attr2.put("abc", "xyz");
        attr2.put("xyz", "321");
        attr2.put("world", "aaa");

        runner.enqueue(new byte[1], attr1);
        runner.enqueue(new byte[1], attr2);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile outFile = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);

        outFile.assertAttributeEquals("abc", "xyz");
        outFile.assertAttributeEquals("hello", "good-bye");
        outFile.assertAttributeEquals("world", "aaa");
        outFile.assertAttributeNotExists("xyz");
    }

    @Test
    public void testCommonAttributesOnly() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.ATTRIBUTE_STRATEGY, MergeContent.ATTRIBUTE_STRATEGY_ALL_COMMON);
        runner.setProperty(MergeContent.MAX_SIZE, "2 B");
        runner.setProperty(MergeContent.MIN_SIZE, "2 B");

        final Map<String, String> attr1 = new HashMap<>();
        attr1.put("abc", "xyz");
        attr1.put("xyz", "123");
        attr1.put("hello", "good-bye");

        final Map<String, String> attr2 = new HashMap<>();
        attr2.put("abc", "xyz");
        attr2.put("xyz", "321");
        attr2.put("world", "aaa");

        runner.enqueue(new byte[1], attr1);
        runner.enqueue(new byte[1], attr2);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile outFile = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);

        outFile.assertAttributeEquals("abc", "xyz");
        outFile.assertAttributeNotExists("hello");
        outFile.assertAttributeNotExists("world");
        outFile.assertAttributeNotExists("xyz");

        final Set<String> uuids = new HashSet<>();
        for (final MockFlowFile mff : runner.getFlowFilesForRelationship(MergeContent.REL_ORIGINAL)) {
            uuids.add(mff.getAttribute(CoreAttributes.UUID.key()));
        }
        uuids.add(outFile.getAttribute(CoreAttributes.UUID.key()));

        assertEquals(3, uuids.size());
    }

    @Test
    public void testCountAttribute() throws IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);

        createFlowFiles(runner);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        bundle.assertContentEquals("Hello, World!".getBytes("UTF-8"));
        bundle.assertAttributeEquals(MergeContent.MERGE_COUNT_ATTRIBUTE, "3");
        bundle.assertAttributeExists(MergeContent.MERGE_BIN_AGE_ATTRIBUTE);
    }

    private void createFlowFiles(final TestRunner testRunner) throws UnsupportedEncodingException {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");

        testRunner.enqueue("Hello".getBytes("UTF-8"), attributes);
        testRunner.enqueue(", ".getBytes("UTF-8"), attributes);
        testRunner.enqueue("World!".getBytes("UTF-8"), attributes);
    }

}
