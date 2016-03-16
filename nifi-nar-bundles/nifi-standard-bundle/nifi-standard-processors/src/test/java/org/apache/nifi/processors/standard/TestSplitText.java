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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class TestSplitText {

    final String originalFilename = "original.txt";
    final Path dataPath = Paths.get("src/test/resources/TestSplitText");
    final Path file = dataPath.resolve(originalFilename);
    final Path crFile = dataPath.resolve("carriage-return.txt");
    final Path blankLinesFile = dataPath.resolve("blankLines.txt");
    final Path longLineFile = dataPath.resolve("longLines.txt");
    final Path headerOnlyFile = dataPath.resolve("header.txt");

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.standard.SplitText", "debug");
    }

    @Test
    public void testWithoutFlowFile() {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "50 B");
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");

        runner.run();

        runner.assertTransferCount(SplitText.REL_SPLITS, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 0);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
    }

    @Test
    public void testFlowFileIsOnlyHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "50 B");
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");

        runner.enqueue(headerOnlyFile);
        runner.run();

        runner.assertTransferCount(SplitText.REL_SPLITS, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
    }

    @Test
    public void testMultipleSplitDirectives() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "50 B");
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");

        runner.enqueue(longLineFile);
        runner.run();

        runner.assertTransferCount(SplitText.REL_SPLITS, 6);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        assertEquals(46, splits.get(0).getSize());
        assertEquals(118, splits.get(1).getSize());
        assertEquals(46, splits.get(2).getSize());
        assertEquals(46, splits.get(3).getSize());
        assertEquals(46, splits.get(4).getSize());
        assertEquals(39, splits.get(5).getSize());
    }

    @Test
    public void testZeroLinesNoMaxSize() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "0");

        runner.assertNotValid();
    }

    @Test
    public void testCarriageReturnNoNewline() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");
        runner.setProperty(SplitText.HEADER_MARKER, "#");

        runner.enqueue(crFile);
        runner.run();

        runner.assertTransferCount(SplitText.REL_SPLITS, 2);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final String expected0 = "#Header line 1\r#Header line 2\rData line 1\rData line 2\r";
        final String expected1 = "#Header line 1\r#Header line 2\rData line 3\rData line 4\r";

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        splits.get(0).assertContentEquals(expected0);
        splits.get(1).assertContentEquals(expected1);
    }

    @Test
    public void testRemoveTrailingNewlines() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");
        runner.setProperty(SplitText.REMOVE_TRAILING_NEWLINES, "true");

        runner.assertNotValid();
    }

    @Test
    public void testMultipleHeaderIndicators() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "1");
        runner.setProperty(SplitText.HEADER_MARKER, "Head");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "5");
        runner.setProperty(SplitText.REMOVE_TRAILING_NEWLINES, "false");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_SPLITS, 3);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        splits.get(0).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "5");
        splits.get(0).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(62));
        splits.get(1).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "5");
        splits.get(1).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(55));
        splits.get(2).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "1");
        splits.get(2).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(23));
        final String fragmentUUID = splits.get(0).getAttribute("fragment.identifier");
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
            split.assertAttributeEquals(SplitText.FRAGMENT_ID, fragmentUUID);
            split.assertAttributeEquals(SplitText.FRAGMENT_COUNT, String.valueOf(splits.size()));
            split.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, file.getFileName().toString());
        }
    }

    @Test
    public void testSingleCharacterHeaderMarker() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "7");
        runner.setProperty(SplitText.HEADER_MARKER, "H");

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 2);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        splits.get(0).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "7");
        splits.get(0).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(86));
        splits.get(1).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "3");
        splits.get(1).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(54));
        final String fragmentUUID = splits.get(0).getAttribute("fragment.identifier");
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
            split.assertAttributeEquals(SplitText.FRAGMENT_ID, fragmentUUID);
            split.assertAttributeEquals(SplitText.FRAGMENT_COUNT, String.valueOf(splits.size()));
            split.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, file.getFileName().toString());
        }
    }

    @Test
    public void testMulticharacterHeaderMarker() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "6");
        runner.setProperty(SplitText.HEADER_MARKER, "Head");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_SPLITS, 2);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        splits.get(0).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "6");
        splits.get(0).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(78));
        splits.get(1).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "4");
        splits.get(1).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(62));
        final String fragmentUUID = splits.get(0).getAttribute("fragment.identifier");
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
            split.assertAttributeEquals(SplitText.FRAGMENT_ID, fragmentUUID);
            split.assertAttributeEquals(SplitText.FRAGMENT_COUNT, String.valueOf(splits.size()));
            split.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, file.getFileName().toString());
        }
    }

    @Test
    public void testMaxSizeExceededWithHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "0");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "71 B");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_SPLITS, 2);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        final String fragmentUUID = splits.get(0).getAttribute("fragment.identifier");
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "5");
            split.assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(70));
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
            split.assertAttributeEquals(SplitText.FRAGMENT_ID, fragmentUUID);
            split.assertAttributeEquals(SplitText.FRAGMENT_COUNT, String.valueOf(splits.size()));
            split.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, file.getFileName().toString());
        }
    }

    @Test
    public void testMaxSizeExceededNoHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "0");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "99999");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "100 B");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_SPLITS, 2);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        splits.get(0).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "10");
        splits.get(0).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(94));
        splits.get(1).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "2");
        splits.get(1).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(16));
        final String fragmentUUID = splits.get(0).getAttribute("fragment.identifier");
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
            split.assertAttributeEquals(SplitText.FRAGMENT_ID, fragmentUUID);
            split.assertAttributeEquals(SplitText.FRAGMENT_COUNT, String.valueOf(splits.size()));
            split.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, file.getFileName().toString());
        }
    }

    @Test
    public void testMultipleHeaderLinesLargerThanMaxSize() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "99999");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "5 B");
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_SPLITS, 10);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        final String fragmentUUID = splits.get(0).getAttribute("fragment.identifier");
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "1");
            split.assertAttributeEquals(SplitText.FRAGMENT_SIZE, "38");
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
            split.assertAttributeEquals(SplitText.FRAGMENT_ID, fragmentUUID);
            split.assertAttributeEquals(SplitText.FRAGMENT_COUNT, String.valueOf(splits.size()));
            split.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, file.getFileName().toString());
        }
    }

    @Test
    public void testRoutesToFailureIfHeaderLinesNotAllPresent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "100");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "3");

        runner.enqueue(file);
        runner.run();

        runner.assertAllFlowFilesTransferred(SplitText.REL_FAILURE, 1);
    }

    @Test
    public void testSplitWithoutHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "0");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "3");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_SPLITS, 4);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);

        final String expected0 = "Header Line #1\nHeader Line #2\nLine #1\n";
        final String expected1 = "Line #2\nLine #3\nLine #4\n";
        final String expected2 = "Line #5\nLine #6\nLine #7\n";
        final String expected3 = "Line #8\nLine #9\nLine #10";

        splits.get(0).assertContentEquals(expected0);
        splits.get(1).assertContentEquals(expected1);
        splits.get(2).assertContentEquals(expected2);
        splits.get(3).assertContentEquals(expected3);
    }

    @Test
    public void testSplitWithTwoLineHeaderLineLimit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "3");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_SPLITS, 4);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertContentEquals(file.getParent().resolve((i + 1) + ".txt"));
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
        }
    }

    @Test
    public void testSplitWithTwoLineHeaderSizeLimit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "55 B");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "99999");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_SPLITS, 4);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertContentEquals(file.getParent().resolve((i + 1) + ".txt"));
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
        }
    }

    @Test
    public void testBlankLinesWithHeaderAndSmallSplitSize() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");

        runner.enqueue(blankLinesFile);
        runner.run();

        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_SPLITS, 3);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertContentEquals(blankLinesFile.getParent().resolve("blankLines_out" + (i+1) + ".txt"));
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
        }
    }

    @Test
    public void testBlankLinesNoHeaderAndSmallSplitSize() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "0");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");

        runner.enqueue(blankLinesFile);
        runner.run();

        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_SPLITS, 4);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertContentEquals(blankLinesFile.getParent().resolve("blankLines_noHeader_out" + (i+1) + ".txt"));
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
        }
    }

    @Test
    public void testSplitWithTwoLineHeaderAndEvenMultipleOfLines() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "5");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_SPLITS, 2);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        splits.get(0).assertContentEquals(file.getParent().resolve("5.txt"));
        splits.get(0).assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(1));
        splits.get(1).assertContentEquals(file.getParent().resolve("6.txt"));
        splits.get(1).assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(2));
    }

    @Test
    public void testSplitThenMerge() throws IOException {
        final TestRunner splitRunner = TestRunners.newTestRunner(new SplitText());
        splitRunner.setProperty(SplitText.LINE_SPLIT_COUNT, "3");

        splitRunner.enqueue(file);
        splitRunner.run();

        splitRunner.assertTransferCount(SplitText.REL_SPLITS, 4);
        splitRunner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        splitRunner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = splitRunner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (final MockFlowFile flowFile : splits) {
            flowFile.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, originalFilename);
            flowFile.assertAttributeEquals(SplitText.FRAGMENT_COUNT, String.valueOf(splits.size()));
        }

        final TestRunner mergeRunner = TestRunners.newTestRunner(new MergeContent());
        mergeRunner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        mergeRunner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        mergeRunner.enqueue(splits.toArray(new MockFlowFile[splits.size()]));
        mergeRunner.run();

        mergeRunner.assertTransferCount(MergeContent.REL_MERGED, 1);
        mergeRunner.assertTransferCount(MergeContent.REL_ORIGINAL, 4);
        mergeRunner.assertTransferCount(MergeContent.REL_FAILURE, 0);

        final List<MockFlowFile> packed = mergeRunner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        MockFlowFile flowFile = packed.get(0);
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), originalFilename);
        assertEquals(Files.size(dataPath.resolve(originalFilename)), flowFile.getSize());
        flowFile.assertContentEquals(file);
    }

}
