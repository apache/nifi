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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

public class TestSplitText {

    final String originalFilename = "original.txt";
    final Path dataPath = Paths.get("src/test/resources/TestSplitText");
    final Path file = dataPath.resolve(originalFilename);

    final String alternateFilename = "data.txt";
    final Path file2 = dataPath.resolve(alternateFilename);
    final String alternateFilename2 = "data2.txt";
    final Path file3 = dataPath.resolve(alternateFilename2);

    final String longLine = "longline.txt";
    final Path fileLongLine = dataPath.resolve(longLine);

    final String longHeader = "longheader.txt";
    final Path fileLongHeader = dataPath.resolve(longHeader);

    final String originalHeaderOnly = "headersonly.txt";
    final Path fileHeaderOnly = dataPath.resolve(originalHeaderOnly);

    @Test
    public void testRelationships() {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        final Set<Relationship> relationships = runner.getProcessor().getRelationships();
        assertEquals(3, relationships.size());
        assertTrue(relationships.contains(SplitText.REL_ORIGINAL));
        assertTrue(relationships.contains(SplitText.REL_SPLITS));
        assertTrue(relationships.contains(SplitText.REL_FAILURE));
    }

    @Test
    public void testMoreThan10Splits() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "0");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "1");

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 12);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
    }

    @Test
    public void testMulticharacterHeaderMarker() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "5");
        runner.setProperty(SplitText.HEADER_MARKER, "#REM");
        runner.setProperty(SplitText.REMOVE_TRAILING_NEWLINES, "false");

        runner.enqueue(file3);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 2);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertContentEquals(file.getParent().resolve((i + 9) + ".txt"));
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
            split.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, alternateFilename2);
            split.assertAttributeEquals(SplitText.FRAGMENT_COUNT, "2");
        }
        splits.get(0).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(88));
        splits.get(0).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "5");
        splits.get(1).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(72));
        splits.get(1).assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "4");
    }

    @Test
    public void testMultipleHeaderIndicators() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.HEADER_MARKER, "#");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "5");

        runner.enqueue(file2);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 3);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        splits.get(0).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(79));
        splits.get(1).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(71));
        splits.get(2).assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(41));
    }

    @Test
    public void testCustomHeaderMarker() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "5");
        runner.setProperty(SplitText.HEADER_MARKER, "#");
        runner.setProperty(SplitText.REMOVE_TRAILING_NEWLINES, "false");

        runner.enqueue(file2);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 2);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertContentEquals(file.getParent().resolve((i + 7) + ".txt"));
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
            split.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, alternateFilename);
            split.assertAttributeEquals(SplitText.FRAGMENT_COUNT, "2");
            split.assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "5");
            split.assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(88+i));
        }
    }

    @Test
    public void testContentDivisibleBySplitCount() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 5);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
    }

    @Test
    public void testHeadersOnly() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");

        runner.enqueue(fileHeaderOnly);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
    }

    @Test
    public void testNotEnoughHeaderLines() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "3");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "2");

        runner.enqueue(fileHeaderOnly);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 0);
        runner.assertTransferCount(SplitText.REL_FAILURE, 1);
    }

    @Test
    public void testMaxSizeExceeded() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "80");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "39 B");
        runner.setProperty(SplitText.REMOVE_TRAILING_NEWLINES, "true");

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 10);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
    }

    @Test
    public void testMaxSizeExceededOnOneLine() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "80");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "100 B");
        runner.setProperty(SplitText.REMOVE_TRAILING_NEWLINES, "false");

        runner.enqueue(fileLongLine);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 3);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
    }

    @Test
    public void testMaxSizeExceededOnHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "80");
        runner.setProperty(SplitText.FRAGMENT_MAX_SIZE, "100 B");
        runner.setProperty(SplitText.REMOVE_TRAILING_NEWLINES, "false");

        runner.enqueue(fileLongHeader);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 5);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
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
    public void testZeroByteOutput() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "0");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "3");

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(SplitText.REL_SPLITS, 4);
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

        final String expected0 = "Header Line #1\nHeader Line #2\nLine #1";
        final String expected1 = "Line #2\nLine #3\nLine #4";
        final String expected2 = "Line #5\nLine #6\nLine #7";
        final String expected3 = "Line #8\nLine #9\nLine #10";

        splits.get(0).assertContentEquals(expected0);
        splits.get(1).assertContentEquals(expected1);
        splits.get(2).assertContentEquals(expected2);
        splits.get(3).assertContentEquals(expected3);
    }

    @Test
    public void testSplitWithTwoLineHeader() throws IOException {
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
    public void testSplitWithTwoLineHeaderValidateAttributes() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitText());
        runner.setProperty(SplitText.HEADER_LINE_COUNT, "2");
        runner.setProperty(SplitText.LINE_SPLIT_COUNT, "5");

        runner.enqueue(file);
        runner.run();

        runner.assertTransferCount(SplitText.REL_FAILURE, 0);
        runner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitText.REL_SPLITS, 2);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertContentEquals(file.getParent().resolve((i + 5) + ".txt"));
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.valueOf(i + 1));
            split.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, originalFilename);
            split.assertAttributeEquals(SplitText.FRAGMENT_COUNT, "2");
            split.assertAttributeEquals(SplitText.SPLIT_LINE_COUNT, "5");
            split.assertAttributeEquals(SplitText.FRAGMENT_SIZE, String.valueOf(69+i));
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
        splitRunner.setProperty(SplitText.REMOVE_TRAILING_NEWLINES, "false");

        splitRunner.enqueue(file);
        splitRunner.run();

        splitRunner.assertTransferCount(SplitText.REL_SPLITS, 4);
        splitRunner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        splitRunner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = splitRunner.getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (final MockFlowFile flowFile : splits) {
            flowFile.assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, originalFilename);
            flowFile.assertAttributeEquals(SplitText.FRAGMENT_COUNT, "4");
        }

        final TestRunner mergeRunner = TestRunners.newTestRunner(new MergeContent());
        mergeRunner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        mergeRunner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        mergeRunner.enqueue(splits.toArray(new MockFlowFile[0]));
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
