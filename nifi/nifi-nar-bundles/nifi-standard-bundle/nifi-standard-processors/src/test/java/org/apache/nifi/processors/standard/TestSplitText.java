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

//    public static void main(final String[] args) throws IOException {
//        for (int i=1; i <= 4; i++) {
//            final Path path = Paths.get("src/test/resources/TestSplitText/" + i + ".txt");
//            final byte[] data = Files.readAllBytes(path);
//            final String text = new String(data, StandardCharsets.UTF_8);
//            final String updated = text.replace("\n", "\r\n");
//            final Path updatedPath = Paths.get("src/test/resources/TestSplitText/updated/" + i + ".txt");
//            Files.write(updatedPath, updated.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);
//        }
//    }
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

        final List<MockFlowFile> splits = runner.
                getFlowFilesForRelationship(SplitText.REL_SPLITS);

        final String expected0 = "Header Line #1\nHeader Line #2\nLine #1";
        final String expected1 = "Line #2\nLine #3\nLine #4";
        final String expected2 = "Line #5\nLine #6\nLine #7";
        final String expected3 = "Line #8\nLine #9\nLine #10";

        splits.get(0).
                assertContentEquals(expected0);
        splits.get(1).
                assertContentEquals(expected1);
        splits.get(2).
                assertContentEquals(expected2);
        splits.get(3).
                assertContentEquals(expected3);
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

        final List<MockFlowFile> splits = runner.
                getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (int i = 0; i < splits.size(); i++) {
            final MockFlowFile split = splits.get(i);
            split.assertContentEquals(file.getParent().
                    resolve((i + 1) + ".txt"));
            split.assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.
                    valueOf(i + 1));
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

        final List<MockFlowFile> splits = runner.
                getFlowFilesForRelationship(SplitText.REL_SPLITS);
        splits.get(0).
                assertContentEquals(file.getParent().
                        resolve("5.txt"));
        splits.get(0).
                assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.
                        valueOf(1));
        splits.get(1).
                assertContentEquals(file.getParent().
                        resolve("6.txt"));
        splits.get(1).
                assertAttributeEquals(SplitText.FRAGMENT_INDEX, String.
                        valueOf(2));
    }

    @Test
    public void testSplitThenMerge() throws IOException {
        final TestRunner splitRunner = TestRunners.
                newTestRunner(new SplitText());
        splitRunner.setProperty(SplitText.LINE_SPLIT_COUNT, "3");
        splitRunner.setProperty(SplitText.REMOVE_TRAILING_NEWLINES, "false");

        splitRunner.enqueue(file);
        splitRunner.run();

        splitRunner.assertTransferCount(SplitText.REL_SPLITS, 4);
        splitRunner.assertTransferCount(SplitText.REL_ORIGINAL, 1);
        splitRunner.assertTransferCount(SplitText.REL_FAILURE, 0);

        final List<MockFlowFile> splits = splitRunner.
                getFlowFilesForRelationship(SplitText.REL_SPLITS);
        for (final MockFlowFile flowFile : splits) {
            flowFile.
                    assertAttributeEquals(SplitText.SEGMENT_ORIGINAL_FILENAME, originalFilename);
            flowFile.assertAttributeEquals(SplitText.FRAGMENT_COUNT, "4");
        }

        final TestRunner mergeRunner = TestRunners.
                newTestRunner(new MergeContent());
        mergeRunner.
                setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        mergeRunner.
                setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        mergeRunner.enqueue(splits.toArray(new MockFlowFile[0]));
        mergeRunner.run();

        mergeRunner.assertTransferCount(MergeContent.REL_MERGED, 1);
        mergeRunner.assertTransferCount(MergeContent.REL_ORIGINAL, 4);
        mergeRunner.assertTransferCount(MergeContent.REL_FAILURE, 0);

        final List<MockFlowFile> packed = mergeRunner.
                getFlowFilesForRelationship(MergeContent.REL_MERGED);
        MockFlowFile flowFile = packed.get(0);
        flowFile.
                assertAttributeEquals(CoreAttributes.FILENAME.key(), originalFilename);
        assertEquals(Files.size(dataPath.resolve(originalFilename)), flowFile.
                getSize());
        flowFile.assertContentEquals(file);
    }
}
