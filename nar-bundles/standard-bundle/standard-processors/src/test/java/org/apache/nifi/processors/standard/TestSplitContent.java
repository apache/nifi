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

import org.apache.nifi.processors.standard.MergeContent;
import org.apache.nifi.processors.standard.SplitContent;
import java.io.IOException;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestSplitContent {

    @Test
    public void testSmallSplits() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "FFFF");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 5, 4, 3, 2, 1});
        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5});
        split2.assertContentEquals(new byte[]{(byte) 0xFF, 5, 4, 3, 2, 1});
    }

    @Test
    public void testWithSingleByteSplit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "FF");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, 5, 4, 3, 2, 1});
        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5});
        split2.assertContentEquals(new byte[]{5, 4, 3, 2, 1});
    }

    @Test
    public void testWithLargerSplit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{
            1, 2, 3, 4, 5,
            5, 5, 5, 5,
            5, 4, 3, 2, 1});

        runner.run();
        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4});
        split2.assertContentEquals(new byte[]{5, 5, 4, 3, 2, 1});
    }

    @Test
    public void testKeepingSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{
            1, 2, 3, 4, 5,
            5, 5, 5, 5,
            5, 4, 3, 2, 1});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5, 5, 5, 5});
        split2.assertContentEquals(new byte[]{5, 5, 4, 3, 2, 1});
    }

    @Test
    public void testEndsWithSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, 5, 5, 5});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitContent.REL_SPLITS, 1);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4});
    }

    @Test
    public void testEndsWithSequenceAndKeepSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, 5, 5, 5});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitContent.REL_SPLITS, 1);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5, 5, 5, 5});
    }

    @Test
    public void testStartsWithSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "false");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{5, 5, 5, 5, 1, 2, 3, 4});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitContent.REL_SPLITS, 1);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4});
    }

    @Test
    public void testStartsWithSequenceAndKeepSequence() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "05050505");

        runner.enqueue(new byte[]{5, 5, 5, 5, 1, 2, 3, 4});

        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        splits.get(0).assertContentEquals(new byte[]{5, 5, 5, 5});
        splits.get(1).assertContentEquals(new byte[]{1, 2, 3, 4});
    }

    @Test
    public void testSmallSplitsThenMerge() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitContent());
        runner.setProperty(SplitContent.KEEP_SEQUENCE, "true");
        runner.setProperty(SplitContent.BYTE_SEQUENCE.getName(), "FFFF");

        runner.enqueue(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 5, 4, 3, 2, 1});
        runner.run();

        runner.assertTransferCount(SplitContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitContent.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitContent.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        split1.assertContentEquals(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, (byte) 0xFF});
        split2.assertContentEquals(new byte[]{(byte) 0xFF, 5, 4, 3, 2, 1});

        final TestRunner mergeRunner = TestRunners.newTestRunner(new MergeContent());
        mergeRunner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        mergeRunner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        mergeRunner.enqueue(splits.toArray(new MockFlowFile[0]));
        mergeRunner.run();

        mergeRunner.assertTransferCount(MergeContent.REL_MERGED, 1);
        mergeRunner.assertTransferCount(MergeContent.REL_ORIGINAL, 2);
        mergeRunner.assertTransferCount(MergeContent.REL_FAILURE, 0);

        final List<MockFlowFile> packed = mergeRunner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        packed.get(0).assertContentEquals(new byte[]{1, 2, 3, 4, 5, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 5, 4, 3, 2, 1});
    }
}
