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

package org.apache.nifi.processors.email;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class TestExtractTNEFAttachments {

    @Test
    public void testValidTNEFWithoutAttachment() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractTNEFAttachments());


        runner.enqueue(Paths.get("src/test/resources/winmail-simple.dat"));
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 0);
        // Have a look at the attachments...
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailAttachments.REL_ATTACHMENTS);
        Assert.assertTrue(splits.size() == 0);
    }

    @Test
    public void testValidTNEFWithMultipleAttachments() throws Exception {
        Random rnd = new Random() ;
        final TestRunner runner = TestRunners.newTestRunner(new ExtractTNEFAttachments());

        runner.enqueue(Paths.get("src/test/resources/winmail-with-attachments.dat"));
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 2);
        // Have a look at the attachments...
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractTNEFAttachments.REL_ATTACHMENTS);

        List<String> filenames = new ArrayList<>();
        for (int a = 0 ; a < splits.size() ; a++ ) {
            filenames.add(splits.get(a).getAttribute("filename").toString());
        }

        Assert.assertTrue(filenames.containsAll(Arrays.asList("nifiDrop.svg", "MINIFI~1.PNG")));
    }

    @Test
    public void testValidTNEFWithAttachment() throws Exception {
        Random rnd = new Random() ;
        final TestRunner runner = TestRunners.newTestRunner(new ExtractTNEFAttachments());

        runner.enqueue(Paths.get("src/test/resources/winmail-with-attachment.dat"));
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 1);
        // Have a look at the attachments...
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractTNEFAttachments.REL_ATTACHMENTS);

        List<String> filenames = new ArrayList<>();
        for (int a = 0 ; a < splits.size() ; a++ ) {
            filenames.add(splits.get(a).getAttribute("filename").toString());
        }

        Assert.assertTrue(filenames.containsAll(Arrays.asList("nifiDrop.svg")));
    }

    @Test
    public void testInvalidTNEF() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractTNEFAttachments());
        runner.enqueue("test test test chocolate".getBytes());
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 0);
    }
}