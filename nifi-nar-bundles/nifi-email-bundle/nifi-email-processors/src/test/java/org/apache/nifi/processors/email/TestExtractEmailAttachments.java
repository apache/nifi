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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class TestExtractEmailAttachments {
    // Setups the fields to be used...
    String from = "Alice <alice@nifi.apache.org>";
    String to = "bob@nifi.apache.org";
    String subject = "Just a test email";
    String message = "Test test test chocolate";
    String hostName = "bermudatriangle";

    GenerateAttachment attachmentGenerator = new GenerateAttachment(from, to, subject, message, hostName);


    @Test
    public void testValidEmailWithAttachments() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailAttachments());

        // Create the message dynamically
        byte [] withAttachment = attachmentGenerator.WithAttachments(1);

        runner.enqueue(withAttachment);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 1);
        // Have a look at the attachments...
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailAttachments.REL_ATTACHMENTS);
        splits.get(0).assertAttributeEquals("filename", "pom.xml1");
    }

    @Test
    public void testValidEmailWithMultipleAttachments() throws Exception {
        Random rnd = new Random() ;
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailAttachments());

        // Create the message dynamically
        int amount = rnd.nextInt(10) + 1;
        byte [] withAttachment = attachmentGenerator.WithAttachments(amount);

        runner.enqueue(withAttachment);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, amount);
        // Have a look at the attachments...
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailAttachments.REL_ATTACHMENTS);

        List<String> filenames = new ArrayList<>();
        for (int a = 0 ; a < amount ; a++ ) {
            filenames.add(splits.get(a).getAttribute("filename").toString());
        }

        Assert.assertTrue(filenames.containsAll(Arrays.asList("pom.xml1", "pom.xml" + amount)));
    }

    @Test
    public void testValidEmailWithoutAttachments() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailAttachments());

        // Create the message dynamically
        byte [] simpleEmail = attachmentGenerator.SimpleEmail();

        runner.enqueue(simpleEmail);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 0);

    }

    @Test
    public void testInvalidEmail() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailAttachments());
        runner.enqueue("test test test chocolate".getBytes());
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 0);
    }
}