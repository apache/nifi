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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExtractEmailAttachments {
    String from = "Alice <alice@nifi.apache.org>";
    String to = "bob@nifi.apache.org";
    String subject = "Just a test email";
    String message = "Test test test chocolate";
    String hostName = "bermudatriangle";

    GenerateAttachment attachmentGenerator = new GenerateAttachment(from, to, subject, message, hostName);

    @Test
    public void testValidEmailWithAttachments() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailAttachments());

        byte [] withAttachment = attachmentGenerator.withAttachments(1);

        runner.enqueue(withAttachment);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 1);
        // Have a look at the attachments...
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailAttachments.REL_ATTACHMENTS);
        splits.get(0).assertAttributeEquals("filename", "pom.xml-0");
    }

    @Test
    public void testValidEmailWithMultipleAttachments() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailAttachments());

        int amount = 3;
        byte [] withAttachment = attachmentGenerator.withAttachments(amount);

        runner.enqueue(withAttachment);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, amount);

        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailAttachments.REL_ATTACHMENTS);

        List<String> filenames = new ArrayList<>();
        for (int a = 0 ; a < amount ; a++ ) {
            filenames.add(splits.get(a).getAttribute("filename"));
        }

        assertTrue(filenames.containsAll(Arrays.asList("pom.xml-0", "pom.xml-1", "pom.xml-2")));
    }

    @Test
    public void testValidEmailWithoutAttachments() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailAttachments());

        byte [] simpleEmail = attachmentGenerator.simpleMessage();

        runner.enqueue(simpleEmail);
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 0);
    }

    @Test
    public void testInvalidEmail() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailAttachments());
        runner.enqueue("test test test chocolate".getBytes());
        runner.run();

        runner.assertTransferCount(ExtractEmailAttachments.REL_ORIGINAL, 0);
        runner.assertTransferCount(ExtractEmailAttachments.REL_FAILURE, 1);
        runner.assertTransferCount(ExtractEmailAttachments.REL_ATTACHMENTS, 0);
    }
}