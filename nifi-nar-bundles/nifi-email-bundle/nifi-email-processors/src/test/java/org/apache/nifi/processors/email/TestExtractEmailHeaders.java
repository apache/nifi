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

import java.util.List;

public class TestExtractEmailHeaders {
    String from = "Alice <alice@nifi.apache.org>";
    String to = "bob@nifi.apache.org";
    String subject = "Just a test email";
    String message = "Test test test chocolate";
    String hostName = "bermudatriangle";

    GenerateAttachment attachmentGenerator = new GenerateAttachment(from, to, subject, message, hostName);

    @Test
    public void testValidEmailWithAttachments() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailHeaders());

        byte [] withAttachment = attachmentGenerator.withAttachments(1);

        runner.enqueue(withAttachment);
        runner.run();

        runner.assertTransferCount(ExtractEmailHeaders.REL_SUCCESS, 1);
        runner.assertTransferCount(ExtractEmailHeaders.REL_FAILURE, 0);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailHeaders.REL_SUCCESS);
        splits.get(0).assertAttributeEquals("email.headers.from.0", from);
        splits.get(0).assertAttributeEquals("email.headers.to.0", to);
        splits.get(0).assertAttributeEquals("email.headers.subject", subject);
        splits.get(0).assertAttributeEquals("email.attachment_count", "1");
    }

    @Test
    public void testValidEmailWithoutAttachments() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailHeaders());
        runner.setProperty(ExtractEmailHeaders.CAPTURED_HEADERS, "MIME-Version");

        byte [] simpleEmail = attachmentGenerator.simpleMessage(to);

        runner.enqueue(simpleEmail);
        runner.run();

        runner.assertTransferCount(ExtractEmailHeaders.REL_SUCCESS, 1);
        runner.assertTransferCount(ExtractEmailHeaders.REL_FAILURE, 0);


        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailHeaders.REL_SUCCESS);
        splits.get(0).assertAttributeEquals("email.headers.from.0", from);
        splits.get(0).assertAttributeEquals("email.headers.to.0", to);
        splits.get(0).assertAttributeEquals("email.attachment_count", "0");
        splits.get(0).assertAttributeExists("email.headers.mime-version");
    }

    /**
     * Test case added for NIFI-4326 for a potential NPE bug
     * if the email message contains no recipient header fields, ie,
     * TO, CC, BCC.
     */
    @Test
    public void testValidEmailWithNoRecipients() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailHeaders());
        runner.setProperty(ExtractEmailHeaders.CAPTURED_HEADERS, "MIME-Version");

        final byte[] message = attachmentGenerator.simpleMessage();
        runner.enqueue(message);
        runner.run();

        runner.assertTransferCount(ExtractEmailHeaders.REL_SUCCESS, 1);
        runner.assertTransferCount(ExtractEmailHeaders.REL_FAILURE, 0);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailHeaders.REL_SUCCESS);
        splits.get(0).assertAttributeEquals("email.headers.from.0", from);
        splits.get(0).assertAttributeExists("email.headers.mime-version");
        splits.get(0).assertAttributeNotExists("email.headers.to");
        splits.get(0).assertAttributeNotExists("email.headers.cc");
        splits.get(0).assertAttributeNotExists("email.headers.bcc");
    }

    /**
     * NIFI-4326 adds a new feature to disable strict address parsing for
     * mailbox list header fields. This is a test case that asserts that
     * lax address parsing passes (when set to "strict=false") for malformed
     * addresses.
     */
    @Test
    public void testNonStrictParsingPassesForInvalidAddresses() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailHeaders());
        runner.setProperty(ExtractEmailHeaders.STRICT_PARSING, "false");

        final byte[] message = attachmentGenerator.simpleMessage("<>, Joe, \"\" <>");

        runner.enqueue(message);
        runner.run();

        runner.assertTransferCount(ExtractEmailHeaders.REL_SUCCESS, 1);
        runner.assertTransferCount(ExtractEmailHeaders.REL_FAILURE, 0);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ExtractEmailHeaders.REL_SUCCESS);

        splits.get(0).assertAttributeEquals("email.headers.to.0", "");
        splits.get(0).assertAttributeEquals("email.headers.to.1", "Joe");
        splits.get(0).assertAttributeEquals("email.headers.to.2", "");
    }

    /**
     * NIFI-4326 adds a new feature to disable strict address parsing for
     * mailbox list header fields. This is a test case that asserts that
     * strict address parsing fails (when set to "strict=true") for malformed
     * addresses.
     */
    @Test
    public void testStrictParsingFailsForInvalidAddresses() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailHeaders());
        runner.setProperty(ExtractEmailHeaders.STRICT_PARSING, "true");

        final byte[] message = attachmentGenerator.simpleMessage("<>, Joe, \"\" <>");

        runner.enqueue(message);
        runner.run();

        runner.assertTransferCount(ExtractEmailHeaders.REL_SUCCESS, 0);
        runner.assertTransferCount(ExtractEmailHeaders.REL_FAILURE, 1);
    }

    @Test
    public void testInvalidEmail() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractEmailHeaders());
        runner.enqueue("test test test chocolate".getBytes());
        runner.run();

        runner.assertTransferCount(ExtractEmailHeaders.REL_SUCCESS, 0);
        runner.assertTransferCount(ExtractEmailHeaders.REL_FAILURE, 1);
    }
}
