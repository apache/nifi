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
package org.apache.nifi.processors.document;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExtractDocumentTextTest {
    private TestRunner testRunner;

    @BeforeEach
    public void setTestRunner() {
        testRunner = TestRunners.newTestRunner(ExtractDocumentText.class);
    }

    @Test
    public void testRunPdf() throws Exception {
        final String filename = "simple.pdf";
        testRunner.enqueue(getFileInputStream(filename));
        testRunner.run();
        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            String result = new String(mockFile.toByteArray(), StandardCharsets.UTF_8);
            String trimmedResult = result.trim();
            assertTrue(trimmedResult.startsWith("A Simple PDF File"));
        }
    }

    @Test
    public void testRunDoc() throws Exception {
        final String filename = "simple.doc";
        testRunner.enqueue(getFileInputStream(filename));
        testRunner.run();
        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            String result = new String(mockFile.toByteArray(), StandardCharsets.UTF_8);
            String trimmedResult = result.trim();
            assertTrue(trimmedResult.startsWith("A Simple WORD DOC File"));
        }
    }

    @Test
    public void testRunDocx() throws Exception {
        final String filename = "simple.docx";
        testRunner.enqueue(getFileInputStream(filename));
        testRunner.run();
        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_EXTRACTED);
        for (MockFlowFile mockFile : successFiles) {
            String result = new String(mockFile.toByteArray(), StandardCharsets.UTF_8);
            String trimmedResult = result.trim();
            assertTrue(trimmedResult.startsWith("A Simple WORD DOCX File"));
        }
    }

    private FileInputStream getFileInputStream(final String filename) throws FileNotFoundException {
        return new FileInputStream("src/test/resources/" + filename);
    }
}