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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ExtractDocumentTextTest {

    private static final String ORIG_MIME_TYPE_ATTR = "orig.mime.type";
    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(ExtractDocumentText.class);
    }

    @Test
    @DisplayName("Should support PDF types without exceptions being thrown")
    public void processorShouldSupportPDF() {
        try {
            final String filename = "simple.pdf";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = Collections.singletonMap("filename", filename);
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {

        }

        testRunner.assertValid();
        testRunner.run();
        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            try {
                String result = new String(mockFile.toByteArray(), "UTF-8");
                String trimmedResult = result.trim();
                assertTrue(trimmedResult.startsWith("A Simple PDF File"));
            } catch (UnsupportedEncodingException e) {
            }
        }
    }

    @Test
    @DisplayName("Should support MS Word DOC types without throwing exceptions")
    public void processorShouldSupportDOC() {
        try {
            final String filename = "simple.doc";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = Collections.singletonMap("filename", filename);
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {

        }

        testRunner.assertValid();
        testRunner.run();
        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            try {
                String result = new String(mockFile.toByteArray(), "UTF-8");
                String trimmedResult = result.trim();
                assertTrue(trimmedResult.startsWith("A Simple WORD DOC File"));
            } catch (UnsupportedEncodingException e) {
            }
        }
    }

    @Test
    @DisplayName("Should support MS Word DOCX types without exception")
    public void processorShouldSupportDOCX() {
        try {
            final String filename = "simple.docx";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = Collections.singletonMap("filename", filename);
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {

        }

        testRunner.assertValid();
        testRunner.run();
        testRunner.assertTransferCount(ExtractDocumentText.REL_FAILURE, 0);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            try {
                String result = new String(mockFile.toByteArray(), "UTF-8");
                String trimmedResult = result.trim();
                assertTrue(trimmedResult.startsWith("A Simple WORD DOCX File"));
            } catch (UnsupportedEncodingException e) {
            }
        }
    }

    @Test
    @DisplayName("The PDF mime type should be discovered when examining a PDF")
    public void shouldFindPDFMimeTypeWhenProcessingPDFs() {
        try {
            final String filename = "simple.pdf";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = Collections.singletonMap("filename", filename);
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {

        }

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractDocumentText.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            mockFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            mockFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
            mockFile.assertAttributeExists(ORIG_MIME_TYPE_ATTR);
            mockFile.assertAttributeEquals(ORIG_MIME_TYPE_ATTR, "application/pdf");
        }
    }

    @Test
    @DisplayName("DOC mime type should be discovered when processing a MS Word doc file")
    public void shouldFindDOCMimeTypeWhenProcessingMSWordDoc() {
        try {
            final String filename = "simple.doc";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = Collections.singletonMap("filename", filename);
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {

        }

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractDocumentText.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            mockFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            mockFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
            mockFile.assertAttributeExists(ORIG_MIME_TYPE_ATTR);
            mockFile.assertAttributeEquals(ORIG_MIME_TYPE_ATTR, "application/msword");
        }
    }

    @Test
    @DisplayName("Should discover DOCX mime type when processing docx file")
    public void shouldFindDOCXMimeType() {
        try {
            final String filename = "simple.docx";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = Collections.singletonMap("filename", filename);
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {

        }

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractDocumentText.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            mockFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            mockFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
            mockFile.assertAttributeExists(ORIG_MIME_TYPE_ATTR);
            mockFile.assertAttributeEquals(ORIG_MIME_TYPE_ATTR, "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        }
    }

    @Test
    @DisplayName("Unlimited text length should be the default setting")
    public void unlimitedTextShouldBeDefault() {
        try {
            final String filename = "big.pdf";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = Collections.singletonMap("filename", filename);
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {

        }

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractDocumentText.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            try {
                String result = new String(mockFile.toByteArray(), "UTF-8");
                assertTrue(result.length() > 100);
            } catch (UnsupportedEncodingException e) {

            }
        }
    }

    @Test
    @DisplayName("When running with a text limit, length should be <= the limit")
    public void testTextLimit() {
        try {
            final String filename = "simple.pdf";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = Collections.singletonMap("filename", filename);
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {

        }

        testRunner.setProperty(ExtractDocumentText.MAX_TEXT_LENGTH, "100");
        testRunner.assertValid();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractDocumentText.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            try {
                String result = new String(mockFile.toByteArray(), "UTF-8");
                assertFalse(result.length() > 100);
            } catch (UnsupportedEncodingException e) {

            }
        }
    }
}