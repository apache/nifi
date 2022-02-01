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
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ExtractDocumentTextTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(ExtractDocumentText.class);
    }

    @Test
    public void processor_should_support_pdf_types_without_exception() {
        try {
            final String filename = "simple.pdf";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = new HashMap<String, String>() {{ put("filename", filename); }};
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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
                System.out.println("FILE:" + result);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void processor_should_support_doc_types_without_exception() {
        try {
            final String filename = "simple.doc";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = new HashMap<String, String>() {{ put("filename", filename); }};
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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
                System.out.println("FILE:" + result);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void processor_should_support_docx_types_without_exception() {
        try {
            final String filename = "simple.docx";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = new HashMap<String, String>() {{ put("filename", filename); }};
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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
                System.out.println("FILE:" + result);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void when_running_processor_mime_type_should_be_discovered_for_pdf_input() {
        try {
            final String filename = "simple.pdf";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = new HashMap<String, String>() {{ put("filename", filename); }};
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractDocumentText.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            mockFile.assertAttributeExists("mime.type");
            mockFile.assertAttributeEquals("mime.type", "text/plain");
            mockFile.assertAttributeExists("orig.mime.type");
            mockFile.assertAttributeEquals("orig.mime.type", "application/pdf");
        }
    }

    @Test
    public void when_running_processor_mime_type_should_be_discovered_for_doc_input() {
        try {
            final String filename = "simple.doc";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = new HashMap<String, String>() {{ put("filename", filename); }};
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractDocumentText.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            mockFile.assertAttributeExists("mime.type");
            mockFile.assertAttributeEquals("mime.type", "text/plain");
            mockFile.assertAttributeExists("orig.mime.type");
            mockFile.assertAttributeEquals("orig.mime.type", "application/msword");
        }
    }

    @Test
    public void when_running_processor_mime_type_should_be_discovered_for_docx_input() {
        try {
            final String filename = "simple.docx";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = new HashMap<String, String>() {{ put("filename", filename); }};
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractDocumentText.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            mockFile.assertAttributeExists("mime.type");
            mockFile.assertAttributeEquals("mime.type", "text/plain");
            mockFile.assertAttributeExists("orig.mime.type");
            mockFile.assertAttributeEquals("orig.mime.type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        }
    }

    @Test
    public void when_running_processor_as_default_unlimited_text_length_should_be_used() {
        try {
            final String filename = "big.pdf";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = new HashMap<String, String>() {{ put("filename", filename); }};
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractDocumentText.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractDocumentText.REL_SUCCESS);
        for (MockFlowFile mockFile : successFiles) {
            try {
                String result = new String(mockFile.toByteArray(), "UTF-8");
                assertTrue(result.length() > 100);
                System.out.println(result.length());
                System.out.println("FILE:" + result);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void when_running_processor_with_limit_text_length_should_be_less_than_or_equal_to_limit() {
        try {
            final String filename = "simple.pdf";
            MockFlowFile flowFile = testRunner.enqueue(new FileInputStream("src/test/resources/" + filename));
            Map<String, String> attrs = new HashMap<String, String>() {{ put("filename", filename); }};
            flowFile.putAttributes(attrs);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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
                System.out.println("FILE:" + result);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }
}