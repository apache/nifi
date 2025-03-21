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
package org.apache.nifi.processors.box;

import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxFile;
import com.box.sdk.Metadata;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListBoxFileMetadataTemplatesTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_1_ID = "12345";
    private static final String TEMPLATE_1_NAME = "fileMetadata";
    private static final String TEMPLATE_1_SCOPE = "enterprise_123";
    private static final String TEMPLATE_2_ID = "67890";
    private static final String TEMPLATE_2_NAME = "properties";
    private static final String TEMPLATE_2_SCOPE = "global";

    @Mock
    private BoxFile mockBoxFile;

    @Mock
    private Metadata mockMetadata1;

    @Mock
    private Metadata mockMetadata2;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final ListBoxFileMetadataTemplates testSubject = new ListBoxFileMetadataTemplates() {
            @Override
            BoxFile getBoxFile(String fileId) {
                return mockBoxFile;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testSuccessfulMetadataRetrieval() throws InitializationException {
        // Set up a JSON record writer
        addJsonRecordWriterFactory();

        // Set up mock metadata
        final List<Metadata> metadataList = new ArrayList<>();
        metadataList.add(mockMetadata1);
        metadataList.add(mockMetadata2);

        // Template 1 setup (fileMetadata)
        when(mockMetadata1.getID()).thenReturn(TEMPLATE_1_ID);
        when(mockMetadata1.getTemplateName()).thenReturn(TEMPLATE_1_NAME);
        when(mockMetadata1.getScope()).thenReturn(TEMPLATE_1_SCOPE);
        List<String> template1Fields = List.of("fileName", "fileExtension");
        when(mockMetadata1.getPropertyPaths()).thenReturn(template1Fields);
        when(mockMetadata1.getValue("fileName")).thenReturn(null);
        when(mockMetadata1.getValue("fileExtension")).thenReturn(null);

        // Template 2 setup (properties)
        when(mockMetadata2.getID()).thenReturn(TEMPLATE_2_ID);
        when(mockMetadata2.getTemplateName()).thenReturn(TEMPLATE_2_NAME);
        when(mockMetadata2.getScope()).thenReturn(TEMPLATE_2_SCOPE);
        List<String> template2Fields = List.of("Test Number", "Title", "Author", "Date");
        when(mockMetadata2.getPropertyPaths()).thenReturn(template2Fields);
        when(mockMetadata2.getValue("Test Number")).thenReturn(null);
        when(mockMetadata2.getValue("Title")).thenReturn(null);
        when(mockMetadata2.getValue("Author")).thenReturn(null);
        when(mockMetadata2.getValue("Date")).thenReturn(null);

        // Make the iterable return our metadata list
        doReturn(metadataList).when(mockBoxFile).getAllMetadata();

        // Run the processor
        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        // Verify the results
        testRunner.assertAllFlowFilesTransferred(ListBoxFileMetadataTemplates.REL_SUCCESS, 1);

        // Check record count
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListBoxFileMetadataTemplates.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("box.file.id", TEST_FILE_ID);
        flowFile.assertAttributeExists("record.count");
    }

    @Test
    void testNoMetadata() throws InitializationException {
        // Set up a JSON record writer
        addJsonRecordWriterFactory();

        // Return empty list for getAllMetadata
        when(mockBoxFile.getAllMetadata()).thenReturn(new ArrayList<>());

        // Run the processor
        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        // Verify the results - should still be success but with no records
        testRunner.assertAllFlowFilesTransferred(ListBoxFileMetadataTemplates.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListBoxFileMetadataTemplates.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("box.file.id", TEST_FILE_ID);
    }

    @Test
    void testFileNotFound() throws InitializationException {
        // Set up a JSON record writer
        addJsonRecordWriterFactory();

        // Simulate 404 error
        BoxAPIResponseException mockException = new BoxAPIResponseException("API Error", 404, "Box File Not Found", null);
        doThrow(mockException).when(mockBoxFile).getAllMetadata();

        // Run the processor
        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        // Verify not found relationship and attributes
        testRunner.assertAllFlowFilesTransferred(ListBoxFileMetadataTemplates.REL_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListBoxFileMetadataTemplates.REL_NOT_FOUND).get(0);
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    @Test
    void testBoxApiException() throws InitializationException {
        // Set up a JSON record writer
        addJsonRecordWriterFactory();

        // Simulate general API error
        BoxAPIException mockException = new BoxAPIException("General API Error", 500, "Unexpected Error");
        doThrow(mockException).when(mockBoxFile).getAllMetadata();

        // Run the processor
        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        // Verify failure relationship and attributes
        testRunner.assertAllFlowFilesTransferred(ListBoxFileMetadataTemplates.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListBoxFileMetadataTemplates.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "General API Error\nUnexpected Error");
    }

    private void addJsonRecordWriterFactory() throws InitializationException {
        final RecordSetWriterFactory recordSetWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record_writer", recordSetWriter);
        testRunner.enableControllerService(recordSetWriter);
        testRunner.setProperty(ListBoxFileMetadataTemplates.RECORD_WRITER, "record_writer");
    }
}
