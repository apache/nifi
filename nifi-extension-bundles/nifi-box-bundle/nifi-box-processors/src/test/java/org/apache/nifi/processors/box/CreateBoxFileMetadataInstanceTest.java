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

import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxFile;
import com.box.sdk.Metadata;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class CreateBoxFileMetadataInstanceTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_NAME = "fileProperties";

    @Mock
    private BoxFile mockBoxFile;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final CreateBoxFileMetadataInstance testSubject = new CreateBoxFileMetadataInstance() {
            @Override
            BoxFile getBoxFile(String fileId) {
                return mockBoxFile;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        configureJsonRecordReader(testRunner);

        testRunner.setProperty(CreateBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(CreateBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_NAME);
        testRunner.setProperty(CreateBoxFileMetadataInstance.RECORD_READER, "json-reader");
    }

    private void configureJsonRecordReader(TestRunner runner) throws InitializationException {
        final JsonTreeReader readerService = new JsonTreeReader();

        runner.addControllerService("json-reader", readerService);
        runner.setProperty(readerService, "Date Format", "yyyy-MM-dd");
        runner.setProperty(readerService, "Timestamp Format", "yyyy-MM-dd HH:mm:ss");

        runner.enableControllerService(readerService);
    }

    @Test
    void testSuccessfulMetadataCreation() throws ParseException {
        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans",
                  "competitiveDocument": "no",
                  "status": "active",
                  "author": "Jones",
                  "int": 1,
                  "double": 1.234,
                  "almostTenToThePowerOfThirty": 1000000000000000000000000000123,
                  "array": [ "one", "two", "three" ],
                  "intArray": [ 1, 2, 3 ],
                  "date": "2025-01-01"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        final ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
        verify(mockBoxFile).createMetadata(any(), metadataCaptor.capture());

        final Metadata capturedMetadata = metadataCaptor.getValue();
        assertEquals("internal", capturedMetadata.getValue("/audience").asString());
        assertEquals("Q1 plans", capturedMetadata.getValue("/documentType").asString());
        assertEquals("no", capturedMetadata.getValue("/competitiveDocument").asString());
        assertEquals("active", capturedMetadata.getValue("/status").asString());
        assertEquals("Jones", capturedMetadata.getValue("/author").asString());
        assertEquals(1, capturedMetadata.getValue("/int").asInt());
        assertEquals(1.234, capturedMetadata.getDouble("/double"));
        assertEquals(1e30, capturedMetadata.getDouble("/almostTenToThePowerOfThirty")); // Precision loss is accepted.
        assertEquals(List.of("one", "two", "three"), capturedMetadata.getMultiSelect("/array"));
        assertEquals(List.of("1", "2", "3"), capturedMetadata.getMultiSelect("/intArray"));
        assertEquals(createLegacyDate(2025, 1, 1), capturedMetadata.getDate("/date"));

        testRunner.assertAllFlowFilesTransferred(CreateBoxFileMetadataInstance.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxFileMetadataInstance.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.template.key", TEMPLATE_NAME);
    }

    @Test
    void testEmptyInput() {
        final String inputJson = "{}";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(CreateBoxFileMetadataInstance.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxFileMetadataInstance.REL_FAILURE).getFirst();
        flowFile.assertAttributeExists("error.message");
    }

    @Test
    void testFileNotFound() {
        final BoxAPIResponseException mockException = new BoxAPIResponseException("API Error", 404, "Box File Not Found", null);
        doThrow(mockException).when(mockBoxFile).createMetadata(any(String.class), any(Metadata.class));

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(CreateBoxFileMetadataInstance.REL_FILE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxFileMetadataInstance.REL_FILE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    @Test
    void testTemplateNotFound() {
        final BoxAPIResponseException mockException = new BoxAPIResponseException("API Error", 404, "Specified Metadata Template not found", null);
        doThrow(mockException).when(mockBoxFile).createMetadata(any(String.class), any(Metadata.class));

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(CreateBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    private static Date createLegacyDate(int year, int month, int day) {
        final LocalDate date = LocalDate.of(year, month, day);
        final Instant instant = date.atStartOfDay(UTC).toInstant();
        return Date.from(instant);
    }
}
