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
import com.box.sdk.MetadataTemplate;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateBoxMetadataTemplateTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_KEY = "customerInfo";
    private static final String SCOPE = "enterprise";

    private List<MetadataTemplate.FieldOperation> capturedOperations = new ArrayList<>();
    private String capturedScope;
    private String capturedTemplateKey;

    private class TestUpdateBoxMetadataTemplate extends UpdateBoxMetadataTemplate {
        @Override
        protected MetadataTemplate getMetadataTemplate(final String scope, final String templateKey) {
            if (scope.equals("notFound") || templateKey.equals("notFound")) {
                throw new BoxAPIResponseException("Not Found", 404, "Not Found", null);
            }

            final MetadataTemplate mockTemplate = mock(MetadataTemplate.class);
            final List<MetadataTemplate.Field> fields = new ArrayList<>();

            // Create an existing field
            final MetadataTemplate.Field field1 = mock(MetadataTemplate.Field.class);
            when(field1.getKey()).thenReturn("name");
            when(field1.getDisplayName()).thenReturn("Name");
            when(field1.getType()).thenReturn("string");
            when(field1.getIsHidden()).thenReturn(false);
            fields.add(field1);

            // Create another existing field
            final MetadataTemplate.Field field2 = mock(MetadataTemplate.Field.class);
            when(field2.getKey()).thenReturn("industry");
            when(field2.getDisplayName()).thenReturn("Industry");
            when(field2.getType()).thenReturn("enum");
            when(field2.getIsHidden()).thenReturn(false);
            when(field2.getOptions()).thenReturn(Arrays.asList("Technology", "Healthcare", "Legal"));
            fields.add(field2);

            when(mockTemplate.getFields()).thenReturn(fields);
            return mockTemplate;
        }

        @Override
        protected void updateMetadataTemplate(final String scope,
                                              final String templateKey,
                                              final List<MetadataTemplate.FieldOperation> operations) {
            if (scope.equals("forbidden") || templateKey.equals("forbidden")) {
                throw new BoxAPIResponseException("Permission Denied", 403, "Permission Denied", null);
            }

            capturedScope = scope;
            capturedTemplateKey = templateKey;
            capturedOperations = operations;
        }
    }

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final TestUpdateBoxMetadataTemplate processor = new TestUpdateBoxMetadataTemplate();
        testRunner = TestRunners.newTestRunner(processor);
        super.setUp();

        configureJsonRecordReader(testRunner);
        testRunner.setProperty(UpdateBoxMetadataTemplate.TEMPLATE_KEY, TEMPLATE_KEY);
        testRunner.setProperty(UpdateBoxMetadataTemplate.SCOPE, SCOPE);
        testRunner.setProperty(UpdateBoxMetadataTemplate.RECORD_READER, "json-reader");
    }

    private void configureJsonRecordReader(TestRunner runner) throws InitializationException {
        final JsonTreeReader readerService = new JsonTreeReader();
        runner.addControllerService("json-reader", readerService);
        runner.enableControllerService(readerService);
    }

    @Test
    public void testUpdateFields() {
        // Content represents desired template state
        final String content = """
                [
                    {
                        "key": "company_name",
                        "displayName": "Company Name",
                        "type": "string",
                        "hidden": false
                    },
                    {
                        "key": "industry",
                        "displayName": "Industry Sector",
                        "type": "enum",
                        "hidden": false,
                        "options": ["Technology", "Healthcare", "Legal", "Finance"]
                    },
                    {
                        "key": "tav",
                        "displayName": "Total Account Value",
                        "type": "float",
                        "hidden": false
                    }
                ]
                """;

        testRunner.enqueue(content);
        testRunner.run();

        // Verify operations were generated correctly (should create multiple operations)
        assertEquals(SCOPE, capturedScope);
        assertEquals(TEMPLATE_KEY, capturedTemplateKey);
        assertFalse(capturedOperations.isEmpty(), "Should generate operations for template updates");

        testRunner.assertAllFlowFilesTransferred(UpdateBoxMetadataTemplate.REL_SUCCESS, 1);
        final MockFlowFile outFile = testRunner.getFlowFilesForRelationship(UpdateBoxMetadataTemplate.REL_SUCCESS).getFirst();
        outFile.assertAttributeEquals("box.template.key", TEMPLATE_KEY);
        outFile.assertAttributeEquals("box.template.scope", SCOPE);
        outFile.assertAttributeEquals("box.template.operations.count", String.valueOf(capturedOperations.size()));
    }

    @Test
    public void testTemplateNotFound() {
        // Setup test to throw not found exception
        testRunner.setProperty(UpdateBoxMetadataTemplate.TEMPLATE_KEY, "notFound");

        final String content = """
                [
                    {
                        "key": "company_name",
                        "displayName": "Company Name",
                        "type": "string",
                        "hidden": false
                    }
                ]
                """;

        testRunner.enqueue(content);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(UpdateBoxMetadataTemplate.REL_TEMPLATE_NOT_FOUND, 1);
        final MockFlowFile outFile = testRunner.getFlowFilesForRelationship(UpdateBoxMetadataTemplate.REL_TEMPLATE_NOT_FOUND).getFirst();
        outFile.assertAttributeExists(BoxFileAttributes.ERROR_MESSAGE);
        outFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }

    @Test
    public void testInvalidInputRecord() {
        // Template JSON missing required key field
        final String content = """
                [
                    {
                        "displayName": "Company Name",
                        "type": "string"
                    }
                ]
                """;

        testRunner.enqueue(content);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(UpdateBoxMetadataTemplate.REL_FAILURE, 1);
    }

    @Test
    public void testNoChangesNeeded() {
        // Input is the same as the current template (no changes needed)
        final String content = """
                [
                    {
                        "key": "name",
                        "displayName": "Name",
                        "type": "string",
                        "hidden": false
                    },
                    {
                        "key": "industry",
                        "displayName": "Industry",
                        "type": "enum",
                        "hidden": false,
                        "options": ["Technology", "Healthcare", "Legal"]
                    }
                ]
                """;

        testRunner.enqueue(content);
        testRunner.run();
        assertEquals(0, capturedOperations.size(), "Should not generate any operations when no changes needed");
        testRunner.assertAllFlowFilesTransferred(UpdateBoxMetadataTemplate.REL_SUCCESS, 1);
    }

    @Test
    public void testApiError() {
        testRunner.setProperty(UpdateBoxMetadataTemplate.TEMPLATE_KEY, "forbidden");

        final String content = """
                [
                    {
                        "key": "company_name",
                        "displayName": "Company Name",
                        "type": "string",
                        "hidden": false
                    }
                ]
                """;

        testRunner.enqueue(content);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(UpdateBoxMetadataTemplate.REL_FAILURE, 1);
    }
}
