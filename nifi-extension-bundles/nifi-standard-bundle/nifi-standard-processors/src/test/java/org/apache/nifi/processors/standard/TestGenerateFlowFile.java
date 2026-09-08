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
package org.apache.nifi.processors.standard;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the GenerateFlowFile processor.
 */
public class TestGenerateFlowFile {
    private static final String STREAMED_FILE_SIZE = "9 KB";
    private static final long STREAMED_FILE_SIZE_BYTES = 9 * 1024;

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(new GenerateFlowFile());
    }

    @Test
    public void testGenerateCustomText() {
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "1B");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.CUSTOM_TEXT, "This is my custom text!");

        runner.setProperty(GenerateFlowFile.BATCH_SIZE, "2");

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 2);
        runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0).assertContentEquals("This is my custom text!");
        runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(1).assertContentEquals("This is my custom text!");
        runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0).assertAttributeNotExists("mime.type");
    }

    @Test
    public void testInvalidCustomText() {
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "1B");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_BINARY);
        runner.setProperty(GenerateFlowFile.CUSTOM_TEXT, "This is my custom text!");
        runner.assertNotValid();

        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.UNIQUE_FLOWFILES, "true");
        runner.assertNotValid();
    }

    @Test
    public void testFileSizeLargerThanIntegerMaxIsInvalid() {
        // Configuration only — do not runner.run() with this size. GitHub runners cannot host a 3 GB FlowFile.
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "3 GB");

        runner.assertNotValid();
    }

    @Test
    public void testGenerateNonUniqueBinaryContentStreamed() {
        assertStreamedContent(false, GenerateFlowFile.DATA_FORMAT_BINARY);
    }

    @Test
    public void testGenerateNonUniqueTextContentStreamed() {
        assertStreamedContent(false, GenerateFlowFile.DATA_FORMAT_TEXT);
    }

    @Test
    public void testGenerateUniqueBinaryContentStreamed() {
        assertStreamedContent(true, GenerateFlowFile.DATA_FORMAT_BINARY);
    }

    @Test
    public void testGenerateUniqueTextContentStreamed() {
        assertStreamedContent(true, GenerateFlowFile.DATA_FORMAT_TEXT);
    }

    @Test
    public void testDynamicPropertiesToAttributes() {
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "1B");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.MIME_TYPE, "application/text");
        runner.setProperty("plain.dynamic.property", "Plain Value");
        runner.setProperty("expression.dynamic.property", "${literal('Expression Value')}");
        runner.assertValid();

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        MockFlowFile generatedFlowFile = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).getFirst();
        generatedFlowFile.assertAttributeEquals("plain.dynamic.property", "Plain Value");
        generatedFlowFile.assertAttributeEquals("expression.dynamic.property", "Expression Value");
        generatedFlowFile.assertAttributeEquals("mime.type", "application/text");
    }

    @Test
    public void testContextParametersToAttributes() {
        runner.setParameterContextValue("context.parameter.property", "context.parameter.value");
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "1B");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.MIME_TYPE, "application/text");
        runner.setProperty("expression.context.parameter", "#{context.parameter.property}");
        runner.assertValid();

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        MockFlowFile generatedFlowFile = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).getFirst();
        generatedFlowFile.assertAttributeEquals("expression.context.parameter", "context.parameter.value");
        generatedFlowFile.assertAttributeEquals("mime.type", "application/text");
    }

    @Test
    public void testExpressionLanguageSupport() {
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "${nextInt()}B");
        runner.setProperty(GenerateFlowFile.UNIQUE_FLOWFILES, "true");
        runner.setProperty(GenerateFlowFile.BATCH_SIZE, "2");
        runner.assertValid();

        runner.run();

        // verify multiple files in a batch each have a unique file size based on the given Expression Language and uniqueness set to true
        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 2);
        assertTrue(runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0).getSize() < runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(1).getSize());
        runner.clearTransferState();

        runner.setProperty(GenerateFlowFile.UNIQUE_FLOWFILES, "false");
        runner.assertValid();

        runner.run();

        // verify multiple files in a batch each have the same file size when uniqueness is set to false
        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 2);
        assertEquals(runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0).getSize(), runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(1).getSize());
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.of(
                "generate-ff-custom-text", GenerateFlowFile.CUSTOM_TEXT.getName(),
                "character-set", GenerateFlowFile.CHARSET.getName(),
                "mime-type", GenerateFlowFile.MIME_TYPE.getName()
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private void assertStreamedContent(final boolean unique, final String dataFormat) {
        runner.setProperty(GenerateFlowFile.FILE_SIZE, STREAMED_FILE_SIZE);
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, dataFormat);
        runner.setProperty(GenerateFlowFile.UNIQUE_FLOWFILES, Boolean.toString(unique));
        runner.setProperty(GenerateFlowFile.BATCH_SIZE, unique ? "2" : "3");

        runner.run();

        runner.assertAllFlowFilesTransferred(GenerateFlowFile.SUCCESS, unique ? 2 : 3);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS);
        final MockFlowFile first = flowFiles.getFirst();
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals(STREAMED_FILE_SIZE_BYTES, flowFile.getSize());
        }

        if (unique) {
            assertFalse(Arrays.equals(first.getData(), flowFiles.get(1).getData()));
        } else {
            for (int i = 1; i < flowFiles.size(); i++) {
                assertArrayEquals(first.getData(), flowFiles.get(i).getData());
            }
        }
    }

}
