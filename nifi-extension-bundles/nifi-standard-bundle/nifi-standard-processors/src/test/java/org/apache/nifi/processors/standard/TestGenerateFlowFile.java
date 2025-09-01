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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Unit tests for the GenerateFlowFile processor.
 */
public class TestGenerateFlowFile {

    @Test
    public void testGenerateCustomText() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "100MB");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.CUSTOM_TEXT, "This is my custom text!");

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        MockFlowFile generatedFlowFile = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0);
        generatedFlowFile.assertContentEquals("This is my custom text!");
        generatedFlowFile.assertAttributeNotExists("mime.type");
    }

    @Test
    public void testInvalidCustomText() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "100MB");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_BINARY);
        runner.setProperty(GenerateFlowFile.CUSTOM_TEXT, "This is my custom text!");
        runner.assertNotValid();

        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.UNIQUE_FLOWFILES, "true");
        runner.assertNotValid();
    }

    @Test
    public void testDynamicPropertiesToAttributes() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "1B");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.MIME_TYPE, "application/text");
        runner.setProperty("plain.dynamic.property", "Plain Value");
        runner.setProperty("expression.dynamic.property", "${literal('Expression Value')}");
        runner.assertValid();

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        MockFlowFile generatedFlowFile = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0);
        generatedFlowFile.assertAttributeEquals("plain.dynamic.property", "Plain Value");
        generatedFlowFile.assertAttributeEquals("expression.dynamic.property", "Expression Value");
        generatedFlowFile.assertAttributeEquals("mime.type", "application/text");
    }

    @Test
    public void testContextParametersToAttributes() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setParameterContextValue("context.parameter.property", "context.parameter.value");
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "1B");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.MIME_TYPE, "application/text");
        runner.setProperty("expression.context.parameter", "#{context.parameter.property}");
        runner.assertValid();

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        MockFlowFile generatedFlowFile = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0);
        generatedFlowFile.assertAttributeEquals("expression.context.parameter", "context.parameter.value");
        generatedFlowFile.assertAttributeEquals("mime.type", "application/text");
    }


    @Test
    public void testExpressionLanguageSupport() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
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


}