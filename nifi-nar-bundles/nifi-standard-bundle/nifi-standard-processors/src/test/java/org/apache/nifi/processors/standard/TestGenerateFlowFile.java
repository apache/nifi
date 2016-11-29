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

import java.io.IOException;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

/**
 * Unit tests for the GenerateFlowFile processor.
 */
public class TestGenerateFlowFile {

    @Test
    public void testGenerateCustomText() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "100MB");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.CUSTOM_TEXT, "This is my custom text!");

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0).assertContentEquals("This is my custom text!");
    }

    @Test
    public void testInvalidCustomText() throws IOException {
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
    public void testDynamicPropertiesToAttributes() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "1B");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty("plain.dynamic.property", "Plain Value");
        runner.setProperty("expression.dynamic.property", "${literal('Expression Value')}");
        runner.assertValid();

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        MockFlowFile generatedFlowFile = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0);
        generatedFlowFile.assertAttributeEquals("plain.dynamic.property", "Plain Value");
        generatedFlowFile.assertAttributeEquals("expression.dynamic.property", "Expression Value");
    }

}