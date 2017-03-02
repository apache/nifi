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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import static junit.framework.TestCase.fail;

public class TestConvertCharacterSet {

    @Test
    public void testSimple() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertCharacterSet());
        runner.setProperty(ConvertCharacterSet.INPUT_CHARSET, "ASCII");
        runner.setProperty(ConvertCharacterSet.OUTPUT_CHARSET, "UTF-32");

        runner.enqueue(Paths.get("src/test/resources/CharacterSetConversionSamples/Original.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertCharacterSet.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(ConvertCharacterSet.REL_SUCCESS).get(0);
        output.assertContentEquals(new File("src/test/resources/CharacterSetConversionSamples/Converted2.txt"));
    }

    @Test
    public void testExpressionLanguageInput() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertCharacterSet());
        runner.setProperty(ConvertCharacterSet.INPUT_CHARSET, "${characterSet}");
        runner.setProperty(ConvertCharacterSet.OUTPUT_CHARSET, "UTF-32");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("characterSet", "ASCII");
        runner.enqueue(Paths.get("src/test/resources/CharacterSetConversionSamples/Original.txt"),attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertCharacterSet.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(ConvertCharacterSet.REL_SUCCESS).get(0);
        output.assertContentEquals(new File("src/test/resources/CharacterSetConversionSamples/Converted2.txt"));
    }

    @Test
    public void testExpressionLanguageOutput() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertCharacterSet());
        runner.setProperty(ConvertCharacterSet.INPUT_CHARSET, "ASCII");
        runner.setProperty(ConvertCharacterSet.OUTPUT_CHARSET, "${characterSet}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("characterSet", "UTF-32");
        runner.enqueue(Paths.get("src/test/resources/CharacterSetConversionSamples/Original.txt"),attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertCharacterSet.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(ConvertCharacterSet.REL_SUCCESS).get(0);
        output.assertContentEquals(new File("src/test/resources/CharacterSetConversionSamples/Converted2.txt"));
    }

    @Test
    public void testExpressionLanguageConfig() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertCharacterSet());
        runner.setProperty(ConvertCharacterSet.INPUT_CHARSET, "${now()}");
        runner.setProperty(ConvertCharacterSet.OUTPUT_CHARSET, "UTF-32");

        runner.enqueue(Paths.get("src/test/resources/CharacterSetConversionSamples/Original.txt"));
        try {
            runner.run();
            fail("Should fail to validate config and fail to run the on trigger");
        } catch (AssertionError e){
            // Expect to fail assertion for passing a date to the character set validator
        }


        runner.setProperty(ConvertCharacterSet.INPUT_CHARSET, "UTF-32");
        runner.setProperty(ConvertCharacterSet.OUTPUT_CHARSET, "${anyAttribute(\"abc\", \"xyz\"):contains(\"bye\")}");

        runner.enqueue(Paths.get("src/test/resources/CharacterSetConversionSamples/Original.txt"));
        try {
            runner.run();
            fail("Should fail to validate config and fail to run the on trigger");
        } catch (AssertionError e) {
            // Expect to fail assertion for passing a boolean to the character set validator
        }
    }
}
