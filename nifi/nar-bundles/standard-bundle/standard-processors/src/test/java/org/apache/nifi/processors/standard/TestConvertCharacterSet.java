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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestConvertCharacterSet {

    @Test
    public void test() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertCharacterSet());
        runner.setProperty(ConvertCharacterSet.INPUT_CHARSET, "ASCII");
        runner.setProperty(ConvertCharacterSet.OUTPUT_CHARSET, "UTF-32");

        runner.enqueue(Paths.get("src/test/resources/CharacterSetConversionSamples/Original.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertCharacterSet.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(ConvertCharacterSet.REL_SUCCESS).get(0);
        output.assertContentEquals(new File("src/test/resources/CharacterSetConversionSamples/Converted2.txt"));
    }

}
