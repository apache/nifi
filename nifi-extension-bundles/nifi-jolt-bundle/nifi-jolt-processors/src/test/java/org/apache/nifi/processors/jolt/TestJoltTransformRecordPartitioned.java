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
package org.apache.nifi.processors.jolt;

import org.apache.nifi.jolt.util.JoltTransformStrategy;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@DisabledOnOs(OS.WINDOWS) //The pretty printed json comparisons don't work on windows
public class TestJoltTransformRecordPartitioned extends TestBaseJoltTransformRecord {

    @Override
    protected String getWritingStrategy() {
        return JoltTransformWritingStrategy.PARTITION_BY_SCHEMA.getValue();
    }

    @Test
    public void testTransformInputWithDifferentSchemas() throws InitializationException, IOException {
        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        runner.enableControllerService(reader);
        runner.setProperty(JoltTransformRecord.RECORD_READER, "reader");

        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);

        final String flattenSpec = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/multipleSchemasSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, flattenSpec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.CHAINR);

        final String inputJson = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/multipleSchemasInput.json"));
        runner.enqueue(inputJson);

        runner.run();

        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 2);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);

        final String expectedOutput1 = getExpectedContent("src/test/resources/TestJoltTransformRecord/multipleSchemasOutput1.json");
        final String expectedOutput2 = getExpectedContent("src/test/resources/TestJoltTransformRecord/multipleSchemasOutput2.json");

        final java.util.List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS);
        final MockFlowFile result1 = flowFiles.get(0);
        final MockFlowFile result2 = flowFiles.get(1);

        // Handles non-deterministic order
        if (result1.getContent().contains("TRASH")) {
            result1.assertContentEquals(expectedOutput1);
            result2.assertContentEquals(expectedOutput2);
        } else {
            result1.assertContentEquals(expectedOutput2);
            result2.assertContentEquals(expectedOutput1);
        }
    }
}
