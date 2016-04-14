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

import com.bazaarvoice.jolt.Diffy;
import com.bazaarvoice.jolt.JsonUtils;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;


public class TestTransformJSON {

    final static Path JSON_INPUT = Paths.get("src/test/resources/TestTransformJSON/input.json");
    final static Diffy DIFFY = new Diffy();

    @Test
    public void testInvalidJOLTSpec() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = "[{}]";
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.assertNotValid();
    }

    @Test
    public void testIncorrectJOLTSpec() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJSON/chainrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, chainrSpec);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testInvalidFlowFileContent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJSON/chainrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.enqueue("invalid json");
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_FAILURE);
    }

    @Test
    public void testTransformInputWithChainr() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJSON/chainrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJSON/chainrOutput.json")));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    @Test
    public void testTransformInputWithShiftr() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJSON/shiftrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.SHIFTR);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJSON/shiftrOutput.json")));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    @Test
    public void testTransformInputWithDefaultr() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJSON/defaultrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.DEFAULTR);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJSON/defaultrOutput.json")));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    @Test
    public void testTransformInputWithRemovr() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJSON/removrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.REMOVR);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJSON/removrOutput.json")));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

}
