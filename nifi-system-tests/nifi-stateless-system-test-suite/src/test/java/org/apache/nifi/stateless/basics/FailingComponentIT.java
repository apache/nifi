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

package org.apache.nifi.stateless.basics;

import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.FailingComponent;
import org.apache.nifi.stateless.flow.FailurePortEncounteredException;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FailingComponentIT extends StatelessSystemIT {
    private static final String EXCEPTION_TEXT = "Intentional Exception to verify FailingComponentIT";

    @TempDir
    private Path tempDir;

    @Test
    public void testExceptionAttributedToThrowingProcessor() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessGroup group = builder.createProcessGroup("Inner");
        final VersionedProcessor generate = builder.createSimpleProcessor("GenerateFlowFile", group);
        final VersionedProcessor setAttribute = builder.createSimpleProcessor("SetAttribute", group);
        final VersionedProcessor throwException = builder.createSimpleProcessor("ThrowProcessException", group);
        throwException.setProperties(Collections.singletonMap("Text", EXCEPTION_TEXT));

        builder.createConnection(generate, setAttribute, "success", group);
        builder.createConnection(setAttribute, throwException, "success", group);

        final TriggerResult result = loadDataflow(builder.getFlowSnapshot()).trigger().getResult();
        assertFalse(result.isSuccessful());
        assertInstanceOf(ProcessException.class, result.getFailureCause().orElseThrow());

        final FailingComponent component = result.getFailingComponent().orElseThrow();
        assertNotNull(component.id());
        assertEquals(Optional.of(throwException.getIdentifier()), component.versionedId());
        assertEquals("ThrowProcessException", component.name());
        assertEquals("org.apache.nifi.processors.tests.system.ThrowProcessException", component.type());
        assertNotNull(component.groupId());
        assertEquals("Inner", component.groupName());
    }

    @Test
    public void testSynchronousCommitAttributesFailureToThrowingProcessor() throws IOException, StatelessConfigurationException, InterruptedException {
        final File inputFile = tempDir.resolve("input.txt").toFile();
        Files.writeString(inputFile.toPath(), "Hello World", StandardCharsets.UTF_8);

        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor ingestFile = builder.createSimpleProcessor("IngestFile");
        ingestFile.setProperties(Map.of(
            "Filename", inputFile.getAbsolutePath(),
            "Commit Mode", "synchronous",
            "Delete File", "false"));

        final VersionedProcessor throwException = builder.createSimpleProcessor("ThrowProcessException");
        throwException.setProperties(Collections.singletonMap("Text", EXCEPTION_TEXT));
        builder.createConnection(ingestFile, throwException, "success");

        final TriggerResult result = loadDataflow(builder.getFlowSnapshot()).trigger().getResult();
        assertFalse(result.isSuccessful());

        // The Exception propagates through IngestFile's synchronous commit, but IngestFile did not fail
        assertEquals("ThrowProcessException", result.getFailingComponent().orElseThrow().name());
    }

    @Test
    public void testNoFailingComponentWhenFailurePortReached() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor generate = builder.createSimpleProcessor("GenerateFlowFile");
        final VersionedProcessor setAttribute = builder.createSimpleProcessor("SetAttribute");
        final VersionedPort out = builder.createOutputPort("Out");

        builder.createConnection(generate, setAttribute, "success");
        builder.createConnection(setAttribute, out, "success");

        final StatelessDataflow dataflow = loadDataflow(builder.getFlowSnapshot(), Collections.emptyList(), Collections.singleton("Out"));
        final TriggerResult result = dataflow.trigger().getResult();
        assertFalse(result.isSuccessful());
        assertInstanceOf(FailurePortEncounteredException.class, result.getFailureCause().orElseThrow());
        assertTrue(result.getFailingComponent().isEmpty());
    }

    @Test
    public void testNoFailingComponentWhenSuccessful() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor generate = builder.createSimpleProcessor("GenerateFlowFile");
        final VersionedPort out = builder.createOutputPort("Out");
        builder.createConnection(generate, out, "success");

        final TriggerResult result = loadDataflow(builder.getFlowSnapshot()).trigger().getResult();
        assertTrue(result.isSuccessful());
        assertTrue(result.getFailingComponent().isEmpty());
        result.acknowledge();
    }
}
