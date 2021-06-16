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

package org.apache.nifi.kafka.connect;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StatelessNiFiSinkTaskIT {
    private final File DEFAULT_OUTPUT_DIRECTORY = new File("target/sink-output");

    @Rule
    public final TestName testName = new TestName();

    @Test
    public void testSimpleFlow() throws IOException {
        final StatelessNiFiSinkTask sinkTask = new StatelessNiFiSinkTask();
        sinkTask.initialize(Mockito.mock(SinkTaskContext.class));

        final Map<String, String> properties = createDefaultProperties();
        sinkTask.start(properties);

        final SinkRecord record = new SinkRecord("topic", 0, null, "key", null, "Hello World", 0L);

        final File[] files = DEFAULT_OUTPUT_DIRECTORY.listFiles();
        if (files != null) {
            for (final File file : files) {
                assertTrue("Failed to delete existing file " + file.getAbsolutePath(), file.delete());
            }
        }

        sinkTask.put(Collections.singleton(record));
        sinkTask.flush(Collections.emptyMap());

        final File[] outputFiles = DEFAULT_OUTPUT_DIRECTORY.listFiles();
        assertNotNull(outputFiles);
        assertEquals(1, outputFiles.length);
        final File outputFile = outputFiles[0];

        final String output = new String(Files.readAllBytes(outputFile.toPath()));
        assertEquals("Hello World", output);

        sinkTask.stop();
    }

    @Test
    public void testParameters() throws IOException {
        final StatelessNiFiSinkTask sinkTask = new StatelessNiFiSinkTask();
        sinkTask.initialize(Mockito.mock(SinkTaskContext.class));

        final Map<String, String> properties = createDefaultProperties();
        properties.put("parameter.Directory", "target/sink-output-2");
        sinkTask.start(properties);

        final SinkRecord record = new SinkRecord("topic", 0, null, "key", null, "Hello World", 0L);

        final File outputDir = new File("target/sink-output-2");
        final File[] files = outputDir.listFiles();
        if (files != null) {
            for (final File file : files) {
                assertTrue("Failed to delete existing file " + file.getAbsolutePath(), file.delete());
            }
        }

        sinkTask.put(Collections.singleton(record));
        sinkTask.flush(Collections.emptyMap());

        final File[] outputFiles = outputDir.listFiles();
        assertNotNull(outputFiles);
        assertEquals(1, outputFiles.length);
        final File outputFile = outputFiles[0];

        final String output = new String(Files.readAllBytes(outputFile.toPath()));
        assertEquals("Hello World", output);

        sinkTask.stop();
    }

    @Test
    public void testWrongOutputPort() {
        final StatelessNiFiSinkTask sinkTask = new StatelessNiFiSinkTask();
        sinkTask.initialize(Mockito.mock(SinkTaskContext.class));

        final Map<String, String> properties = createDefaultProperties();
        properties.put(StatelessNiFiSinkConnector.FAILURE_PORTS, "Success, Failure");
        sinkTask.start(properties);

        final SinkRecord record = new SinkRecord("topic", 0, null, "key", null, "Hello World", 0L);

        final File[] files = DEFAULT_OUTPUT_DIRECTORY.listFiles();
        if (files != null) {
            for (final File file : files) {
                assertTrue("Failed to delete existing file " + file.getAbsolutePath(), file.delete());
            }
        }

        try {
            sinkTask.put(Collections.singleton(record));
            sinkTask.flush(Collections.emptyMap());
            Assert.fail("Expected RetriableException to be thrown");
        } catch (final RetriableException re) {
            // Expected
        }
    }

    private Map<String, String> createDefaultProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(StatelessKafkaConnectorUtil.DATAFLOW_TIMEOUT, "30 sec");
        properties.put(StatelessNiFiSinkConnector.INPUT_PORT_NAME, "In");
        properties.put(StatelessKafkaConnectorUtil.FLOW_SNAPSHOT, "src/test/resources/flows/Write_To_File.json");
        properties.put(StatelessKafkaConnectorUtil.NAR_DIRECTORY, "target/nifi-kafka-connector-bin/nars");
        properties.put(StatelessKafkaConnectorUtil.WORKING_DIRECTORY, "target/nifi-kafka-connector-bin/working");
        properties.put(StatelessKafkaConnectorUtil.DATAFLOW_NAME, testName.getMethodName());
        return properties;
    }

}
