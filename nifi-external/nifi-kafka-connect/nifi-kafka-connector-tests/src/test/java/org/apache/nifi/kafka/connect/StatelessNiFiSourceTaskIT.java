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
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StatelessNiFiSourceTaskIT {

    @Test
    public void testSimpleFlow() throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        final Map<String, String> properties = createDefaultProperties();
        sourceTask.start(properties);

        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(1, sourceRecords.size());

        final SourceRecord record = sourceRecords.get(0);
        assertEquals("Hello World", new String((byte[]) record.value()));
        assertNull(record.key());
        assertEquals("my-topic", record.topic());

        sourceTask.stop();
    }

    @Test
    public void testKeyAttribute() throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        final Map<String, String> properties = createDefaultProperties();
        properties.put(StatelessNiFiSourceConnector.KEY_ATTRIBUTE, "greeting");
        sourceTask.start(properties);

        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(1, sourceRecords.size());

        final SourceRecord record = sourceRecords.get(0);
        final Object key = record.key();
        assertEquals("hello", key);
        assertEquals("my-topic", record.topic());

        sourceTask.stop();
    }

    @Test
    public void testTopicNameAttribute() throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        final Map<String, String> properties = createDefaultProperties();
        properties.put(StatelessNiFiSourceConnector.TOPIC_NAME_ATTRIBUTE, "greeting");
        sourceTask.start(properties);

        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(1, sourceRecords.size());

        final SourceRecord record = sourceRecords.get(0);
        assertEquals("hello", record.topic());

        sourceTask.stop();
    }

    @Test
    public void testHeaders() throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        final Map<String, String> properties = createDefaultProperties();
        properties.put(StatelessNiFiSourceConnector.HEADER_REGEX, "uuid|greeting|num.*");
        sourceTask.start(properties);

        final List<SourceRecord> sourceRecords = sourceTask.poll();
        assertEquals(1, sourceRecords.size());

        final SourceRecord record = sourceRecords.get(0);
        assertEquals("my-topic", record.topic());

        final Map<String, String> headerValues = new HashMap<>();
        final Headers headers = record.headers();
        for (final Header header : headers) {
            headerValues.put(header.key(), (String) header.value());
        }

        assertEquals("hello", headerValues.get("greeting"));
        assertTrue(headerValues.containsKey("uuid"));
        assertTrue(headerValues.containsKey("number"));

        sourceTask.stop();
    }

    @Test
    public void testTransferToWrongPort() throws InterruptedException {
        final StatelessNiFiSourceTask sourceTask = new StatelessNiFiSourceTask();
        final Map<String, String> properties = createDefaultProperties();
        properties.put(StatelessNiFiSourceConnector.OUTPUT_PORT_NAME, "Another");
        sourceTask.start(properties);

        try {
            sourceTask.poll();
            Assert.fail("Expected RetriableException to be thrown");
        } catch (final RetriableException re) {
            // Expected
        }
    }


    private Map<String, String> createDefaultProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(StatelessKafkaConnectorUtil.DATAFLOW_TIMEOUT, "30 sec");
        properties.put(StatelessNiFiSourceConnector.OUTPUT_PORT_NAME, "Out");
        properties.put(StatelessNiFiSourceConnector.TOPIC_NAME, "my-topic");
        properties.put(StatelessNiFiSourceConnector.KEY_ATTRIBUTE, "kafka.key");
        properties.put(StatelessKafkaConnectorUtil.FLOW_SNAPSHOT, "src/test/resources/flows/Generate_Data.json");
        properties.put(StatelessKafkaConnectorUtil.NAR_DIRECTORY, "target/nifi-kafka-connector-bin/nars");
        properties.put(StatelessKafkaConnectorUtil.WORKING_DIRECTORY, "target/nifi-kafka-connector-bin/working");
        return properties;
    }
}
