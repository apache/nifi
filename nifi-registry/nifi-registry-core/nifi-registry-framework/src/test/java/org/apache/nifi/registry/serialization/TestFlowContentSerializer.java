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
package org.apache.nifi.registry.serialization;

import org.apache.nifi.registry.flow.ExternalControllerServiceReference;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

public class TestFlowContentSerializer {

    private FlowContentSerializer serializer;

    @Before
    public void setup() {
        serializer = new FlowContentSerializer();
    }

    @Test
    public void testSerializeDeserializeFlowContent() {
        final VersionedProcessor processor1 = new VersionedProcessor();
        processor1.setIdentifier("processor1");
        processor1.setName("My Processor 1");

        final VersionedProcessGroup processGroup1 = new VersionedProcessGroup();
        processGroup1.setIdentifier("pg1");
        processGroup1.setName("My Process Group");
        processGroup1.getProcessors().add(processor1);

        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setFlowContents(processGroup1);

        final FlowContent flowContent = new FlowContent();
        flowContent.setFlowSnapshot(snapshot);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serializeFlowContent(flowContent, out);

        //final String json = new String(out.toByteArray(), StandardCharsets.UTF_8);
        //System.out.println(json);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

        // make sure we can read the version from the input stream and it should be the current version
        final Integer version = serializer.readDataModelVersion(in);
        assertEquals(serializer.getCurrentDataModelVersion(), version);
        assertEquals(false, serializer.isProcessGroupVersion(version));

        // make sure we can deserialize back to FlowContent
        final FlowContent deserializedFlowContent = serializer.deserializeFlowContent(version, in);
        assertNotNull(deserializedFlowContent);

        final VersionedFlowSnapshot deserializedSnapshot = deserializedFlowContent.getFlowSnapshot();
        assertNotNull(deserializedSnapshot);

        final VersionedProcessGroup deserializedProcessGroup1 = deserializedSnapshot.getFlowContents();
        assertNotNull(deserializedProcessGroup1);
        assertEquals(processGroup1.getIdentifier(), deserializedProcessGroup1.getIdentifier());
        assertEquals(processGroup1.getName(), deserializedProcessGroup1.getName());

        assertEquals(1, deserializedProcessGroup1.getProcessors().size());

        final VersionedProcessor deserializedProcessor1 = deserializedProcessGroup1.getProcessors().iterator().next();
        assertEquals(processor1.getIdentifier(), deserializedProcessor1.getIdentifier());
        assertEquals(processor1.getName(), deserializedProcessor1.getName());
    }

    @Test
    public void testSerializeDeserializeWithExternalServices() throws SerializationException {
        final VersionedProcessGroup processGroup1 = new VersionedProcessGroup();
        processGroup1.setIdentifier("pg1");
        processGroup1.setName("My Process Group");

        final ExternalControllerServiceReference serviceReference1 = new ExternalControllerServiceReference();
        serviceReference1.setIdentifier("1");
        serviceReference1.setName("Service 1");

        final ExternalControllerServiceReference serviceReference2 = new ExternalControllerServiceReference();
        serviceReference2.setIdentifier("2");
        serviceReference2.setName("Service 2");

        final Map<String,ExternalControllerServiceReference> serviceReferences = new HashMap<>();
        serviceReferences.put(serviceReference1.getIdentifier(), serviceReference1);
        serviceReferences.put(serviceReference2.getIdentifier(), serviceReference2);

        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setFlowContents(processGroup1);
        snapshot.setExternalControllerServices(serviceReferences);

        final FlowContent flowContent = new FlowContent();
        flowContent.setFlowSnapshot(snapshot);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serializeFlowContent(flowContent, out);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

        // make sure we can read the version from the input stream and it should be the current version
        final Integer version = serializer.readDataModelVersion(in);
        assertEquals(serializer.getCurrentDataModelVersion(), version);

        // make sure we can deserialize back to FlowContent
        final FlowContent deserializedFlowContent = serializer.deserializeFlowContent(version, in);
        assertNotNull(deserializedFlowContent);

        final VersionedFlowSnapshot deserializedSnapshot = deserializedFlowContent.getFlowSnapshot();
        assertNotNull(deserializedSnapshot);

        final VersionedProcessGroup deserializedProcessGroup = deserializedSnapshot.getFlowContents();
        assertEquals(processGroup1.getIdentifier(), deserializedProcessGroup.getIdentifier());
        assertEquals(processGroup1.getName(), deserializedProcessGroup.getName());

        final Map<String,ExternalControllerServiceReference> deserializedServiceReferences = deserializedSnapshot.getExternalControllerServices();
        assertNotNull(deserializedServiceReferences);
        assertEquals(2, deserializedServiceReferences.size());

        final ExternalControllerServiceReference deserializedServiceReference1 = deserializedServiceReferences.get(serviceReference1.getIdentifier());
        assertNotNull(deserializedServiceReference1);
        assertEquals(serviceReference1.getIdentifier(), deserializedServiceReference1.getIdentifier());
        assertEquals(serviceReference1.getName(), deserializedServiceReference1.getName());
    }

    @Test
    public void testDeserializeJsonNonIntegerVersion() throws IOException {
        final String file = "/serialization/json/non-integer-version.snapshot";
        try (final InputStream is = this.getClass().getResourceAsStream(file)) {
            try {
                serializer.readDataModelVersion(is);
                fail("Should fail");
            } catch (SerializationException e) {
                assertEquals("Unable to read the data model version for the flow content.", e.getMessage());
            }
        }
    }

    @Test
    public void testDeserializeJsonNoVersion() throws IOException {
        final String file = "/serialization/json/no-version.snapshot";
        try (final InputStream is = this.getClass().getResourceAsStream(file)) {
            try {
                serializer.readDataModelVersion(is);
                fail("Should fail");
            } catch (SerializationException e) {
                assertEquals("Unable to read the data model version for the flow content.", e.getMessage());
            }
        }
    }

    @Test
    public void testDeserializeVer1() throws IOException {
        final String file = "/serialization/ver1.snapshot";
        final VersionedProcessGroup processGroup;
        try (final InputStream is = this.getClass().getResourceAsStream(file)) {
            final Integer version = serializer.readDataModelVersion(is);
            assertNotNull(version);
            assertEquals(1, version.intValue());

            if (serializer.isProcessGroupVersion(version)) {
                processGroup = serializer.deserializeProcessGroup(version, is);
            } else {
                processGroup = null;
            }
        }

        assertNotNull(processGroup);
        assertNotNull(processGroup.getProcessors());
        assertTrue(processGroup.getProcessors().size() > 0);
        //System.out.printf("processGroup=" + processGroup);
    }

    @Test
    public void testDeserializeVer2() throws IOException {
        final String file = "/serialization/ver2.snapshot";
        final VersionedProcessGroup processGroup;
        try (final InputStream is = this.getClass().getResourceAsStream(file)) {
            final Integer version = serializer.readDataModelVersion(is);
            assertNotNull(version);
            assertEquals(2, version.intValue());

            if (serializer.isProcessGroupVersion(version)) {
                processGroup = serializer.deserializeProcessGroup(version, is);
            } else {
                processGroup = null;
            }
        }

        assertNotNull(processGroup);
        assertNotNull(processGroup.getProcessors());
        assertTrue(processGroup.getProcessors().size() > 0);
        //System.out.printf("processGroup=" + processGroup);
    }

    @Test
    public void testDeserializeVer3() throws IOException {
        final String file = "/serialization/ver3.snapshot";
        try (final InputStream is = this.getClass().getResourceAsStream(file)) {
            final Integer version = serializer.readDataModelVersion(is);
            assertNotNull(version);
            assertEquals(3, version.intValue());
            assertFalse(serializer.isProcessGroupVersion(version));

            final FlowContent flowContent = serializer.deserializeFlowContent(version, is);
            assertNotNull(flowContent);

            final VersionedFlowSnapshot flowSnapshot = flowContent.getFlowSnapshot();
            assertNotNull(flowSnapshot);

            final VersionedProcessGroup processGroup = flowSnapshot.getFlowContents();
            assertNotNull(processGroup);
            assertNotNull(processGroup.getProcessors());
            assertEquals(1, processGroup.getProcessors().size());
        }
    }

    @Test
    public void testDeserializeVer9999() throws IOException {
        final String file = "/serialization/ver9999.snapshot";
        try (final InputStream is = this.getClass().getResourceAsStream(file)) {
            final Integer version = serializer.readDataModelVersion(is);
            assertNotNull(version);
            assertEquals(9999, version.intValue());
            assertFalse(serializer.isProcessGroupVersion(version));

            try {
                serializer.deserializeFlowContent(version, is);
                fail("Should fail");
            } catch (IllegalArgumentException e) {
                assertEquals("No FlowContent serializer exists for data model version: " + version, e.getMessage());
            }

            try {
                serializer.deserializeProcessGroup(version, is);
                fail("Should fail");
            } catch (IllegalArgumentException e) {
                assertEquals("No VersionedProcessGroup serializer exists for data model version: " + version, e.getMessage());
            }
        }
    }

    @Test
    public void testDeserializeProcessGroupAsFlowContent() throws IOException {
        final String file = "/serialization/ver2.snapshot";
        try (final InputStream is = this.getClass().getResourceAsStream(file)) {
            final Integer version = serializer.readDataModelVersion(is);
            assertNotNull(version);
            assertEquals(2, version.intValue());
            assertTrue(serializer.isProcessGroupVersion(version));

            final VersionedProcessGroup processGroup = serializer.deserializeProcessGroup(version, is);
            assertNotNull(processGroup);
        }
    }
}
