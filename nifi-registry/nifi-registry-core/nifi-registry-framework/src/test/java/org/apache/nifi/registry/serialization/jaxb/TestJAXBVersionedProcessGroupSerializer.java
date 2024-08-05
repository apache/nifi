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
package org.apache.nifi.registry.serialization.jaxb;

import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.registry.serialization.SerializationException;
import org.apache.nifi.registry.serialization.VersionedSerializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJAXBVersionedProcessGroupSerializer {

    @Test
    public void testSerializeDeserializeFlowSnapshot() throws SerializationException {
        final VersionedSerializer<VersionedProcessGroup> serializer = new JAXBVersionedProcessGroupSerializer();

        final VersionedProcessGroup processGroup1 = new VersionedProcessGroup();
        processGroup1.setIdentifier("pg1");
        processGroup1.setName("My Process Group");

        final VersionedProcessor processor1 = new VersionedProcessor();
        processor1.setIdentifier("processor1");
        processor1.setName("My Processor 1");

        // make sure nested objects are serialized/deserialized
        processGroup1.getProcessors().add(processor1);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(1, processGroup1, out);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        in.mark(1024);
        final int version = serializer.readDataModelVersion(in);

        assertEquals(1, version);

        in.reset();
        final VersionedProcessGroup deserializedProcessGroup1 = serializer.deserialize(in);

        assertEquals(processGroup1.getIdentifier(), deserializedProcessGroup1.getIdentifier());
        assertEquals(processGroup1.getName(), deserializedProcessGroup1.getName());

        assertEquals(1, deserializedProcessGroup1.getProcessors().size());

        final VersionedProcessor deserializedProcessor1 = deserializedProcessGroup1.getProcessors().iterator().next();
        assertEquals(processor1.getIdentifier(), deserializedProcessor1.getIdentifier());
        assertEquals(processor1.getName(), deserializedProcessor1.getName());
    }

}
