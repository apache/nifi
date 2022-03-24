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

package org.apache.nifi.toolkit.s2s;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.remote.protocol.DataPacket;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DataPacketDtoTest {
    public static DataPacketDto create(byte[] data) {
        return new DataPacketDto(new HashMap<>(), data);
    }

    @Test
    public void testNoArgConstructor() {
        DataPacketDto dataPacketDto = new DataPacketDto();
        assertEquals(0, dataPacketDto.getAttributes().size());
        assertNull(dataPacketDto.getData());
    }

    @Test
    public void testGetSetAttributes() {
        DataPacketDto dataPacketDto = create(null);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        dataPacketDto.setAttributes(attributes);
        assertEquals(attributes, Collections.unmodifiableMap(dataPacketDto.getAttributes()));
    }

    @Test
    public void testDataFileConstructor() {
        String dataFile = "dataFile";
        assertEquals(dataFile, new DataPacketDto(null, dataFile).getDataFile());
    }

    @Test
    public void testParserNone() throws IOException {
        List<DataPacket> dataPackets = DataPacketDto.getDataPacketStream(new ByteArrayInputStream(("[]").getBytes(StandardCharsets.UTF_8))).collect(Collectors.toList());
        assertEquals(0, dataPackets.size());
    }

    @Test
    public void testParserSingle() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        StringBuilder stringBuilder = new StringBuilder("[");
        DataPacketDto dataPacketDto = new DataPacketDto("test data".getBytes(StandardCharsets.UTF_8)).putAttribute("key", "value");
        stringBuilder.append(objectMapper.writeValueAsString(dataPacketDto));
        stringBuilder.append("]");
        List<DataPacket> dataPackets = DataPacketDto.getDataPacketStream(new ByteArrayInputStream(stringBuilder.toString().getBytes(StandardCharsets.UTF_8))).collect(Collectors.toList());
        assertEquals(1, dataPackets.size());
        assertEquals(dataPacketDto.toDataPacket(), dataPackets.get(0));
    }

    @Test
    public void testParserMultiple() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        StringBuilder stringBuilder = new StringBuilder("[");
        DataPacketDto dataPacketDto = new DataPacketDto("test data".getBytes(StandardCharsets.UTF_8)).putAttribute("key", "value");
        stringBuilder.append(objectMapper.writeValueAsString(dataPacketDto));
        DataPacketDto dataPacketDto2 = new DataPacketDto("test data 2".getBytes(StandardCharsets.UTF_8)).putAttribute("key2", "value2");
        stringBuilder.append(",");
        stringBuilder.append(objectMapper.writeValueAsString(dataPacketDto2));
        stringBuilder.append("]");
        List<DataPacket> dataPackets = DataPacketDto.getDataPacketStream(new ByteArrayInputStream(stringBuilder.toString().getBytes(StandardCharsets.UTF_8))).collect(Collectors.toList());
        assertEquals(2, dataPackets.size());
        assertEquals(dataPacketDto.toDataPacket(), dataPackets.get(0));
        assertEquals(dataPacketDto2.toDataPacket(), dataPackets.get(1));
    }
}
