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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.remote.protocol.DataPacket;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * DTO object for serializing and deserializing DataPackets via JSON
 */
public class DataPacketDto {
    public static final TypeReference<DataPacketDto> DATA_PACKET_DTO_TYPE_REFERENCE = new TypeReference<DataPacketDto>() {
    };
    private Map<String, String> attributes;
    private byte[] data;
    private String dataFile;

    public DataPacketDto() {
        this(null);
    }

    public DataPacketDto(byte[] data) {
        this(new HashMap<>(), data);
    }

    public DataPacketDto(Map<String, String> attributes, byte[] data) {
        this(attributes, data, null);
    }

    public DataPacketDto(Map<String, String> attributes, String dataFile) {
        this(attributes, null, dataFile);
    }

    public DataPacketDto(Map<String, String> attributes, byte[] data, String dataFile) {
        this.attributes = attributes;
        this.data = data;
        this.dataFile = dataFile;
    }

    public static Stream<DataPacket> getDataPacketStream(InputStream inputStream) throws IOException {
        JsonParser jsonParser = new JsonFactory().createParser(inputStream);
        if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
            throw new IOException("Expecting start array token to begin object array.");
        }
        jsonParser.setCodec(new ObjectMapper());
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<DataPacket>() {
            DataPacket next = getNext();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public DataPacket next() {
                DataPacket next = this.next;
                this.next = getNext();
                return next;
            }

            DataPacket getNext() throws RuntimeException {
                try {
                    if (jsonParser.nextToken() == JsonToken.END_ARRAY) {
                        return null;
                    }
                    DataPacketDto dataPacketDto = jsonParser.readValueAs(DATA_PACKET_DTO_TYPE_REFERENCE);
                    return dataPacketDto.toDataPacket();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, Spliterator.ORDERED), false);
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getDataFile() {
        return dataFile;
    }

    public void setDataFile(String dataFile) {
        this.dataFile = dataFile;
    }

    public DataPacket toDataPacket() {
        return new DataPacketImpl(attributes, data, dataFile);
    }

    public DataPacketDto putAttribute(String key, String value) {
        attributes.put(key, value);
        return this;
    }
}
