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

import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.serialization.jackson.JacksonFlowContentSerializer;
import org.apache.nifi.registry.serialization.jackson.JacksonVersionedProcessGroupSerializer;
import org.apache.nifi.registry.serialization.jaxb.JAXBVersionedProcessGroupSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializer that handles versioned serialization for flow content.
 *
 * <p>
 * Current data model version is 3.
 * Data Model Version Histories:
 * <ul>
 *     <li>version 3: Serialized by {@link JacksonFlowContentSerializer}</li>
 *     <li>version 2: Serialized by {@link JacksonVersionedProcessGroupSerializer}</li>
 *     <li>version 1: Serialized by {@link JAXBVersionedProcessGroupSerializer}</li>
 * </ul>
 * </p>
 */
@Service
public class FlowContentSerializer {

    private static final Logger logger = LoggerFactory.getLogger(FlowContentSerializer.class);

    static final Integer START_USING_SNAPSHOT_VERSION = 3;
    static final Integer CURRENT_DATA_MODEL_VERSION = 3;

    private final Map<Integer, VersionedSerializer<VersionedProcessGroup>> processGroupSerializers;
    private final Map<Integer, VersionedSerializer<FlowContent>> flowContentSerializers;
    private final Map<Integer, VersionedSerializer<?>> allSerializers;

    private final List<Integer> descendingVersions;

    public FlowContentSerializer() {
        final Map<Integer, VersionedSerializer<FlowContent>> tempFlowContentSerializers = new HashMap<>();
        tempFlowContentSerializers.put(3, new JacksonFlowContentSerializer());
        flowContentSerializers = Collections.unmodifiableMap(tempFlowContentSerializers);

        final Map<Integer, VersionedSerializer<VersionedProcessGroup>> tempProcessGroupSerializers = new HashMap<>();
        tempProcessGroupSerializers.put(2, new JacksonVersionedProcessGroupSerializer());
        tempProcessGroupSerializers.put(1, new JAXBVersionedProcessGroupSerializer());
        processGroupSerializers = Collections.unmodifiableMap(tempProcessGroupSerializers);

        final Map<Integer,VersionedSerializer<?>> tempAllSerializers = new HashMap<>();
        tempAllSerializers.putAll(processGroupSerializers);
        tempAllSerializers.putAll(flowContentSerializers);
        allSerializers = Collections.unmodifiableMap(tempAllSerializers);

        final List<Integer> sortedVersions = new ArrayList<>(allSerializers.keySet());
        sortedVersions.sort(Collections.reverseOrder(Integer::compareTo));
        this.descendingVersions = sortedVersions;
    }

    /**
     * Tries to read a data model version using each VersionedSerializer, in descending version order.
     * If no version could be read from any serializer, then a SerializationException is thrown.
     *
     * When deserializing, clients are expected to call this method to obtain the version, then call
     * {@method isProcessGroupVersion}, which then determines if {@method deserializeProcessGroup}
     * should be used, or if {@method deserializeFlowContent} should be used.
     *
     * @param input the input stream containing serialized flow content
     * @return the data model version from the input stream
     * @throws SerializationException if the data model version could not be read with any serializer
     */
    public int readDataModelVersion(final InputStream input) throws SerializationException {
        final InputStream markSupportedInput = input.markSupported() ? input : new BufferedInputStream(input);

        // Mark the beginning of the stream.
        markSupportedInput.mark(SerializationConstants.MAX_HEADER_BYTES);

        // Try each serializer in descending version order
        for (final int serializerVersion : descendingVersions) {
            final VersionedSerializer<?> serializer = allSerializers.get(serializerVersion);
            try {
                return serializer.readDataModelVersion(markSupportedInput);
            } catch (SerializationException e) {
                if (logger.isDebugEnabled()) {
                    logger.error("Unable to read the data model version due to: {}", e.getMessage());
                }
                continue;
            } finally {
                // Reset the stream position.
                try {
                    markSupportedInput.reset();
                } catch (IOException resetException) {
                    // Should not happen.
                    logger.error("Unable to reset the input stream.", resetException);
                }
            }
        }

        throw new SerializationException("Unable to read the data model version for the flow content.");
    }

    public Integer getCurrentDataModelVersion() {
        return CURRENT_DATA_MODEL_VERSION;
    }

    public boolean isProcessGroupVersion(final int dataModelVersion) {
        return dataModelVersion < START_USING_SNAPSHOT_VERSION;
    }

    public VersionedProcessGroup deserializeProcessGroup(final int dataModelVersion, final InputStream input) throws SerializationException {
        final VersionedSerializer<VersionedProcessGroup> serializer = processGroupSerializers.get(dataModelVersion);
        if (serializer == null) {
            throw new IllegalArgumentException("No VersionedProcessGroup serializer exists for data model version: " + dataModelVersion);
        }

        return serializer.deserialize(input);
    }

    public FlowContent deserializeFlowContent(final int dataModelVersion, final InputStream input) throws SerializationException {
        final VersionedSerializer<FlowContent> serializer = flowContentSerializers.get(dataModelVersion);
        if (serializer == null) {
            throw new IllegalArgumentException("No FlowContent serializer exists for data model version: " + dataModelVersion);
        }

        return serializer.deserialize(input);
    }

    public void serializeFlowContent(final FlowContent flowContent, final OutputStream out) throws SerializationException {
        final VersionedSerializer<FlowContent> serializer = flowContentSerializers.get(CURRENT_DATA_MODEL_VERSION);
        serializer.serialize(CURRENT_DATA_MODEL_VERSION, flowContent, out);
    }
}
