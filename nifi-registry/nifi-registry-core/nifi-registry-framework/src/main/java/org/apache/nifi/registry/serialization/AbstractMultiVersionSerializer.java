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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * A serializer for an entity of type T that maps a "version" of the data model to a serializer.
 * </p>
 *
 * <p>
 * When serializing, the serializer associated with the {@link #getCurrentDataModelVersion()} is used.
 * The version will be written as a header at the beginning of the OutputStream then followed by the content.
 * </p>
 *
 * <p>
 * When deserializing, each registered serializer will be asked to read a data model version number from the input stream
 * in descending version order until a version number is read successfully.
 *
 * Then the associated serializer to the read data model version is used to deserialize content back to the target object.
 * If no serializer can read the version, or no serializer is registered for the read version, then SerializationException is thrown.
 * </p>
 *
 */
public abstract class AbstractMultiVersionSerializer<T> implements Serializer<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMultiVersionSerializer.class);

    private final Map<Integer, VersionedSerializer<T>> serializersByVersion;
    private final VersionedSerializer<T> defaultSerializer;
    private final List<Integer> descendingVersions;
    public static final int MAX_HEADER_BYTES = 1024;

    public AbstractMultiVersionSerializer() {
        final Map<Integer, VersionedSerializer<T>> tempSerializers = createVersionedSerializers();
        this.serializersByVersion = Collections.unmodifiableMap(tempSerializers);
        this.defaultSerializer = tempSerializers.get(getCurrentDataModelVersion());

        final List<Integer> sortedVersions = new ArrayList<>(serializersByVersion.keySet());
        sortedVersions.sort(Collections.reverseOrder(Integer::compareTo));
        this.descendingVersions = sortedVersions;
    }

    /**
     * Called from default constructor to create the map from data model version to corresponding serializer.
     *
     * @return the map of versioned serializers
     */
    protected abstract Map<Integer, VersionedSerializer<T>> createVersionedSerializers();

    /**
     * @return the current data model version
     */
    protected abstract int getCurrentDataModelVersion();

    @Override
    public void serialize(final T entity, final OutputStream out) throws SerializationException {
        defaultSerializer.serialize(getCurrentDataModelVersion(), entity, out);
    }

    @Override
    public T deserialize(final InputStream input) throws SerializationException {

        final InputStream markSupportedInput = input.markSupported() ? input : new BufferedInputStream(input);

        // Mark the beginning of the stream.
        markSupportedInput.mark(MAX_HEADER_BYTES);

        // Applying each serializer
        for (int serializerVersion : descendingVersions) {
            final VersionedSerializer<T> serializer = serializersByVersion.get(serializerVersion);

            // Serializer version will not be the data model version always.
            // E.g. higher version of serializer can read the old data model version number if it has the same header structure,
            // but it does not mean the serializer is compatible with the old format.
            final int version;
            try {
                version = serializer.readDataModelVersion(markSupportedInput);
                if (!serializersByVersion.containsKey(version)) {
                    throw new SerializationException(String.format(
                            "Version %d was returned by %s, but no serializer is registered for that version.", version, serializer));
                }
            } catch (SerializationException e) {
                logger.debug("Deserialization failed with {}", serializer, e);
                continue;
            } finally {
                // Either when continue with the next serializer, or proceed deserialization with the corresponding serializer,
                // reset the stream position.
                try {
                    markSupportedInput.reset();
                } catch (IOException resetException) {
                    // Should not happen.
                    logger.error("Unable to reset the input stream.", resetException);
                }
            }

            return serializersByVersion.get(version).deserialize(markSupportedInput);
        }

        throw new SerializationException("Unable to find a serializer compatible with the input.");
    }

}
