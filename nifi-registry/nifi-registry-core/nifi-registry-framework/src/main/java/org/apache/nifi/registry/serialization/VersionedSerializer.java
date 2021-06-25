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

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serializes and de-serializes objects.
 * This interface is designed to provide backward compatibility to different versioned serialization formats.
 * So that serialized data model and format can evolve overtime.
 */
public interface VersionedSerializer<T> {

    /**
     * Serialize the given object into the target output stream with the specified version format.
     * Implementation classes are responsible to serialize the version to the head of the serialized content,
     * so that it can be retrieved by {@link #readDataModelVersion(InputStream)} method efficiently
     * without reading the entire byte array.
     *
     * @param dataModelVersion the data model version
     * @param t the object to serialize
     * @param out the target output stream
     * @throws SerializationException thrown when serialization failed
     */
    void serialize(int dataModelVersion, T t, OutputStream out) throws SerializationException;

    /**
     * Read data model version from the given InputStream.
     * <p>
     * Even if an implementation serializer was able to read a version, it does not necessary mean
     * the same serializers {@link #deserialize(InputStream)} method will be called.
     * For example, when the header structure has not been changed, the newer version of serializer may be able to
     * read older data model version. But deserialization should be done with the older serializer.
     * </p>
     * @param input the input stream to read version from
     * @return the read data model version
     * @throws SerializationException thrown when reading version failed
     */
    int readDataModelVersion(InputStream input) throws SerializationException;

    /**
     * Deserializes the given InputStream back to an object of the given type.
     *
     * @param input the InputStream to deserialize,
     *              the position of input is reset to the the beginning of the stream when this method is called
     * @return the deserialized object
     * @throws SerializationException thrown when deserialization failed
     */
    T deserialize(InputStream input) throws SerializationException;
}
