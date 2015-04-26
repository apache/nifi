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
package org.wali;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * A mechanism for Serializing and De-Serializing a Record of a given Type
 *
 * @param <T> the type of record that is to be Serialized and De-Serialized by
 * this object
 */
public interface SerDe<T> {

    /**
     * <p>
     * Serializes an Edit Record to the log via the given
     * {@link DataOutputStream}.
     * </p>
     *
     * @param previousRecordState previous state
     * @param newRecordState new state
     * @param out stream to write to
     * @throws IOException if fail during write
     */
    void serializeEdit(T previousRecordState, T newRecordState, DataOutputStream out) throws IOException;

    /**
     * <p>
     * Serializes a Record in a form suitable for a Snapshot via the given
     * {@link DataOutputStream}.
     * </p>
     *
     * @param record to serialize
     * @param out to write to
     * @throws IOException if failed to write
     */
    void serializeRecord(T record, DataOutputStream out) throws IOException;

    /**
     * <p>
     * Reads an Edit Record from the given {@link DataInputStream} and merges
     * that edit with the current version of the record, returning the new,
     * merged version. If the Edit Record indicates that the entity was deleted,
     * must return a Record with an UpdateType of {@link UpdateType#DELETE}.
     * This method must never return <code>null</code>.
     * </p>
     *
     * @param in to deserialize from
     * @param currentRecordStates an unmodifiable map of Record ID's to the
     * current state of that record
     * @param version the version of the SerDe that was used to serialize the
     * edit record
     * @return deserialized record
     * @throws IOException if failure reading
     */
    T deserializeEdit(DataInputStream in, Map<Object, T> currentRecordStates, int version) throws IOException;

    /**
     * <p>
     * Reads a Record from the given {@link DataInputStream} and returns this
     * record. If no data is available, returns <code>null</code>.
     * </p>
     *
     * @param in stream to read from
     * @param version the version of the SerDe that was used to serialize the
     * record
     * @return record
     * @throws IOException failure reading
     */
    T deserializeRecord(DataInputStream in, int version) throws IOException;

    /**
     * Returns the unique ID for the given record
     *
     * @param record to obtain identifier for
     * @return identifier of record
     */
    Object getRecordIdentifier(T record);

    /**
     * Returns the UpdateType for the given record
     *
     * @param record to retrieve update type for
     * @return update type
     */
    UpdateType getUpdateType(T record);

    /**
     * Returns the external location of the given record; this is used when a
     * record is moved away from WALI or is being re-introduced to WALI. For
     * example, WALI can be updated with a record of type
     * {@link UpdateType#SWAP_OUT} that indicates a Location of
     * file://tmp/external1 and can then be re-introduced to WALI by updating
     * WALI with a record of type {@link UpdateType#CREATE} that indicates a
     * Location of file://tmp/external1
     *
     * @param record to get location of
     * @return location
     */
    String getLocation(T record);

    /**
     * Returns the version that this SerDe will use when writing. This used used
     * when serializing/deserializing the edit logs so that if the version
     * changes, we are still able to deserialize old versions
     *
     * @return version
     */
    int getVersion();
}
