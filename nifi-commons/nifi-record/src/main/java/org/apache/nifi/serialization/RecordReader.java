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

package org.apache.nifi.serialization;

import java.io.Closeable;
import java.io.IOException;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

/**
 * <p>
 * A RowRecordReader is responsible for parsing data and returning a record at a time
 * in order to allow the caller to iterate over the records individually.
 * </p>
 *
 * <p>
 * PLEASE NOTE: This interface is still considered 'unstable' and may change in a non-backward-compatible
 * manner between minor or incremental releases of NiFi.
 * </p>
 */
public interface RecordReader extends Closeable {

    /**
     * Returns the next record in the stream or <code>null</code> if no more records are available. Types will be coerced and any unknown fields will be dropped.
     *
     * @return the next record in the stream or <code>null</code> if no more records are available.
     *
     * @throws IOException if unable to read from the underlying data
     * @throws MalformedRecordException if an unrecoverable failure occurs when trying to parse a record
     * @throws SchemaValidationException if a Record contains a field that violates the schema and cannot be coerced into the appropriate field type.
     */
    default Record nextRecord() throws IOException, MalformedRecordException {
        return nextRecord(true, true);
    }

    /**
     * Reads the next record from the underlying stream. If type coercion is enabled, then any field in the Record whose type does not
     * match the schema will be coerced to the correct type and a MalformedRecordException will be thrown if unable to coerce the data into
     * the correct type. If type coercion is disabled, then no type coercion will occur. As a result, calling
     * {@link Record#getValue(org.apache.nifi.serialization.record.RecordField)}
     * may return any type of Object, such as a String or another Record, even though the schema indicates that the field must be an integer.
     *
     * @param coerceTypes whether or not fields in the Record should be validated against the schema and coerced when necessary
     * @param dropUnknownFields if <code>true</code>, any field that is found in the data that is not present in the schema will be dropped. If <code>false</code>,
     *            those fields will still be part of the Record (though their type cannot be coerced, since the schema does not provide a type for it).
     *
     * @return the next record in the stream or <code>null</code> if no more records are available
     * @throws IOException if unable to read from the underlying data
     * @throws MalformedRecordException if an unrecoverable failure occurs when trying to parse a record, or a Record contains a field
     *             that violates the schema and cannot be coerced into the appropriate field type.
     * @throws SchemaValidationException if a Record contains a field that violates the schema and cannot be coerced into the appropriate
     *             field type and schema enforcement is enabled
     */
    Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException;

    /**
     * @return a RecordSchema that is appropriate for the records in the stream
     * @throws MalformedRecordException if an unrecoverable failure occurs when trying to parse the underlying data
     */
    RecordSchema getSchema() throws MalformedRecordException;

    /**
     * @return a RecordSet that returns the records in this Record Reader in a streaming fashion
     */
    default RecordSet createRecordSet() {
        return new RecordSet() {
            @Override
            public RecordSchema getSchema() throws IOException {
                try {
                    return RecordReader.this.getSchema();
                } catch (final MalformedRecordException mre) {
                    throw new IOException(mre);
                }
            }

            @Override
            public Record next() throws IOException {
                try {
                    return RecordReader.this.nextRecord();
                } catch (final MalformedRecordException mre) {
                    throw new IOException(mre);
                }
            }
        };
    }
}
