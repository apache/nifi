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

package org.apache.nifi.serialization.record;

import java.io.IOException;

public interface RecordSet {

    /**
     * @return the {@link RecordSchema} that applies to the records in this RecordSet
     */
    RecordSchema getSchema() throws IOException;

    /**
     * @return the next {@link Record} in the set or <code>null</code> if there are no more records
     */
    Record next() throws IOException;

    /**
     * Returns a new Record Set that will return no more than {@code maxRecords} records from this
     * RecordSet. Any Records that are pulled from this newly created RecordSet will also advance
     * the cursor in this Record Set and vice versa.
     *
     * @param maxRecords the maximum number of records to return from the new RecordSet
     * @return a view of this RecordSet that limits the number of records returned
     */
    default RecordSet limit(final int maxRecords) {
        if (maxRecords < 0) {
            throw new IllegalArgumentException("Cannot limit number of records to " + maxRecords + ". Limit must be a non-negative integer");
        }

        final RecordSet original = this;
        return new RecordSet() {
            private int count = 0;

            @Override
            public RecordSchema getSchema() throws IOException {
                return original.getSchema();
            }

            @Override
            public Record next() throws IOException {
                if (count >= maxRecords) {
                    return null;
                }

                final Record record = original.next();
                if (record != null) {
                    count++;
                }

                return record;
            }
        };
    }

    public static RecordSet of(final RecordSchema schema, final Record... records) {
        return new RecordSet() {
            private int index = 0;

            @Override
            public RecordSchema getSchema() {
                return schema;
            }

            @Override
            public Record next() {
                if (index >= records.length) {
                    return null;
                }

                return records[index++];
            }
        };
    }
}
