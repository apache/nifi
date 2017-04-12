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
package org.apache.nifi.processors.hadoop.record;

import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;

/**
 * Writes Records to HDFS.
 */
public interface HDFSRecordWriter extends Closeable {

    /**
     * @param record the record to write
     * @throws IOException if an I/O error happens writing the record
     */
    void write(Record record) throws IOException;

    /**
     * @param recordSet the RecordSet to write
     * @return the result of writing the record set
     * @throws IOException if an I/O error happens reading from the RecordSet, or writing a Record
     */
    default WriteResult write(final RecordSet recordSet) throws IOException {
        int recordCount = 0;

        Record record;
        while ((record = recordSet.next()) != null) {
            write(record);
            recordCount++;
        }

        return WriteResult.of(recordCount, Collections.emptyMap());
    }

}
