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
package org.apache.nifi.provenance.serialization;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.nifi.provenance.ProvenanceEventRecord;

public interface RecordWriter extends Closeable {

    /**
     * Writes header information to the underlying stream
     *
     * @throws IOException
     */
    void writeHeader() throws IOException;

    /**
     * Writes the given record out to the underlying stream
     *
     * @param record
     * @param recordIdentifier
     * @return the number of bytes written for the given records
     * @throws IOException
     */
    long writeRecord(ProvenanceEventRecord record, long recordIdentifier) throws IOException;

    /**
     * Returns the number of Records that have been written to this RecordWriter
     *
     * @return
     */
    int getRecordsWritten();

    /**
     * Returns the file that this RecordWriter is writing to
     *
     * @return
     */
    File getFile();

    /**
     * Obtains a mutually exclusive lock for this Writer so that operations that
     * must be atomic can be achieved atomically.
     */
    void lock();

    /**
     * Releases the lock obtained via a call to {@link #lock()}
     */
    void unlock();

    /**
     * Attempts to obtain a mutually exclusive lock for this Writer so that
     * operations that must be atomic can be achieved atomically. If the lock is
     * not immediately available, returns <code>false</code>; otherwise, obtains
     * the lock and returns <code>true</code>.
     *
     * @return
     */
    boolean tryLock();

    /**
     * Syncs the content written to this writer to disk.
     * @throws java.io.IOException
     */
    void sync() throws IOException;

}
