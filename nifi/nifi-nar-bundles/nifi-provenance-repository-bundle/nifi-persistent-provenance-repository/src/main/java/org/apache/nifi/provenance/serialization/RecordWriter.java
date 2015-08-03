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
import org.apache.nifi.provenance.toc.TocWriter;

public interface RecordWriter extends Closeable {

    /**
     * Writes header information to the underlying stream
     *
     * @param firstEventId the ID of the first provenance event that will be written to the stream
     * @throws IOException if unable to write header information to the underlying stream
     */
    void writeHeader(long firstEventId) throws IOException;

    /**
     * Writes the given record out to the underlying stream
     *
     * @param record the record to write
     * @param recordIdentifier the new identifier of the record
     * @return the number of bytes written for the given records
     * @throws IOException if unable to write the record to the stream
     */
    long writeRecord(ProvenanceEventRecord record, long recordIdentifier) throws IOException;

    /**
     * @return the number of Records that have been written to this RecordWriter
     */
    int getRecordsWritten();

    /**
     * @return the file that this RecordWriter is writing to
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
     * not immediately available, or if the writer is 'dirty' (see {@link #markDirty()},
     * returns <code>false</code>; otherwise, obtains the lock and returns <code>true</code>.
     *
     * @return <code>true</code> if the lock was obtained, <code>false</code> otherwise.
     */
    boolean tryLock();

    /**
     * Indicates that this Record Writer is 'dirty', meaning that it can no longer be
     * updated. This can happen, for example, if a partial record is written. In this case,
     * writing to this RecordWriter again could cause corruption.
     */
    void markDirty();

    /**
     * Syncs the content written to this writer to disk.
     *
     * @throws IOException if unable to sync content to disk
     */
    void sync() throws IOException;

    /**
     * @return the TOC Writer that is being used to write the Table of Contents for this journal
     */
    TocWriter getTocWriter();
}
