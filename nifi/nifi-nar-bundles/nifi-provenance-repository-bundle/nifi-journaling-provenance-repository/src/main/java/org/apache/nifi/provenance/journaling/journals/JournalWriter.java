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
package org.apache.nifi.provenance.journaling.journals;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.provenance.ProvenanceEventRecord;

/**
 * Responsible for writing events to an append-only journal, or write-ahead-log. Events are written in "Blocks."
 * These Blocks are used so that if we are compressing data, we can compress individual Blocks. This allows us
 * to store a "Block Index" so that we can quickly lookup the start of a Block when reading the data to quickly
 * obtain the data that we need.
 */
public interface JournalWriter extends Closeable {
    
    /**
     * Returns the identifier of this journal. The identifier is unique per 'section' of the repository
     * @return
     */
    long getJournalId();
    
    /**
     * Writes the given events to the journal and assigns the events sequential ID's starting with the
     * ID given
     * 
     * @param records
     * @param firstRecordId
     * @return
     * @throws IOException
     */
    void write(Collection<ProvenanceEventRecord> events, long firstEventId) throws IOException;
    
    /**
     * Returns the File that the Journal is writing to
     */
    File getJournalFile();
    
    /**
     * Synchronizes changes to the underlying file system
     * @throws IOException
     */
    void sync() throws IOException;
    
    /**
     * Returns the size of the journal
     * @return
     */
    long getSize();
    
    /**
     * Returns the number of events that have been written to this journal
     * @return
     */
    int getEventCount();
    
    /**
     * Returns the amount of time that has elapsed since the point at which the writer was created.
     * @param timeUnit
     * @return
     */
    long getAge(TimeUnit timeUnit);
    
    /**
     * Marks the end of a Block in the output file. If the previous Block has been finished and no new
     * Block has been started, this method will return silently without doing anything.
     * @throws IOException
     */
    void finishBlock() throws IOException;
    
    /**
     * Starts a new Block in the output file. If a Block has already been started, this method throws
     * an IllegalStateException
     * 
     * @throws IOException
     */
    void beginNewBlock() throws IOException;
}
