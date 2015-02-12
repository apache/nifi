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
package org.apache.nifi.provenance.journaling.partition;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.journaling.index.EventIndexSearcher;


/**
 * Represents a single Partition of the Journaling Provenance Repository. The repository is split into multiple
 * partitions in order to provide higher throughput.
 * 
 * Implementations of this interface MUST be threadsafe.
 */
public interface Partition {

    /**
     * Returns a new EventIndexSearcher that can be used to search the events in this partition
     * @return
     * @throws IOException
     */
    EventIndexSearcher newIndexSearcher() throws IOException;
    
    /**
     * Registers the given events with this partition. This includes persisting the events and indexing
     * them so that they are searchable.
     * @param events
     * @return
     */
    List<JournaledProvenanceEvent> registerEvents(Collection<ProvenanceEventRecord> events, long firstEventId) throws IOException;
    
    /**
     * Restore state after a restart of NiFi
     */
    void restore() throws IOException;
    
    /**
     * Shuts down the Partition so that it can no longer be used
     */
    void shutdown();

    /**
     * Returns the largest event ID stored in this partition
     * @return
     */
    long getMaxEventId();
    
    /**
     * Returns the locations of events that have an id at least equal to minEventId, returning the events
     * with the smallest ID's possible that are greater than minEventId
     *
     * @param minEventId
     * @param maxRecords
     * @return
     */
    List<JournaledStorageLocation> getEvents(long minEventId, int maxRecords) throws IOException;
    
    /**
     * Returns the timestamp of the earliest event in this Partition, or <code>null</code> if the Partition
     * contains no events
     * @return
     * @throws IOException
     */
    Long getEarliestEventTime() throws IOException;
}
