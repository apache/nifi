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
package org.apache.nifi.provenance.journaling.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;

public interface EventIndexWriter extends Closeable {

    /**
     * Adds all of the events to the index.
     * @param events
     * @throws IOException
     */
    void index(final Collection<JournaledProvenanceEvent> events) throws IOException;
    
    /**
     * Forces all updates to the index to be pushed to disk.
     * @throws IOException
     */
    void sync() throws IOException;
    
    /**
     * Deletes any records that belong to the given container/section/journal
     * @param containerName
     * @param section
     * @param journalId
     * @throws IOException
     */
    void delete(String containerName, String section, Long journalId) throws IOException;
    
    /**
     * Deletes any records that belong to the given container and section but have a journal Id less
     * than the specified value
     * @param containerName
     * @param section
     * @param journalId
     * @throws IOException
     */
    void deleteEventsBefore(String containerName, String section, Long journalId) throws IOException;
    
    
    /**
     * Removes all events from the index that occurred before the given time
     * @param earliestEventTimeToDelete
     * @throws IOException
     */
    void deleteOldEvents(long earliestEventTimeToDelete) throws IOException;
}
