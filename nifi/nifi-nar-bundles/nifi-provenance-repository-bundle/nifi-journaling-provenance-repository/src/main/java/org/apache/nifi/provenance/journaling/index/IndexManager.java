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
import java.io.File;
import java.io.IOException;
import java.util.Set;

public interface IndexManager extends Closeable {

    /**
     * Returns an EventIndexWriter for the given container.
     * @param container
     * @return
     */
    EventIndexWriter getIndexWriter(final String container);

    /**
     * Returns the max event ID that has been indexed for the given container and section.
     * 
     * @param container
     * @param section
     * @return
     */
    Long getMaxEventId(String container, String section) throws IOException;
    
    /**
     * Returns the total number of indices for all containers
     * @return
     */
    int getNumberOfIndices();
    
    /**
     * Returns a new {@link EventIndexSearcher} that can be used to search the indices for
     * the given container
     * 
     * @param containerName the containerName to search
     * 
     * @return
     * @throws IOException
     */
    EventIndexSearcher newIndexSearcher(String containerName) throws IOException;
    
    /**
     * Executes the given action against each index and returns a Set of results,
     * where each result is obtained from performing the given action against one index
     * 
     * @param action the action to perform
     * @return
     * @throws IOException
     */
    <T> Set<T> withEachIndex(IndexAction<T> action) throws IOException;
    
    /**
     * Performs the given action against each index, waiting for the action to complete
     * against each index before returning
     * 
     * @param action the action to perform against each index
     * @throws IOException
     */
    void withEachIndex(VoidIndexAction action) throws IOException;
    
    /**
     * Performs the given action against each index
     * 
     * @param action the action to perform
     * 
     * @param async if true, the method will return immediatley and the actions will occur
     * in the background. If <code>false</code>, the method will not return until the action
     * has been performed against all indices
     * 
     * @throws IOException
     */
    void withEachIndex(VoidIndexAction action, boolean async) throws IOException;
    
    /**
     * Removes any events that have a Storage Location that includes the provided containerName, secitonIndex, and journalId,
     * and then re-adds all of the events that are in the given journalFile.
     * @param containerName
     * @param sectionIndex
     * @param journalId
     * @param journalFile
     * @throws IOException
     */
    void reindex(final String containerName, final int sectionIndex, final Long journalId, final File journalFile) throws IOException;
    
    /**
     * Syncs all indices
     * @throws IOException
     */
    void sync() throws IOException;
    
    /**
     * Returns the total number of events in all indices
     * @return
     */
    long getNumberOfEvents() throws IOException;
    
    /**
     * Removes all events from all indices that occurred before the given time
     * @param earliestEventTimeToDelete
     * @throws IOException
     */
    void deleteOldEvents(long earliestEventTimeToDelete) throws IOException;
    
    /**
     * Deletes any events from the index that belong to the given container, section, and journal id
     * 
     * @param containerName
     * @param sectionIndex
     * @param journalId
     * 
     * @throws IOException
     */
    void deleteEvents(String containerName, int sectionIndex, Long journalId) throws IOException;
    
    /**
     * Deletes any events from the index that belong to the given container and section but have
     * a journal id before the value specified
     * 
     * @param containerName
     * @param sectionIndex
     * @param journalId
     * 
     * @throws IOException
     */
    void deleteEventsBefore(String containerName, int sectionIndex, Long journalId) throws IOException;
    
    /**
     * Returns the size (in bytes) of the index for the given container
     * @param containerName
     * @return
     */
    long getSize(String containerName);
}
