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
import java.util.List;

import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.search.Query;

public interface EventIndexSearcher extends Closeable {
    /**
     * Searches the repository for any events that match the provided query and returns the locations
     * where those events are stored
     * @param query
     * @return
     */
    SearchResult search(Query query) throws IOException;
    
    /**
     * Returns the locations of all events for a FlowFile that has a FlowFile UUID in the collection of
     * UUIDs provided, if the event time occurs between earliestTime and latestTime. The return value is 
     * ordered in the order in which the records should be read from the journals in order to obtain 
     * maximum efficiency
     * 
     * @param flowFileUuids
     * @param earliestTime
     * @param latestTime
     * 
     * @return
     * @throws IOException
     */
    List<JournaledStorageLocation> getEventsForFlowFiles(Collection<String> flowFileUuids, long earliestTime, long latestTime) throws IOException;
    
    /**
     * Returns the locations of events that have Event ID's at least equal to minEventId, and returns
     * up to the given number of results
     * 
     * @param minEventId
     * @param maxResults
     * @return
     * @throws IOException
     */
    List<JournaledStorageLocation> getEvents(long minEventId, int maxResults) throws IOException;
    
    /**
     * Returns the largest event id that is known by the index being searched
     * @param container
     * @param section
     * @return
     * @throws IOException
     */
    Long getMaxEventId(String container, String section) throws IOException;
    
    /**
     * Returns the locations of the latest events for the index being searched
     * @param numEvents
     * @return
     * @throws IOException
     */
    List<JournaledStorageLocation> getLatestEvents(int numEvents) throws IOException;
    
    /**
     * Returns the total number of events that exist for the index being searched
     * @return
     * @throws IOException
     */
    long getNumberOfEvents() throws IOException;
}
