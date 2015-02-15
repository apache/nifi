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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.search.Query;

public class MultiIndexSearcher implements EventIndexSearcher {
    private final List<EventIndexSearcher> searchers;
    
    public MultiIndexSearcher(final List<EventIndexSearcher> searchers) {
        this.searchers = searchers;
    }

    @Override
    public void close() throws IOException {
        IOException suppressed = null;
        
        for ( final EventIndexSearcher searcher : searchers ) {
            try {
                searcher.close();
            } catch (final IOException ioe) {
                if ( suppressed == null ) {
                    suppressed = ioe;
                } else {
                    suppressed.addSuppressed(ioe);
                }
            }
        }
        
        if ( suppressed != null ) {
            throw suppressed;
        }
    }

    @Override
    public SearchResult search(final Query query) throws IOException {
        int totalHitCount = 0;
        final List<JournaledStorageLocation> locations = new ArrayList<>();
        
        for ( final EventIndexSearcher searcher : searchers ) {
            final SearchResult result = searcher.search(query);
            totalHitCount += result.getTotalCount();
            locations.addAll(result.getLocations());
        }
        
        return new SearchResult(locations, totalHitCount);
    }

    @Override
    public List<JournaledStorageLocation> getEvents(final long minEventId, final int maxResults) throws IOException {
        final List<JournaledStorageLocation> locations = new ArrayList<>();
        int results = 0;
        
        // Perform search against all searchers and aggregate results.
        for ( final EventIndexSearcher searcher : searchers ) {
            final List<JournaledStorageLocation> searchLocations = searcher.getEvents(minEventId, maxResults);
            locations.addAll(searchLocations);
            if ( !searchLocations.isEmpty() ) {
                results++;
            }
        }
        
        // Results from this call are sorted. If we have only 0 or 1 searchers that had results, then
        // we don't need to sort anything. Otherwise, we need to sort and return just the first X
        // number of results.
        if ( results > 1 ) {
            Collections.sort(locations);
        }
        
        if ( locations.size() > maxResults ) {
            return locations.subList(0, maxResults);
        }
        
        return locations;
    }

    @Override
    public Long getMaxEventId(final String container, final String section) throws IOException {
        Long max = null;
        for ( final EventIndexSearcher searcher : searchers ) {
            final Long maxForWriter = searcher.getMaxEventId(container, section);
            if ( maxForWriter != null ) {
                if (max == null || maxForWriter.longValue() > max.longValue() ) {
                    max = maxForWriter;
                }
            }
        }
        
        return max;
    }

}
