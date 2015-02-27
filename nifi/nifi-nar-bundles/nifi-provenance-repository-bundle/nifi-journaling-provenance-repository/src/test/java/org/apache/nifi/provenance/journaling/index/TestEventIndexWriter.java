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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.journaling.TestUtil;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.SearchTerms;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.util.file.FileUtils;
import org.junit.Test;

public class TestEventIndexWriter {

    @Test
    public void testIndexAndFetch() throws IOException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        config.setSearchableAttributes(Arrays.asList(new SearchableField[] {
                SearchableFields.newSearchableAttribute("test.1")
        }));
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        final File indexDir = new File("target/" + UUID.randomUUID().toString());
        
        try (final LuceneIndexWriter indexWriter = new LuceneIndexWriter(indexDir, config)) {
            final ProvenanceEventRecord event = TestUtil.generateEvent(23L);
            final JournaledStorageLocation location = new JournaledStorageLocation("container", "section", 1L, 2, 23L);
            final JournaledProvenanceEvent storedEvent = new JournaledProvenanceEvent(event, location);
            indexWriter.index(Collections.singleton(storedEvent));
            
            final Query query = new Query("123");
            query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.FlowFileUUID, "00000000-0000-0000-0000-000000000023"));
            
            try (final EventIndexSearcher searcher = indexWriter.newIndexSearcher()) {
                final SearchResult searchResult = searcher.search(query);
                final List<JournaledStorageLocation> locations = searchResult.getLocations();
                assertNotNull(locations);
                assertEquals(1, locations.size());
                
                final JournaledStorageLocation found = locations.get(0);
                assertNotNull(found);
                assertEquals("container", found.getContainerName());
                assertEquals("section", found.getSectionName());
                assertEquals(1L, found.getJournalId().longValue());
                assertEquals(2, found.getBlockIndex());
                assertEquals(23L, found.getEventId());
            }
        } finally {
            FileUtils.deleteFile(indexDir, true);
        }
        
    }
    
}
