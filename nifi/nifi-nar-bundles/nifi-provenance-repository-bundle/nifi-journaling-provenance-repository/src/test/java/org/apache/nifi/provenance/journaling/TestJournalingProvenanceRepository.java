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
package org.apache.nifi.provenance.journaling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchTerms;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.util.file.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJournalingProvenanceRepository {

    
    @BeforeClass
    public static void setupLogging() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance.journaling", "DEBUG");
    }
    
    @Test
    public void testStoreAndRetrieve() throws IOException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        config.setPartitionCount(3);
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            final Map<String, String> attributes = new HashMap<>();
            
            for (int i=0; i < 10; i++) {
                attributes.put("i", String.valueOf(i));
                repo.registerEvent(TestUtil.generateEvent(i, attributes));
            }
            
            // retrieve records one at a time.
            for (int i=0; i < 10; i++) {
                final StoredProvenanceEvent event = repo.getEvent(i);
                assertNotNull(event);
                assertEquals((long) i, event.getEventId());
                assertEquals("00000000-0000-0000-0000-00000000000" + i, event.getFlowFileUuid());
            }
            
            final List<StoredProvenanceEvent> events = repo.getEvents(0, 1000);
            assertNotNull(events);
            assertEquals(10, events.size());
            for (int i=0; i < 10; i++) {
                final StoredProvenanceEvent event = events.get(i);
                assertNotNull(event);
                assertEquals((long) i, event.getEventId());
                assertEquals("00000000-0000-0000-0000-00000000000" + i, event.getFlowFileUuid());
            }
        } finally {
            for ( final File file : containers.values() ) {
                if ( file.exists() ) {
                    FileUtils.deleteFile(file, true);
                }
            }
        }
    }
    
    
    @Test(timeout=10000000)
    public void testSearchByUUID() throws IOException, InterruptedException {
        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        final Map<String, File> containers = new HashMap<>();
        containers.put("container1", new File("target/" + UUID.randomUUID().toString()));
        containers.put("container2", new File("target/" + UUID.randomUUID().toString()));
        config.setContainers(containers);
        
        config.setPartitionCount(3);
        config.setSearchableFields(Arrays.asList(new SearchableField[] {
                SearchableFields.FlowFileUUID
        }));
        
        try (final JournalingProvenanceRepository repo = new JournalingProvenanceRepository(config)) {
            repo.initialize(null);
            
            final Map<String, String> attributes = new HashMap<>();
            
            for (int i=0; i < 10; i++) {
                attributes.put("i", String.valueOf(i));
                repo.registerEvent(TestUtil.generateEvent(i, attributes));
            }
            
            final Query query = new Query("query");
            query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.FlowFileUUID, "00000000-0000-0000-0000-000000000005"));
            final QuerySubmission submission = repo.submitQuery(query);
            assertNotNull(submission);
            
            final QueryResult result = submission.getResult();
            while ( !result.isFinished() ) {
                Thread.sleep(50L);
            }
            
            assertNull(result.getError());
            final List<StoredProvenanceEvent> matches = result.getMatchingEvents();
            assertNotNull(matches);
            assertEquals(1, matches.size());
            
            final StoredProvenanceEvent event = matches.get(0);
            assertEquals(5, event.getEventId());
            assertEquals("00000000-0000-0000-0000-000000000005", event.getFlowFileUuid());
        } finally {
            for ( final File file : containers.values() ) {
                FileUtils.deleteFile(file, true);
            }
        }
    }
    
}
