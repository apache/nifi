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
package org.apache.nifi.tests.system.provenance;

import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceSearchValueDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProvenanceEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;

public class ProvenanceRepositoryIT extends NiFiSystemIT {

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        final Map<String, String> properties = new HashMap<>();

        // Force only a single Provenance Event File to exist
        properties.put("nifi.provenance.repository.max.storage.size", "1 KB");

        // Perform maintenance every 2 seconds to ensure that we don't have to wait a long time for old event files to roll off.
        properties.put("nifi.provenance.repository.maintenance.frequency", "2 secs");

        return properties;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        // We need to destroy entire environment after each test to ensure that the repositories are destroyed.
        // This is important because we are expecting exact numbers of events in the repo.
        return true;
    }

    @Test
    public void testSimpleQueryByComponentID() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity count = getClientUtil().createProcessor("CountEvents");
        getClientUtil().setAutoTerminatedRelationships(count, "success");
        getClientUtil().createConnection(generateFlowFile, count, "success");

        getNifiClient().getProcessorClient().startProcessor(generateFlowFile);
        getNifiClient().getProcessorClient().startProcessor(count);

        ProvenanceSearchValueDTO searchValueDto = new ProvenanceSearchValueDTO();
        searchValueDto.setValue(generateFlowFile.getId());
        searchValueDto.setInverse(false);

        final Map<SearchableField, ProvenanceSearchValueDTO> searchTerms = Collections.singletonMap(SearchableFields.ComponentID, searchValueDto);
        ProvenanceEntity provenanceEntity = getClientUtil().queryProvenance(searchTerms, null, null);
        assertEquals(0, provenanceEntity.getProvenance().getResults().getProvenanceEvents().size());

        // Wait for there to be at least 1 event.
        waitForEventCountAtLeast(searchTerms, 1);

        provenanceEntity = getClientUtil().queryProvenance(searchTerms, null, null);

        final List<ProvenanceEventDTO> events = provenanceEntity.getProvenance().getResults().getProvenanceEvents();
        assertEquals(1, events.size());

        final ProvenanceEventDTO firstEvent = events.get(0);
        assertEquals(ProvenanceEventType.CREATE.name(), firstEvent.getEventType());
    }



    // If we add some events for Component ABC and then they age off, we should be able to query and get back 0 results.
    // If we then add some more events for Component ABC and query, we should see those new events. Even if we have aged off
    // 1000+ events (1000 = max results of the provenance query). This should be true whether NiFi is restarted in between or not.
    // To ensure this, we have two tests that are very similar but one restarts NiFi in between and one does not.
    // This test does not restart NiFi.
    @Test
    public void testAgeOffEventsThenAddSomeThenQuery() throws NiFiClientException, IOException, InterruptedException {
        ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        generateFlowFile = getClientUtil().updateProcessorProperties(generateFlowFile, Collections.singletonMap("Batch Size", "800"));

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().setAutoTerminatedRelationships(terminate, "success");
        getClientUtil().createConnection(generateFlowFile, terminate, "success");

        generateFlowFile = getNifiClient().getProcessorClient().startProcessor(generateFlowFile);

        ProvenanceSearchValueDTO searchValueDto = new ProvenanceSearchValueDTO();
        searchValueDto.setValue(generateFlowFile.getId());
        searchValueDto.setInverse(false);

        final Map<SearchableField, ProvenanceSearchValueDTO> generateSearchTerms = Collections.singletonMap(SearchableFields.ComponentID, searchValueDto);

        // Wait for there to be at least 1000 events for Generate processor and then stop the processor
        waitForEventCountAtLeast(generateSearchTerms, 800);
        getNifiClient().getProcessorClient().stopProcessor(generateFlowFile);

        // Start Terminate proc & wait for at least 600 events to be registered. We do this because each Event File can hold up to 1,000 Events.
        // The GenerateFlowFile would have 800. The first 200 events from Terminate will be in the first Event File, causing that one to
        // roll over and subsequently be aged off. The second Event File will hold the other 600. So we may have 600 or 800 events,
        // depending on when the query is executed.
        getNifiClient().getProcessorClient().startProcessor(terminate);

        ProvenanceSearchValueDTO terminateSearchValueDto = new ProvenanceSearchValueDTO();
        terminateSearchValueDto.setValue(terminate.getId());
        terminateSearchValueDto.setInverse(false);

        final Map<SearchableField, ProvenanceSearchValueDTO> terminateSearchTerms = Collections.singletonMap(SearchableFields.ComponentID, terminateSearchValueDto);
        waitForEventCountAtLeast(terminateSearchTerms, 600);

        waitForEventCountExactly(generateSearchTerms, 0);

        // Emit 25 more events
        getClientUtil().updateProcessorProperties(generateFlowFile, Collections.singletonMap("Batch Size", "25"));
        getNifiClient().getProcessorClient().startProcessor(generateFlowFile);

        // Wait for those 25 events to be emitted
        waitForEventCountAtLeast(generateSearchTerms, 25);
    }


    // If we add some events for Component ABC and then they age off, we should be able to query and get back 0 results.
    // If we then add some more events for Component ABC and query, we should see those new events. Even if we have aged off
    // 1000+ events (1000 = max results of the provenance query). This should be true whether NiFi is restarted in between or not.
    // To ensure this, we have two tests that are very similar but one restarts NiFi in between and one does not.
    // This test does restart NiFi.
    @Test
    public void testAgeOffEventsThenRestartAddSomeThenQuery() throws NiFiClientException, IOException, InterruptedException {
        ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        generateFlowFile = getClientUtil().updateProcessorProperties(generateFlowFile, Collections.singletonMap("Batch Size", "800"));

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().setAutoTerminatedRelationships(terminate, "success");
        getClientUtil().createConnection(generateFlowFile, terminate, "success");

        generateFlowFile = getNifiClient().getProcessorClient().startProcessor(generateFlowFile);

        ProvenanceSearchValueDTO searchValueDto = new ProvenanceSearchValueDTO();
        searchValueDto.setValue(generateFlowFile.getId());
        searchValueDto.setInverse(false);

        final Map<SearchableField, ProvenanceSearchValueDTO> generateSearchTerms = Collections.singletonMap(SearchableFields.ComponentID, searchValueDto);

        // Wait for there to be at least 800 events for Generate processor and then stop it
        waitForEventCountAtLeast(generateSearchTerms, 800);
        getNifiClient().getProcessorClient().stopProcessor(generateFlowFile);

        // Start Terminate proc & wait for at least 600 events to be registered. We do this because each Event File can hold up to 1,000 Events.
        // The GenerateFlowFile would have 800. The first 200 events from Terminate will be in the first Event File, causing that one to
        // roll over and subsequently be aged off. The second Event File will hold the other 600. So we may have 600 or 800 events,
        // depending on when the query is executed.
        getNifiClient().getProcessorClient().startProcessor(terminate);

        ProvenanceSearchValueDTO terminateSearchValueDto = new ProvenanceSearchValueDTO();
        terminateSearchValueDto.setValue(terminate.getId());
        terminateSearchValueDto.setInverse(false);

        final Map<SearchableField, ProvenanceSearchValueDTO> terminateSearchTerms = Collections.singletonMap(SearchableFields.ComponentID, terminateSearchValueDto);
        waitForEventCountAtLeast(terminateSearchTerms, 600);
        getNifiClient().getProcessorClient().stopProcessor(terminate);

        waitForEventCountExactly(generateSearchTerms, 0);

        // Restart NiFi. We do this so that when we query provenance for the Processor we won't be able to use the "Cached" events
        // and will instead have to query Lucene
        getNiFiInstance().stop();
        getNiFiInstance().start();

        // Ensure that Terminate processor is stopped, since nifi could have shutdown before persisting flow.xml.gz
        terminate.getRevision().setVersion(0L); // Reset the revision
        getNifiClient().getProcessorClient().stopProcessor(terminate);
        getClientUtil().waitForStoppedProcessor(terminate.getId());

        // Emit 400 more events
        generateFlowFile.getRevision().setVersion(0L); // Reset the revision
        getClientUtil().updateProcessorProperties(generateFlowFile, Collections.singletonMap("Batch Size", "400"));
        getNifiClient().getProcessorClient().startProcessor(generateFlowFile);

        // Since we restarted, the previous Event File will be rolled over. And since it will be > 1 KB in size, it will age off almost immediately.
        // This will leave us with only the 400 newly created events.
        waitForEventCountExactly(generateSearchTerms, 400);
    }

    private void waitForEventCountExactly(final Map<SearchableField, ProvenanceSearchValueDTO> searchTerms, final int expectedCount) throws InterruptedException {
        waitForEventCount(searchTerms, count -> count == expectedCount);
    }

    private void waitForEventCountAtLeast(final Map<SearchableField, ProvenanceSearchValueDTO> searchTerms, final int expectedCount) throws InterruptedException {
        waitForEventCount(searchTerms, count -> count >= expectedCount);
    }

    private void waitForEventCount(final Map<SearchableField, ProvenanceSearchValueDTO> searchTerms, final Predicate<Integer> predicate) throws InterruptedException {
        // Wait for there to be at least 1000 events for Generate processor
        waitFor(() -> {
            try {
                return predicate.test(getEventCount(searchTerms));
            } catch (final Exception e) {
                return false;
            }
        }, 500L);
    }

    private int getEventCount(final Map<SearchableField, ProvenanceSearchValueDTO> searchTerms) throws NiFiClientException, IOException {
        ProvenanceEntity provEntity = getClientUtil().queryProvenance(searchTerms, null, null);
        return provEntity.getProvenance().getResults().getProvenanceEvents().size();
    }
}
