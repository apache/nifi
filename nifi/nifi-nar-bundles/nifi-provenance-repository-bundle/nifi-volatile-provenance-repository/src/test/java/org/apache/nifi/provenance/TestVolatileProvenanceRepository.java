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
package org.apache.nifi.provenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchTerms;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestVolatileProvenanceRepository {

    private VolatileProvenanceRepository repo;

    @BeforeClass
    public static void before() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH,
                "src/test/resources/nifi.properties");
    }

    @Before
    public void setup() {
        this.repo = new VolatileProvenanceRepository();
    }

    @After
    public void teardown() {
        this.repo = null;
    }

    @Test
    public void testAddAndGet() {

        final String flowFileUUID = "cced57fb-699d-419a-bcb3-45078113b5f7";

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setEventTime(5l);
        builder.setTransitUri("nifi://unit-test");
        builder.setComponentId("1234");
        builder.setComponentType("myProcessor");
        builder.setCurrentContentClaim("", "", "", null, 16l);
        builder.setFlowFileUUID(flowFileUUID);

        final ProvenanceEventRecord expectedRecord = builder.build();

        this.repo.registerEvent(expectedRecord);

        final ProvenanceEventRecord actualRecord = this.repo.getEvent(1);

        assertEquals(ProvenanceEventType.RECEIVE, actualRecord.getEventType());
        assertEquals(5l, actualRecord.getEventTime());
        assertEquals(flowFileUUID, actualRecord.getFlowFileUuid());
        assertEquals(1, actualRecord.getEventId());
    }

    @Test
    public void testAddCollectionAndGet() {

        final String flowFileUUID = "cced57fb-699d-419a-bcb3-45078113b5f7";

        final ProvenanceEventBuilder createEventBuilder = new StandardProvenanceEventRecord.Builder();
        createEventBuilder.setEventType(ProvenanceEventType.CREATE);
        createEventBuilder.setEventTime(5l);
        createEventBuilder.setTransitUri("nifi://unit-test");
        createEventBuilder.setComponentId("1234");
        createEventBuilder.setComponentType("myProcessor1");
        createEventBuilder.setCurrentContentClaim("", "", "", null, 16l);
        createEventBuilder.setFlowFileUUID(flowFileUUID);

        final ProvenanceEventBuilder modifiedEventBuilder = new StandardProvenanceEventRecord.Builder();
        modifiedEventBuilder.setEventType(ProvenanceEventType.CONTENT_MODIFIED);
        modifiedEventBuilder.setEventTime(10l);
        modifiedEventBuilder.setTransitUri("nifi://unit-test");
        modifiedEventBuilder.setComponentId("1234");
        modifiedEventBuilder.setComponentType("myProcessor2");
        modifiedEventBuilder.setCurrentContentClaim("", "", "", null, 16l);
        modifiedEventBuilder.setFlowFileUUID(flowFileUUID);

        final ProvenanceEventBuilder sendEventBuilder = new StandardProvenanceEventRecord.Builder();
        sendEventBuilder.setEventType(ProvenanceEventType.SEND);
        sendEventBuilder.setEventTime(15l);
        sendEventBuilder.setTransitUri("nifi://unit-test");
        sendEventBuilder.setComponentId("1234");
        sendEventBuilder.setComponentType("myProcessor3");
        sendEventBuilder.setCurrentContentClaim("", "", "", null, 16l);
        sendEventBuilder.setFlowFileUUID(flowFileUUID);

        final Collection<ProvenanceEventRecord> expectedEvents = Arrays.asList(
                createEventBuilder.build(), modifiedEventBuilder.build(), sendEventBuilder.build());

        this.repo.registerEvents(expectedEvents);

        final ProvenanceEventRecord actualCreateRecord = this.repo.getEvent(1);
        final ProvenanceEventRecord actualModifiedRecord = this.repo.getEvent(2);
        final ProvenanceEventRecord actualSendRecord = this.repo.getEvent(3);

        assertEquals(ProvenanceEventType.CREATE, actualCreateRecord.getEventType());
        assertEquals(5l, actualCreateRecord.getEventTime());
        assertEquals(flowFileUUID, actualCreateRecord.getFlowFileUuid());
        assertEquals(1, actualCreateRecord.getEventId());

        assertEquals(ProvenanceEventType.CONTENT_MODIFIED, actualModifiedRecord.getEventType());
        assertEquals(10l, actualModifiedRecord.getEventTime());
        assertEquals(flowFileUUID, actualModifiedRecord.getFlowFileUuid());
        assertEquals(2, actualModifiedRecord.getEventId());

        assertEquals(ProvenanceEventType.SEND, actualSendRecord.getEventType());
        assertEquals(15l, actualSendRecord.getEventTime());
        assertEquals(flowFileUUID, actualSendRecord.getFlowFileUuid());
        assertEquals(3, actualSendRecord.getEventId());
    }

    @Test
    public void testGetMaxEventId() {

        final String flowFileUUID = "cced57fb-699d-419a-bcb3-45078113b5f7";

        final ProvenanceEventBuilder createEventBuilder = new StandardProvenanceEventRecord.Builder();
        createEventBuilder.setEventType(ProvenanceEventType.CREATE);
        createEventBuilder.setEventTime(5l);
        createEventBuilder.setTransitUri("nifi://unit-test");
        createEventBuilder.setComponentId("1234");
        createEventBuilder.setComponentType("myProcessor1");
        createEventBuilder.setCurrentContentClaim("", "", "", null, 16l);
        createEventBuilder.setFlowFileUUID(flowFileUUID);

        final ProvenanceEventRecord record = createEventBuilder.build();

        // Register four events
        this.repo.registerEvents(Arrays.asList(record, record, record, record));

        assertEquals(new Long(4l), this.repo.getMaxEventId());
    }

    @Test
    public void testGetEvents() throws IOException {

        final String flowFileUUID1 = "cced57fb-699d-419a-bcb3-45078113b5f7";
        final String flowFileUUID2 = "2ec1f5de-4654-4558-b1d7-7daa6938eb04";
        final String flowFileUUID3 = "7fd72ca9-37ed-4cd7-bce3-f1ac700c8f1a";
        final String flowFileUUID4 = "767e0105-9a5a-4fd1-93a8-836dd3e26933";
        final String flowFileUUID5 = "951a5d0a-9464-4eee-8bc9-d2aa62bf3024";

        final ProvenanceEventBuilder createEventBuilder = new StandardProvenanceEventRecord.Builder();
        createEventBuilder.setEventType(ProvenanceEventType.CREATE);
        createEventBuilder.setEventTime(5l);
        createEventBuilder.setTransitUri("nifi://unit-test");
        createEventBuilder.setComponentId("1234");
        createEventBuilder.setComponentType("myProcessor1");
        createEventBuilder.setCurrentContentClaim("", "", "", null, 16l);

        final ProvenanceEventRecord record1 = createEventBuilder.setFlowFileUUID(flowFileUUID1)
                .build();
        final ProvenanceEventRecord record2 = createEventBuilder.setFlowFileUUID(flowFileUUID2)
                .build();
        final ProvenanceEventRecord record3 = createEventBuilder.setFlowFileUUID(flowFileUUID3)
                .build();
        final ProvenanceEventRecord record4 = createEventBuilder.setFlowFileUUID(flowFileUUID4)
                .build();
        final ProvenanceEventRecord record5 = createEventBuilder.setFlowFileUUID(flowFileUUID5)
                .build();

        // Register five events
        this.repo.registerEvents(Arrays.asList(record1, record2, record3, record4, record5));

        final List<ProvenanceEventRecord> actualRecords = this.repo.getEvents(2l, 2);

        assertNotNull(actualRecords);
        assertEquals(2, actualRecords.size());
        assertEquals(flowFileUUID2, actualRecords.get(0).getFlowFileUuid());
        assertEquals(flowFileUUID3, actualRecords.get(1).getFlowFileUuid());
    }

    @Test
    public void testRetrieveQuerySubmissionFlowFileUUID() {

        final String queryUUID = "2f7bd52d-fcef-4ab4-8991-907d639cbe0e";
        final String flowFileUUID1 = "cced57fb-699d-419a-bcb3-45078113b5f7";
        final String flowFileUUID2 = "2ec1f5de-4654-4558-b1d7-7daa6938eb04";
        final String flowFileUUID3 = "7fd72ca9-37ed-4cd7-bce3-f1ac700c8f1a";
        final String flowFileUUID4 = "767e0105-9a5a-4fd1-93a8-836dd3e26933";
        final String flowFileUUID5 = "951a5d0a-9464-4eee-8bc9-d2aa62bf3024";

        final ProvenanceEventBuilder createEventBuilder = new StandardProvenanceEventRecord.Builder();
        createEventBuilder.setEventType(ProvenanceEventType.CREATE);
        createEventBuilder.setEventTime(5l);
        createEventBuilder.setTransitUri("nifi://unit-test");
        createEventBuilder.setComponentId("1234");
        createEventBuilder.setComponentType("myProcessor1");
        createEventBuilder.setCurrentContentClaim("", "", "", null, 16l);

        final ProvenanceEventRecord record1 = createEventBuilder.setFlowFileUUID(flowFileUUID1)
                .build();
        final ProvenanceEventRecord record2 = createEventBuilder.setFlowFileUUID(flowFileUUID2)
                .build();
        final ProvenanceEventRecord record3 = createEventBuilder.setFlowFileUUID(flowFileUUID3)
                .build();
        final ProvenanceEventRecord record4 = createEventBuilder.setFlowFileUUID(flowFileUUID4)
                .build();
        final ProvenanceEventRecord record5 = createEventBuilder.setFlowFileUUID(flowFileUUID5)
                .build();

        // Register five events
        this.repo.registerEvents(Arrays.asList(record1, record2, record3, record4, record5));

        final Query query = new Query(queryUUID);
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.FlowFileUUID,
                "767e0105-9a5a-4fd1-93a8-836dd3e2693?"));
        query.setMaxResults(1);

        final QuerySubmission submission = repo.submitQuery(query);
        final QuerySubmission result = this.repo.retrieveQuerySubmission(submission
                .getQueryIdentifier());

        final List<ProvenanceEventRecord> matches = result.getResult().getMatchingEvents();

        assertEquals(1, matches.size());
        assertEquals(flowFileUUID4, matches.get(0).getFlowFileUuid());
    }

    /**
     * Fields read out of external properties file
     */
    @Test
    public void testGetSearchableFields() {

        final Collection<String> expectedFields = Arrays.asList("uuid", "filename", "processorId",
                "transitUri");

        final List<SearchableField> actualFields = this.repo.getSearchableFields();

        assertNotNull(actualFields);
        assertEquals(4, actualFields.size());

        for (final SearchableField field : actualFields) {
            final String fieldName = field.getSearchableFieldName();
            assertTrue("No field: " + fieldName, expectedFields.contains(fieldName));
        }

    }

    /**
     * Fields read out of external properties file
     */
    @Test
    public void testGetSearchableAttributes() {

        final Collection<String> expectedFields = Arrays.asList("abc", "xyz");

        final List<SearchableField> actualFields = this.repo.getSearchableAttributes();

        assertNotNull(actualFields);
        assertEquals(2, actualFields.size());

        for (final SearchableField field : actualFields) {
            final String fieldName = field.getSearchableFieldName();
            assertTrue("No field: " + fieldName, expectedFields.contains(fieldName));
        }

    }

    @Test
    public void testSubmitQueryStartDate() {
        final String flowFileUUID1 = "cced57fb-699d-419a-bcb3-45078113b5f7";
        final String flowFileUUID2 = "2ec1f5de-4654-4558-b1d7-7daa6938eb04";
        final String flowFileUUID3 = "7fd72ca9-37ed-4cd7-bce3-f1ac700c8f1a";
        final String flowFileUUID4 = "767e0105-9a5a-4fd1-93a8-836dd3e26933";
        final String flowFileUUID5 = "951a5d0a-9464-4eee-8bc9-d2aa62bf3024";

        final ProvenanceEventBuilder createEventBuilder = new StandardProvenanceEventRecord.Builder();
        createEventBuilder.setEventType(ProvenanceEventType.CREATE);
        createEventBuilder.setTransitUri("nifi://unit-test");
        createEventBuilder.setComponentId("1234");
        createEventBuilder.setComponentType("myProcessor1");
        createEventBuilder.setCurrentContentClaim("", "", "", null, 16l);

        final ProvenanceEventRecord record1 = createEventBuilder.setFlowFileUUID(flowFileUUID1)
                .setEventTime(1).build();
        final ProvenanceEventRecord record2 = createEventBuilder.setFlowFileUUID(flowFileUUID2)
                .setEventTime(2).build();
        final ProvenanceEventRecord record3 = createEventBuilder.setFlowFileUUID(flowFileUUID3)
                .setEventTime(3).build();
        final ProvenanceEventRecord record4 = createEventBuilder.setFlowFileUUID(flowFileUUID4)
                .setEventTime(4).build();
        final ProvenanceEventRecord record5 = createEventBuilder.setFlowFileUUID(flowFileUUID5)
                .setEventTime(5).build();

        // Register five events
        this.repo.registerEvents(Arrays.asList(record1, record2, record3, record4, record5));

        final Query query = new Query("testSubmitQueryStartDate");

        // TODO: setStartDate in query - setEventTime in EventBuilder ???
        query.setStartDate(new Date(3l));

        this.repo.submitQuery(query);

        final QuerySubmission results = this.repo
                .retrieveQuerySubmission("testSubmitQueryStartDate");

        final List<ProvenanceEventRecord> matches = results.getResult().getMatchingEvents();

        assertNotNull(matches);
        assertEquals(3, matches.size());

        final Collection<String> uuids = Arrays.asList(flowFileUUID3, flowFileUUID4, flowFileUUID5);

        for (final ProvenanceEventRecord match : matches) {
            assertTrue(uuids.contains(match.getFlowFileUuid()));
        }
    }

    @Test
    public void testSubmitQueryEndDate() {
        final String flowFileUUID1 = "cced57fb-699d-419a-bcb3-45078113b5f7";
        final String flowFileUUID2 = "2ec1f5de-4654-4558-b1d7-7daa6938eb04";
        final String flowFileUUID3 = "7fd72ca9-37ed-4cd7-bce3-f1ac700c8f1a";
        final String flowFileUUID4 = "767e0105-9a5a-4fd1-93a8-836dd3e26933";
        final String flowFileUUID5 = "951a5d0a-9464-4eee-8bc9-d2aa62bf3024";

        final ProvenanceEventBuilder createEventBuilder = new StandardProvenanceEventRecord.Builder();
        createEventBuilder.setEventType(ProvenanceEventType.CREATE);
        createEventBuilder.setTransitUri("nifi://unit-test");
        createEventBuilder.setComponentId("1234");
        createEventBuilder.setComponentType("myProcessor1");
        createEventBuilder.setCurrentContentClaim("", "", "", null, 16l);

        final ProvenanceEventRecord record1 = createEventBuilder.setFlowFileUUID(flowFileUUID1)
                .setEventTime(1).build();
        final ProvenanceEventRecord record2 = createEventBuilder.setFlowFileUUID(flowFileUUID2)
                .setEventTime(2).build();
        final ProvenanceEventRecord record3 = createEventBuilder.setFlowFileUUID(flowFileUUID3)
                .setEventTime(3).build();
        final ProvenanceEventRecord record4 = createEventBuilder.setFlowFileUUID(flowFileUUID4)
                .setEventTime(4).build();
        final ProvenanceEventRecord record5 = createEventBuilder.setFlowFileUUID(flowFileUUID5)
                .setEventTime(5).build();

        // Register five events
        this.repo.registerEvents(Arrays.asList(record1, record2, record3, record4, record5));

        final Query query = new Query("testSubmitQueryEndDate");

        // TODO: setEndDate in query - setEventTime in EventBuilder ???
        query.setEndDate(new Date(3l));

        this.repo.submitQuery(query);

        final QuerySubmission results = this.repo.retrieveQuerySubmission("testSubmitQueryEndDate");

        final List<ProvenanceEventRecord> matches = results.getResult().getMatchingEvents();

        assertNotNull(matches);
        assertEquals(3, matches.size());

        final Collection<String> uuids = Arrays.asList(flowFileUUID1, flowFileUUID2, flowFileUUID3);

        for (final ProvenanceEventRecord match : matches) {
            assertTrue(uuids.contains(match.getFlowFileUuid()));
        }
    }

    @Test
    public void testSubmitQueryMaxFileSize() {
        final String flowFileUUID1 = "cced57fb-699d-419a-bcb3-45078113b5f7";
        final String flowFileUUID2 = "2ec1f5de-4654-4558-b1d7-7daa6938eb04";
        final String flowFileUUID3 = "7fd72ca9-37ed-4cd7-bce3-f1ac700c8f1a";
        final String flowFileUUID4 = "767e0105-9a5a-4fd1-93a8-836dd3e26933";
        final String flowFileUUID5 = "951a5d0a-9464-4eee-8bc9-d2aa62bf3024";

        final ProvenanceEventBuilder createEventBuilder = new StandardProvenanceEventRecord.Builder();
        createEventBuilder.setEventType(ProvenanceEventType.CREATE);
        createEventBuilder.setTransitUri("nifi://unit-test");
        createEventBuilder.setComponentId("1234");
        createEventBuilder.setComponentType("myProcessor1");
        createEventBuilder.setEventTime(3);

        final ProvenanceEventRecord record1 = createEventBuilder.setFlowFileUUID(flowFileUUID1)
                .setCurrentContentClaim("", "", "", null, 1l).build();
        final ProvenanceEventRecord record2 = createEventBuilder.setFlowFileUUID(flowFileUUID2)
                .setCurrentContentClaim("", "", "", null, 2l).build();
        final ProvenanceEventRecord record3 = createEventBuilder.setFlowFileUUID(flowFileUUID3)
                .setCurrentContentClaim("", "", "", null, 3l).build();
        final ProvenanceEventRecord record4 = createEventBuilder.setFlowFileUUID(flowFileUUID4)
                .setCurrentContentClaim("", "", "", null, 4l).build();
        final ProvenanceEventRecord record5 = createEventBuilder.setFlowFileUUID(flowFileUUID5)
                .setCurrentContentClaim("", "", "", null, 5l).build();

        // Register five events
        this.repo.registerEvents(Arrays.asList(record1, record2, record3, record4, record5));

        final Query query = new Query("testSubmitQueryMaxFileSize");

        query.setMaxFileSize("3B");

        this.repo.submitQuery(query);

        final QuerySubmission results = this.repo
                .retrieveQuerySubmission("testSubmitQueryMaxFileSize");

        final List<ProvenanceEventRecord> matches = results.getResult().getMatchingEvents();

        assertNotNull(matches);
        assertEquals(3, matches.size());

        final Collection<String> uuids = Arrays.asList(flowFileUUID1, flowFileUUID2, flowFileUUID3);

        for (final ProvenanceEventRecord match : matches) {
            assertTrue(uuids.contains(match.getFlowFileUuid()));
        }
    }

    @Test
    public void testSubmitQueryMinFileSize() {
        final String flowFileUUID1 = "cced57fb-699d-419a-bcb3-45078113b5f7";
        final String flowFileUUID2 = "2ec1f5de-4654-4558-b1d7-7daa6938eb04";
        final String flowFileUUID3 = "7fd72ca9-37ed-4cd7-bce3-f1ac700c8f1a";
        final String flowFileUUID4 = "767e0105-9a5a-4fd1-93a8-836dd3e26933";
        final String flowFileUUID5 = "951a5d0a-9464-4eee-8bc9-d2aa62bf3024";

        final ProvenanceEventBuilder createEventBuilder = new StandardProvenanceEventRecord.Builder();
        createEventBuilder.setEventType(ProvenanceEventType.CREATE);
        createEventBuilder.setTransitUri("nifi://unit-test");
        createEventBuilder.setComponentId("1234");
        createEventBuilder.setComponentType("myProcessor1");
        createEventBuilder.setEventTime(3);

        final ProvenanceEventRecord record1 = createEventBuilder.setFlowFileUUID(flowFileUUID1)
                .setCurrentContentClaim("", "", "", null, 1l).build();
        final ProvenanceEventRecord record2 = createEventBuilder.setFlowFileUUID(flowFileUUID2)
                .setCurrentContentClaim("", "", "", null, 2l).build();
        final ProvenanceEventRecord record3 = createEventBuilder.setFlowFileUUID(flowFileUUID3)
                .setCurrentContentClaim("", "", "", null, 3l).build();
        final ProvenanceEventRecord record4 = createEventBuilder.setFlowFileUUID(flowFileUUID4)
                .setCurrentContentClaim("", "", "", null, 4l).build();
        final ProvenanceEventRecord record5 = createEventBuilder.setFlowFileUUID(flowFileUUID5)
                .setCurrentContentClaim("", "", "", null, 5l).build();

        // Register five events
        this.repo.registerEvents(Arrays.asList(record1, record2, record3, record4, record5));

        final Query query = new Query("testSubmitQueryMaxFileSize");

        query.setMinFileSize("3B");

        this.repo.submitQuery(query);

        final QuerySubmission results = this.repo
                .retrieveQuerySubmission("testSubmitQueryMaxFileSize");

        final List<ProvenanceEventRecord> matches = results.getResult().getMatchingEvents();

        assertNotNull(matches);
        assertEquals(3, matches.size());

        final Collection<String> uuids = Arrays.asList(flowFileUUID3, flowFileUUID4, flowFileUUID5);

        for (final ProvenanceEventRecord match : matches) {
            assertTrue(uuids.contains(match.getFlowFileUuid()));
        }
    }

    @Test
    public void testSubmitLineageComputation() {
        final String flowFileUUID1 = "cced57fb-699d-419a-bcb3-45078113b5f7";
        final String flowFileUUID2 = "2ec1f5de-4654-4558-b1d7-7daa6938eb04";
        final String flowFileUUID3 = "7fd72ca9-37ed-4cd7-bce3-f1ac700c8f1a";

        final ProvenanceEventBuilder createEventBuilder = new StandardProvenanceEventRecord.Builder();
        createEventBuilder.setEventType(ProvenanceEventType.CREATE);
        createEventBuilder.setTransitUri("nifi://unit-test");
        createEventBuilder.setComponentId("1234");
        createEventBuilder.setComponentType("myProcessor1");
        createEventBuilder.setEventTime(3);
        createEventBuilder.setCurrentContentClaim("", "", "", null, 1l);

        final ProvenanceEventRecord parent = createEventBuilder.setFlowFileUUID(flowFileUUID2)
                .build();

        final ProvenanceEventRecord child = createEventBuilder.setFlowFileUUID(flowFileUUID3)
                .addParentFlowFile(generateFlowFile(flowFileUUID2)).build();

        final ProvenanceEventRecord grandParent = createEventBuilder.setFlowFileUUID(flowFileUUID1)
                .addChildFlowFile(generateFlowFile(flowFileUUID2)).build();

        this.repo.registerEvents(Arrays.asList(grandParent, parent, child));

        final AsyncLineageSubmission submission = this.repo.submitLineageComputation(flowFileUUID2);
        final ComputeLineageSubmission results = this.repo.retrieveLineageSubmission(submission
                .getLineageIdentifier());

        final List<LineageNode> matches = results.getResult().getNodes();

        assertNotNull(matches);
        assertEquals(6, matches.size());

    }

    private FlowFile generateFlowFile(final String uuid) {
        return new FlowFile() {

            @Override
            public int compareTo(FlowFile o) {
                return 0;
            }

            @Override
            public long getId() {
                return -1l;
            }

            @Override
            public long getEntryDate() {
                return 0;
            }

            @Override
            public long getLineageStartDate() {
                return 0;
            }

            @Override
            public Long getLastQueueDate() {
                return null;
            }

            @Override
            public Set<String> getLineageIdentifiers() {
                return null;
            }

            @Override
            public boolean isPenalized() {
                return false;
            }

            @Override
            public String getAttribute(String key) {
                if (CoreAttributes.UUID.key().equals(key)) {
                    return uuid;
                }
                return null;
            }

            @Override
            public long getSize() {
                return 0;
            }

            @Override
            public Map<String, String> getAttributes() {
                return null;
            }

        };
    }

}
