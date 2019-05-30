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
package org.apache.nifi.integration.provenance;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ProvenanceEventsIT extends FrameworkIntegrationTest {

    @Test
    public void testCreateEventIfNewFlowFileWithoutReceive() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.create();

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("test", "integration");
            attrs.put("integration", "true");

            flowFile = session.putAllAttributes(flowFile, attrs);
            session.transfer(flowFile, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(createProcessor, getTerminateAllProcessor(), REL_SUCCESS);
        triggerOnce(createProcessor);

        // There should be exactly 1 event.
        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(0L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.CREATE, firstEvent.getEventType());
        assertEquals("integration", firstEvent.getAttribute("test"));
        assertEquals("true", firstEvent.getAttribute("integration"));
    }

    @Test
    public void testNoCreateEventIfReceiveExplicitlyCreated() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.create();

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("test", "integration");
            attrs.put("integration", "true");

            flowFile = session.putAllAttributes(flowFile, attrs);
            session.getProvenanceReporter().receive(flowFile, "nifi://unit.test");
            session.transfer(flowFile, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(createProcessor, getTerminateAllProcessor(), REL_SUCCESS);
        triggerOnce(createProcessor);

        // There should be exactly 1 event.
        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(0L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.RECEIVE, firstEvent.getEventType());
        assertEquals("integration", firstEvent.getAttribute("test"));
        assertEquals("true", firstEvent.getAttribute("integration"));
        assertEquals("nifi://unit.test", firstEvent.getTransitUri());
    }

    @Test
    public void testDropEventIfRoutedToAutoTerminatedRelationship() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.create();

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("test", "integration");
            attrs.put("integration", "true");

            flowFile = session.putAllAttributes(flowFile, attrs);
            session.transfer(flowFile, REL_SUCCESS);
        }, REL_SUCCESS);

        createProcessor.setAutoTerminatedRelationships(Collections.singleton(REL_SUCCESS));

        triggerOnce(createProcessor);

        // There should be exactly 1 event.
        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(1L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.CREATE, firstEvent.getEventType());
        assertEquals("integration", firstEvent.getAttribute("test"));
        assertEquals("true", firstEvent.getAttribute("integration"));

        final ProvenanceEventRecord secondEvent = provRepo.getEvent(1L);
        assertEquals(ProvenanceEventType.DROP, secondEvent.getEventType());
        assertEquals("integration", secondEvent.getAttribute("test"));
        assertEquals("true", secondEvent.getAttribute("integration"));
    }

    @Test
    public void testNoEventsIfExplicitlyRemoved() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.create();

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("test", "integration");
            attrs.put("integration", "true");

            flowFile = session.putAllAttributes(flowFile, attrs);
            session.remove(flowFile);
        }, REL_SUCCESS);

        createProcessor.setAutoTerminatedRelationships(Collections.singleton(REL_SUCCESS));

        triggerOnce(createProcessor);

        // There should be exactly 1 event.
        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(-1L, provRepo.getMaxEventId().longValue());
        assertTrue(provRepo.getEvents(0L, 1000).isEmpty());
    }

    @Test
    public void testAttributesModifiedIfNothingElse() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createGenerateProcessor(0);

        final ProcessorNode updateProcessor = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.get();

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("test", "integration");
            attrs.put("integration", "true");

            flowFile = session.putAllAttributes(flowFile, attrs);
            session.transfer(flowFile, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(createProcessor, updateProcessor, REL_SUCCESS);
        connect(updateProcessor, getTerminateAllProcessor(), REL_SUCCESS);

        triggerOnce(createProcessor);
        triggerOnce(updateProcessor);

        // There should be exactly 1 event.
        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(1L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.CREATE, firstEvent.getEventType());
        assertNull(firstEvent.getAttribute("test"));
        assertNull(firstEvent.getAttribute("integration"));

        final ProvenanceEventRecord secondEvent = provRepo.getEvent(1L);
        assertEquals(ProvenanceEventType.ATTRIBUTES_MODIFIED, secondEvent.getEventType());
        assertEquals("integration", secondEvent.getAttribute("test"));
        assertEquals("true", secondEvent.getAttribute("integration"));
    }

    @Test
    public void testAttributesModifiedNotCreatedIfProcessorEmitsIt() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createGenerateProcessor(0);

        final ProcessorNode updateProcessor = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.get();

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("test", "integration");
            attrs.put("integration", "true");

            flowFile = session.putAllAttributes(flowFile, attrs);
            session.getProvenanceReporter().modifyAttributes(flowFile, "Unit Test Details");
            session.transfer(flowFile, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(createProcessor, updateProcessor, REL_SUCCESS);
        connect(updateProcessor, getTerminateAllProcessor(), REL_SUCCESS);

        triggerOnce(createProcessor);
        triggerOnce(updateProcessor);

        // There should be exactly 1 event.
        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(1L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.CREATE, firstEvent.getEventType());
        assertNull(firstEvent.getAttribute("test"));
        assertNull(firstEvent.getAttribute("integration"));

        final ProvenanceEventRecord secondEvent = provRepo.getEvent(1L);
        assertEquals(ProvenanceEventType.ATTRIBUTES_MODIFIED, secondEvent.getEventType());
        assertEquals("integration", secondEvent.getAttribute("test"));
        assertEquals("true", secondEvent.getAttribute("integration"));
        assertEquals("Unit Test Details", secondEvent.getDetails());
    }

    @Test
    public void testAttributesModifiedNotCreatedIfProcessorEmitsOtherEvent() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createGenerateProcessor(0);

        final ProcessorNode updateProcessor = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.get();

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("test", "integration");
            attrs.put("integration", "true");

            flowFile = session.putAllAttributes(flowFile, attrs);
            session.getProvenanceReporter().fetch(flowFile, "nifi://unit.test");
            session.transfer(flowFile, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(createProcessor, updateProcessor, REL_SUCCESS);
        connect(updateProcessor, getTerminateAllProcessor(), REL_SUCCESS);

        triggerOnce(createProcessor);
        triggerOnce(updateProcessor);

        // There should be exactly 1 event.
        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(1L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.CREATE, firstEvent.getEventType());
        assertNull(firstEvent.getAttribute("test"));
        assertNull(firstEvent.getAttribute("integration"));

        final ProvenanceEventRecord secondEvent = provRepo.getEvent(1L);
        assertEquals(ProvenanceEventType.FETCH, secondEvent.getEventType());
        assertEquals("integration", secondEvent.getAttribute("test"));
        assertEquals("true", secondEvent.getAttribute("integration"));
        assertEquals("nifi://unit.test", secondEvent.getTransitUri());
    }


    @Test
    public void testAttributesModifiedNotCreatedIfContentModified() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createGenerateProcessor(0);

        final ProcessorNode updateProcessor = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.get();

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("test", "integration");
            attrs.put("integration", "true");

            flowFile = session.putAllAttributes(flowFile, attrs);
            flowFile = session.write(flowFile, out -> out.write('A'));
            session.transfer(flowFile, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(createProcessor, updateProcessor, REL_SUCCESS);
        connect(updateProcessor, getTerminateAllProcessor(), REL_SUCCESS);

        triggerOnce(createProcessor);
        triggerOnce(updateProcessor);

        // There should be exactly 1 event.
        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(1L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.CREATE, firstEvent.getEventType());
        assertNull(firstEvent.getAttribute("test"));
        assertNull(firstEvent.getAttribute("integration"));

        final ProvenanceEventRecord secondEvent = provRepo.getEvent(1L);
        assertEquals(ProvenanceEventType.CONTENT_MODIFIED, secondEvent.getEventType());
        assertEquals("integration", secondEvent.getAttribute("test"));
        assertEquals("true", secondEvent.getAttribute("integration"));
    }


    @Test
    public void testNoAttributesModifiedOnJoin() throws ExecutionException, InterruptedException, IOException {
        testJoin(false);
    }

    @Test
    public void testNoAttributesModifiedOnJoinWithExplicitJoinEvent() throws ExecutionException, InterruptedException, IOException {
        testJoin(true);
    }

    private void testJoin(final boolean emitJoinEventExplicitly) throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode createProcessor = createGenerateProcessor(0);

        final ProcessorNode joinProcessor = createProcessorNode((context, session) -> {
            final List<FlowFile> originals = new ArrayList<>();
            FlowFile flowFile;
            while ((flowFile = session.get()) != null) {
                originals.add(flowFile);
            }

            FlowFile merged = session.create(originals);

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("test", "integration");
            attrs.put("integration", "true");

            merged = session.putAllAttributes(merged, attrs);
            merged = session.write(merged, out -> out.write('A'));

            if (emitJoinEventExplicitly) {
                session.getProvenanceReporter().join(originals, merged);
            }

            session.remove(originals);
            session.transfer(merged, REL_SUCCESS);
            session.getProvenanceReporter().route(merged, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(createProcessor, joinProcessor, REL_SUCCESS);
        joinProcessor.setAutoTerminatedRelationships(Collections.singleton(REL_SUCCESS));

        for (int i=0; i < 3; i++) {
            triggerOnce(createProcessor);
        }

        triggerOnce(joinProcessor);

        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(8L, provRepo.getMaxEventId().longValue());

        // Crete events are from the first 'generate' processor.
        for (int i=0; i < 3; i++) {
            assertEquals(ProvenanceEventType.CREATE, provRepo.getEvent(i).getEventType());
        }

        // Any FORK/JOIN events will occur first in the Process Session to ensure that any other events that reference the FlowFile
        // that is created as a result have a FlowFile to actually reference.
        final ProvenanceEventRecord joinEvent = provRepo.getEvent(3);
        assertEquals(ProvenanceEventType.JOIN, joinEvent.getEventType());
        assertEquals("integration", joinEvent.getAttribute("test"));
        assertEquals("true", joinEvent.getAttribute("integration"));
        assertEquals(3, joinEvent.getParentUuids().size());
        assertEquals(1, joinEvent.getChildUuids().size());
        assertEquals(joinEvent.getFlowFileUuid(), joinEvent.getChildUuids().get(0));

        // Next event to occur in the Processor is the DORP event
        for (int i=4; i < 7; i++) {
            assertEquals(ProvenanceEventType.DROP, provRepo.getEvent(i).getEventType());
        }

        // Finally Processor will ROUTE the FlowFile
        final ProvenanceEventRecord routeEvent = provRepo.getEvent(7);
        assertEquals(ProvenanceEventType.ROUTE, routeEvent.getEventType());
        assertEquals("success", routeEvent.getRelationship());
        assertEquals("integration", routeEvent.getAttribute("test"));
        assertEquals("true", routeEvent.getAttribute("integration"));

        // Merged FlowFile is then auto-terminated.
        final ProvenanceEventRecord dropJoinedEvent = provRepo.getEvent(8);
        assertEquals(ProvenanceEventType.DROP, dropJoinedEvent.getEventType());
        assertEquals("integration", dropJoinedEvent.getAttribute("test"));
        assertEquals("true", dropJoinedEvent.getAttribute("integration"));
    }

    @Test
    public void testForkAutoGenerated() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode generateProcessor = createGenerateProcessor(0);
        final ProcessorNode forkProcessor = createProcessorNode((context, session) -> {
            FlowFile original = session.get();

            for (int i=0; i < 3; i++) {
                FlowFile child = session.create(original);
                child = session.putAttribute(child, "i", String.valueOf(i));
                session.transfer(child, REL_SUCCESS);
            }

            session.remove(original);
        }, REL_SUCCESS);

        connect(generateProcessor, forkProcessor, REL_SUCCESS);
        connect(forkProcessor, getTerminateAllProcessor(), REL_SUCCESS);

        triggerOnce(generateProcessor);
        triggerOnce(forkProcessor);

        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(2L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.CREATE, firstEvent.getEventType());
        assertNull(firstEvent.getAttribute("test"));
        assertNull(firstEvent.getAttribute("integration"));

        final ProvenanceEventRecord secondEvent = provRepo.getEvent(1L);
        assertEquals(ProvenanceEventType.FORK, secondEvent.getEventType());
        assertEquals(1, secondEvent.getParentUuids().size());
        assertEquals(3, secondEvent.getChildUuids().size());
        assertEquals(secondEvent.getFlowFileUuid(), secondEvent.getParentUuids().get(0));

        final ProvenanceEventRecord thirdEvent = provRepo.getEvent(2L);
        assertEquals(ProvenanceEventType.DROP, thirdEvent.getEventType());
    }

    @Test
    public void testCloneOnMultipleConnectionsForRelationship() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode generateProcessor = createGenerateProcessor(0);
        final ProcessorNode passThroughProcessor = createProcessorNode((context, session) -> {
            FlowFile original = session.get();
            session.transfer(original, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(generateProcessor, passThroughProcessor, REL_SUCCESS);
        connect(passThroughProcessor, getTerminateProcessor(), REL_SUCCESS);
        connect(passThroughProcessor, getTerminateAllProcessor(), REL_SUCCESS);

        triggerOnce(generateProcessor);
        triggerOnce(passThroughProcessor);

        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(1L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.CREATE, firstEvent.getEventType());

        final ProvenanceEventRecord secondEvent = provRepo.getEvent(1L);
        assertEquals(ProvenanceEventType.CLONE, secondEvent.getEventType());
        assertEquals(1, secondEvent.getParentUuids().size());
        assertEquals(1, secondEvent.getChildUuids().size());
    }

    @Test
    public void testCloneOnMultipleConnectionsForRelationshipIncludesUpdatedAttributes() throws ExecutionException, InterruptedException, IOException {
        final ProcessorNode generateProcessor = createGenerateProcessor(0);
        final ProcessorNode passThroughProcessor = createProcessorNode((context, session) -> {
            FlowFile original = session.get();
            original = session.putAttribute(original, "test", "integration");

            session.transfer(original, REL_SUCCESS);
        }, REL_SUCCESS);

        connect(generateProcessor, passThroughProcessor, REL_SUCCESS);
        connect(passThroughProcessor, getTerminateProcessor(), REL_SUCCESS);
        connect(passThroughProcessor, getTerminateAllProcessor(), REL_SUCCESS);

        triggerOnce(generateProcessor);
        triggerOnce(passThroughProcessor);

        final ProvenanceEventRepository provRepo = getProvenanceRepository();
        assertEquals(1L, provRepo.getMaxEventId().longValue());

        final ProvenanceEventRecord firstEvent = provRepo.getEvent(0L);
        assertEquals(ProvenanceEventType.CREATE, firstEvent.getEventType());

        final ProvenanceEventRecord secondEvent = provRepo.getEvent(1L);
        assertEquals(ProvenanceEventType.CLONE, secondEvent.getEventType());
        assertEquals(1, secondEvent.getParentUuids().size());
        assertEquals(1, secondEvent.getChildUuids().size());
        assertEquals("integration", secondEvent.getAttribute("test"));
    }

}
