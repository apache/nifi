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
package org.apache.nifi.controller.repository;


import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class StandardProvenanceReporterTest {

    @Test
    public void testDetailsRetainedWithDelegate() {
        final ProvenanceEventRepository mockRepo = Mockito.mock(ProvenanceEventRepository.class);
        final StandardProvenanceReporter reporter = new StandardProvenanceReporter(null, "1234", "TestProc", mockRepo, null);
        Mockito.when(mockRepo.eventBuilder()).thenReturn(new StandardProvenanceEventRecord.Builder());

        final FlowFile flowFile = new StandardFlowFileRecord.Builder().id(10L).addAttribute("uuid", "10").build();
        reporter.receive(flowFile, "test://noop", "These are details", -1);
        final Set<ProvenanceEventRecord> records = reporter.getEvents();
        assertNotNull(records);
        assertEquals(1, records.size());
        final ProvenanceEventRecord record = records.iterator().next();
        assertNotNull(record);
        assertEquals("test://noop", record.getTransitUri());
        assertNull(record.getSourceSystemFlowFileIdentifier());
        assertEquals("These are details", record.getDetails());
        assertEquals(-1L, record.getEventDuration());
    }

    @Test
    public void testSourceSystemFlowFileIdentifierRetainedWithDelegate() {
        final ProvenanceEventRepository mockRepo = Mockito.mock(ProvenanceEventRepository.class);
        final StandardProvenanceReporter reporter = new StandardProvenanceReporter(null, "1234", "TestProc", mockRepo, null);
        Mockito.when(mockRepo.eventBuilder()).thenReturn(new StandardProvenanceEventRecord.Builder());

        final FlowFile flowFile = new StandardFlowFileRecord.Builder().id(10L).addAttribute("uuid", "10").build();
        reporter.receive(flowFile, "test://noop", "urn:nifi:mock-uuid-value");
        final Set<ProvenanceEventRecord> records = reporter.getEvents();
        assertNotNull(records);
        assertEquals(1, records.size());
        final ProvenanceEventRecord record = records.iterator().next();
        assertNotNull(record);
        assertEquals("test://noop", record.getTransitUri());
        assertEquals("urn:nifi:mock-uuid-value", record.getSourceSystemFlowFileIdentifier());
        assertNull(record.getDetails());
        assertEquals(-1L, record.getEventDuration());
    }

    @Test
    public void testEnrichEvents() {
        final ProvenanceEventRepository mockRepo = Mockito.mock(ProvenanceEventRepository.class);
        final ProvenanceEventEnricher enricher = Mockito.mock(ProvenanceEventEnricher.class);
        final StandardProvenanceReporter reporter = new StandardProvenanceReporter(null, "1234", "TestProc", mockRepo, enricher);
        Mockito.when(mockRepo.eventBuilder()).thenReturn(new StandardProvenanceEventRecord.Builder());

        final FlowFile flowFile = new StandardFlowFileRecord.Builder().id(10L).addAttribute("uuid", "10").build();
        final FlowFile childFlowFile = new StandardFlowFileRecord.Builder().id(11L).addAttribute("uuid", "11").build();
        reporter.send(flowFile, "test://noop");
        reporter.upload(flowFile, 0, "test://noop");
        reporter.clone(flowFile, childFlowFile);
        verify(enricher, times(3)).enrich(any(), eq(flowFile), anyLong());
    }
}
