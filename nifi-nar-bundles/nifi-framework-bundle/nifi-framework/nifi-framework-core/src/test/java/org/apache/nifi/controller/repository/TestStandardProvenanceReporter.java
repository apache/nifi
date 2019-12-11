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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.provenance.FlowFileAcquisitionMethod;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

public class TestStandardProvenanceReporter {

    @Test
    @Ignore
    public void testDuplicatesIgnored() {
        final ProvenanceEventRepository mockRepo = Mockito.mock(ProvenanceEventRepository.class);
        final StandardProvenanceReporter reporter = new StandardProvenanceReporter(null, "1234", "TestProc", mockRepo, null);

        final List<FlowFile> parents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final FlowFile ff = new StandardFlowFileRecord.Builder().id(i).addAttribute("uuid", String.valueOf(i)).build();
            parents.add(ff);
        }

        final FlowFile flowFile = new StandardFlowFileRecord.Builder().id(10L).addAttribute("uuid", "10").build();

        reporter.fork(flowFile, parents);
        reporter.fork(flowFile, parents);

        final Set<ProvenanceEventRecord> records = reporter.getEvents();
        assertEquals(11, records.size());   // 1 for each parent in the spawn and 1 for the spawn itself

        final FlowFile firstParent = parents.get(0);
        parents.clear();
        parents.add(firstParent);
        reporter.fork(flowFile, parents);
        // 1 more emitted for the spawn event containing the child but not for the parent because that one has already been emitted
        assertEquals(12, reporter.getEvents().size());
    }

    @Test
    public void testFlowFileAcquisitionMethodSetOnReceiveFetch() {
        final RepositoryContext repositoryContext = Mockito.mock(RepositoryContext.class);
        final Connectable connectable = Mockito.mock(Connectable.class);
        final ProvenanceEventRepository eventRepository = Mockito.mock(ProvenanceEventRepository.class);
        Mockito.when(repositoryContext.getConnectable()).thenReturn(connectable);
        Mockito.when(connectable.getConnectableType()).thenReturn(ConnectableType.INPUT_PORT);
        Mockito.when(eventRepository.eventBuilder()).thenReturn(new StandardProvenanceEventRecord.Builder());

        final StandardProcessSession session = new StandardProcessSession(repositoryContext, () -> false);
        final StandardProvenanceReporter provenanceReporter = new StandardProvenanceReporter(session, "processorId", "processorType", eventRepository, null);

        final FlowFile flowFile = session.create();
        provenanceReporter.receive(flowFile, "transitUri", "sourceSystemId", "details", FlowFileAcquisitionMethod.ACTIVE_QUERY, 100L);
        provenanceReporter.fetch(flowFile, "transitUri", "details", FlowFileAcquisitionMethod.ACTIVE_QUERY, 100L);

        final Set<ProvenanceEventRecord> events = provenanceReporter.getEvents();
        assertEquals(2, events.size());
        events.forEach(
                event -> assertEquals(FlowFileAcquisitionMethod.ACTIVE_QUERY, event.getFlowFileAcquisitionMethod())
        );
    }
}
