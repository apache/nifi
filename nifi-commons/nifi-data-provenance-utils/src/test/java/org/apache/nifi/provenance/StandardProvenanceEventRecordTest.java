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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StandardProvenanceEventRecordTest {

    private static final String FLOWFILE_ID_1 = "FF-1";
    private static final String FLOWFILE_ID_2 = "FF-2";

    @Test
    void testEqualsAndHashCodeWithChildFlowFilesInDifferentOrder() {
        final ProvenanceEventRecord event1 = initProvenanceEvent()
                .setChildUuids(List.of(FLOWFILE_ID_1, FLOWFILE_ID_2))
                .build();
        final ProvenanceEventRecord event2 = initProvenanceEvent()
                .setChildUuids(List.of(FLOWFILE_ID_2, FLOWFILE_ID_1))
                .build();

        assertEquals(event1, event2);
        assertEquals(event1.hashCode(), event2.hashCode());
    }

    @Test
    void testEqualsAndHashCodeWithParentFlowFilesInDifferentOrder() {
        final ProvenanceEventRecord event1 = initProvenanceEvent()
                .setParentUuids(List.of(FLOWFILE_ID_1, FLOWFILE_ID_2))
                .build();
        final ProvenanceEventRecord event2 = initProvenanceEvent()
                .setParentUuids(List.of(FLOWFILE_ID_2, FLOWFILE_ID_1))
                .build();

        assertEquals(event1, event2);
        assertEquals(event1.hashCode(), event2.hashCode());
    }

    private StandardProvenanceEventRecord.Builder initProvenanceEvent() {
        return new StandardProvenanceEventRecord.Builder()
                .setEventType(ProvenanceEventType.FORK)
                .setComponentId("0")
                .setComponentType("processor")
                .setFlowFileUUID("FF")
                .setCurrentContentClaim("container", "section", "identifier", 0L, 0);
    }
}
