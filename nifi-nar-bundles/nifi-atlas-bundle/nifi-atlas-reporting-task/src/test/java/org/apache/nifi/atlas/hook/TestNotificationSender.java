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
package org.apache.nifi.atlas.hook;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.AtlasUtils;
import org.apache.nifi.atlas.NiFiAtlasClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_GUID;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_TYPENAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_DATASET;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;
import static org.apache.nifi.atlas.hook.NiFiAtlasHook.NIFI_USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNotificationSender {

    private static final Logger logger = LoggerFactory.getLogger(TestNotificationSender.class);

    private static class Notifier implements Consumer<List<HookNotification.HookNotificationMessage>> {
        private final List<List<HookNotification.HookNotificationMessage>> notifications = new ArrayList<>();
        @Override
        public void accept(List<HookNotification.HookNotificationMessage> messages) {
            logger.info("notified at {}, {}", notifications.size(), messages);
            notifications.add(messages);
        }
    }

    @Test
    public void testZeroMessage() {
        final NotificationSender sender = new NotificationSender();
        final List<HookNotification.HookNotificationMessage> messages = Collections.emptyList();
        final Notifier notifier = new Notifier();
        sender.send(messages, notifier);
        assertEquals(0, notifier.notifications.get(0).size());
        assertEquals(0, notifier.notifications.get(1).size());
    }

    private Referenceable createRef(String type, String qname) {
        final Referenceable ref = new Referenceable(type);
        ref.set(ATTR_QUALIFIED_NAME, qname);
        return ref;
    }

    @SuppressWarnings("unchecked")
    private void assertCreateMessage(Notifier notifier, int notificationIndex, Referenceable ... expects) {
        assertTrue(notifier.notifications.size() > notificationIndex);
        final List<HookNotification.HookNotificationMessage> messages = notifier.notifications.get(notificationIndex);
        assertEquals(1, messages.size());
        final HookNotification.EntityCreateRequest message = (HookNotification.EntityCreateRequest) messages.get(0);
        assertEquals(expects.length, message.getEntities().size());

        // The use of 'flatMap' at NotificationSender does not preserve actual entities order.
        // Use typed qname map to assert regardless of ordering.
        final Map<String, Referenceable> entities = message.getEntities().stream().collect(Collectors.toMap(
                ref -> AtlasUtils.toTypedQualifiedName(ref.getTypeName(), (String) ref.get(ATTR_QUALIFIED_NAME)), ref -> ref));

        boolean hasFlowPathSeen = false;
        for (int i = 0; i < expects.length; i++) {
            final Referenceable expect = expects[i];
            final String typeName = expect.getTypeName();
            final Referenceable actual = entities.get(AtlasUtils.toTypedQualifiedName(typeName, (String) expect.get(ATTR_QUALIFIED_NAME)));
            assertNotNull(actual);
            assertEquals(typeName, actual.getTypeName());
            assertEquals(expect.get(ATTR_QUALIFIED_NAME), actual.get(ATTR_QUALIFIED_NAME));

            if (TYPE_NIFI_FLOW_PATH.equals(typeName)) {
                assertIOReferences(expect, actual, ATTR_INPUTS);
                assertIOReferences(expect, actual, ATTR_OUTPUTS);
                hasFlowPathSeen = true;
            } else {
                assertFalse("Types other than nifi_flow_path should be created before any nifi_flow_path entity.", hasFlowPathSeen);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void assertIOReferences(Referenceable expect, Referenceable actual, String attrName) {
        final Collection<Referenceable> expectedRefs = (Collection<Referenceable>) expect.get(attrName);
        if (expectedRefs != null) {
            final Collection<Referenceable> actualRefs = (Collection<Referenceable>) actual.get(attrName);
            assertEquals(expectedRefs.size(), actualRefs.size());
            final Iterator<Referenceable> actualIterator = actualRefs.iterator();
            for (Referenceable expectedRef : expectedRefs) {
                final Referenceable actualRef = actualIterator.next();
                assertEquals(expectedRef.getTypeName(), actualRef.getTypeName());
                assertEquals(expectedRef.get(ATTR_QUALIFIED_NAME), actualRef.get(ATTR_QUALIFIED_NAME));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void assertUpdateFlowPathMessage(Notifier notifier, int notificationIndex, Referenceable ... expects) {
        assertTrue(notifier.notifications.size() > notificationIndex);
        final List<HookNotification.HookNotificationMessage> messages = notifier.notifications.get(notificationIndex);
        assertEquals(expects.length, messages.size());
        for (int i = 0; i < expects.length; i++) {
            final Referenceable expect = expects[i];
            final HookNotification.EntityPartialUpdateRequest actual = (HookNotification.EntityPartialUpdateRequest) messages.get(i);
            assertEquals(expect.getTypeName(), actual.getTypeName());
            assertEquals(ATTR_QUALIFIED_NAME, actual.getAttribute());
            assertEquals(expect.get(ATTR_QUALIFIED_NAME), actual.getAttributeValue());

            final Collection expIn = (Collection) expect.get(ATTR_INPUTS);
            final Collection expOut = (Collection) expect.get(ATTR_OUTPUTS);
            assertTrue(expIn.containsAll((Collection) actual.getEntity().get(ATTR_INPUTS)));
            assertTrue(expOut.containsAll((Collection) actual.getEntity().get(ATTR_OUTPUTS)));
        }
    }

    @Test
    public void testOneCreateDataSetMessage() {
        final NotificationSender sender = new NotificationSender();
        final Referenceable queue1 = createRef(TYPE_NIFI_QUEUE, "queue1@test");
        final List<HookNotification.HookNotificationMessage> messages = Collections.singletonList(
                new HookNotification.EntityCreateRequest(NIFI_USER, queue1));
        final Notifier notifier = new Notifier();
        sender.send(messages, notifier);

        // EntityCreateRequest containing nifi_queue.
        assertCreateMessage(notifier, 0, queue1);
    }

    @Test
    public void testCreateDataSetMessageDeduplication() {
        // Simulating complete path, that there were 4 FlowFiles went through the same partial flow.
        // FF1 was ingested from data1, and so was FF2 from data2.
        // Then FF1 was FORKed to FF11 and FF12, then sent to data11 and data12.
        // Similarly FF2 was FORKed to FF21 and FF22, then sent to data21 and data22.
        // FF3 went through the same as FF1, so did FF4 as FF2.
        // All of those provenance events were processed within a single ReportLineageToAtlas cycle.

        // FF1: data1 -> pathA1 -> FORK11 (FF11) -> pathB11 -> data11
        //                      -> FORK12 (FF12) -> pathB12 -> data12
        // FF2: data2 -> pathA2 -> FORK21 (FF21) -> pathB21 -> data21
        //                      -> FORK22 (FF22) -> pathB22 -> data22
        // FF3: data1 -> pathA1 -> FORK11 (FF31) -> pathB11 -> data11
        //                      -> FORK12 (FF32) -> pathB12 -> data12
        // FF4: data2 -> pathA2 -> FORK21 (FF41) -> pathB21 -> data21
        //                      -> FORK22 (FF42) -> pathB22 -> data22

        // As a result, following lineages are reported to Atlas:
        // data1 -> pathA1 -> FORK11 -> pathB11 -> data11
        //                 -> FORK12 -> pathB12 -> data12
        // data2 -> pathA2 -> FORK21 -> pathB21 -> data21
        //                 -> FORK22 -> pathB22 -> data22

        final NotificationSender sender = new NotificationSender();

        // From FF1
        final Referenceable ff1_data1 = createRef(TYPE_DATASET, "data1@test");
        final Referenceable ff1_data11 = createRef(TYPE_DATASET, "data11@test");
        final Referenceable ff1_data12 = createRef(TYPE_DATASET, "data12@test");
        final Referenceable ff1_pathA11 = createRef(TYPE_NIFI_FLOW_PATH, "A::0001@test");
        final Referenceable ff1_pathA12 = createRef(TYPE_NIFI_FLOW_PATH, "A::0001@test");
        final Referenceable ff1_fork11 = createRef(TYPE_NIFI_QUEUE, "B::0011@test");
        final Referenceable ff1_fork12 = createRef(TYPE_NIFI_QUEUE, "B::0012@test");
        final Referenceable ff1_pathB11 = createRef(TYPE_NIFI_FLOW_PATH, "B::0011@test");
        final Referenceable ff1_pathB12 = createRef(TYPE_NIFI_FLOW_PATH, "B::0012@test");
        // From FF11
        ff1_pathA11.set(ATTR_INPUTS, singleton(ff1_data1));
        ff1_pathA11.set(ATTR_OUTPUTS, singleton(ff1_fork11));
        ff1_pathB11.set(ATTR_INPUTS, singleton(ff1_fork11));
        ff1_pathB11.set(ATTR_OUTPUTS, singleton(ff1_data11));
        // From FF12
        ff1_pathA12.set(ATTR_INPUTS, singleton(ff1_data1));
        ff1_pathA12.set(ATTR_OUTPUTS, singleton(ff1_fork12));
        ff1_pathB12.set(ATTR_INPUTS, singleton(ff1_fork12));
        ff1_pathB12.set(ATTR_OUTPUTS, singleton(ff1_data12));

        // From FF2
        final Referenceable ff2_data2 = createRef(TYPE_DATASET, "data2@test");
        final Referenceable ff2_data21 = createRef(TYPE_DATASET, "data21@test");
        final Referenceable ff2_data22 = createRef(TYPE_DATASET, "data22@test");
        final Referenceable ff2_pathA21 = createRef(TYPE_NIFI_FLOW_PATH, "A::0002@test");
        final Referenceable ff2_pathA22 = createRef(TYPE_NIFI_FLOW_PATH, "A::0002@test");
        final Referenceable ff2_fork21 = createRef(TYPE_NIFI_QUEUE, "B::0021@test");
        final Referenceable ff2_fork22 = createRef(TYPE_NIFI_QUEUE, "B::0022@test");
        final Referenceable ff2_pathB21 = createRef(TYPE_NIFI_FLOW_PATH, "B::0021@test");
        final Referenceable ff2_pathB22 = createRef(TYPE_NIFI_FLOW_PATH, "B::0022@test");
        // From FF21
        ff2_pathA21.set(ATTR_INPUTS, singleton(ff2_data2));
        ff2_pathA21.set(ATTR_OUTPUTS, singleton(ff2_fork21));
        ff2_pathB21.set(ATTR_INPUTS, singleton(ff2_fork21));
        ff2_pathB21.set(ATTR_OUTPUTS, singleton(ff2_data21));
        // From FF22
        ff2_pathA22.set(ATTR_INPUTS, singleton(ff2_data2));
        ff2_pathA22.set(ATTR_OUTPUTS, singleton(ff2_fork22));
        ff2_pathB22.set(ATTR_INPUTS, singleton(ff2_fork22));
        ff2_pathB22.set(ATTR_OUTPUTS, singleton(ff2_data22));

        // From FF3
        final Referenceable ff3_data1 = createRef(TYPE_DATASET, "data1@test");
        final Referenceable ff3_data11 = createRef(TYPE_DATASET, "data11@test");
        final Referenceable ff3_data12 = createRef(TYPE_DATASET, "data12@test");
        final Referenceable ff3_pathA11 = createRef(TYPE_NIFI_FLOW_PATH, "A::0001@test");
        final Referenceable ff3_pathA12 = createRef(TYPE_NIFI_FLOW_PATH, "A::0001@test");
        final Referenceable ff3_fork11 = createRef(TYPE_NIFI_QUEUE, "B::0011@test");
        final Referenceable ff3_fork12 = createRef(TYPE_NIFI_QUEUE, "B::0012@test");
        final Referenceable ff3_pathB11 = createRef(TYPE_NIFI_FLOW_PATH, "B::0011@test");
        final Referenceable ff3_pathB12 = createRef(TYPE_NIFI_FLOW_PATH, "B::0012@test");
        // From FF31
        ff3_pathA11.set(ATTR_INPUTS, singleton(ff3_data1));
        ff3_pathA11.set(ATTR_OUTPUTS, singleton(ff3_fork11));
        ff3_pathB11.set(ATTR_INPUTS, singleton(ff3_fork11));
        ff3_pathB11.set(ATTR_OUTPUTS, singleton(ff3_data11));
        // From FF32
        ff3_pathA12.set(ATTR_INPUTS, singleton(ff3_data1));
        ff3_pathA12.set(ATTR_OUTPUTS, singleton(ff3_fork12));
        ff3_pathB12.set(ATTR_INPUTS, singleton(ff3_fork12));
        ff3_pathB12.set(ATTR_OUTPUTS, singleton(ff3_data12));

        // From FF4
        final Referenceable ff4_data2 = createRef(TYPE_DATASET, "data2@test");
        final Referenceable ff4_data21 = createRef(TYPE_DATASET, "data21@test");
        final Referenceable ff4_data22 = createRef(TYPE_DATASET, "data22@test");
        final Referenceable ff4_pathA21 = createRef(TYPE_NIFI_FLOW_PATH, "A::0002@test");
        final Referenceable ff4_pathA22 = createRef(TYPE_NIFI_FLOW_PATH, "A::0002@test");
        final Referenceable ff4_fork21 = createRef(TYPE_NIFI_QUEUE, "B::0021@test");
        final Referenceable ff4_fork22 = createRef(TYPE_NIFI_QUEUE, "B::0022@test");
        final Referenceable ff4_pathB21 = createRef(TYPE_NIFI_FLOW_PATH, "B::0021@test");
        final Referenceable ff4_pathB22 = createRef(TYPE_NIFI_FLOW_PATH, "B::0022@test");
        // From FF41
        ff4_pathA21.set(ATTR_INPUTS, singleton(ff4_data2));
        ff4_pathA21.set(ATTR_OUTPUTS, singleton(ff4_fork21));
        ff4_pathB21.set(ATTR_INPUTS, singleton(ff4_fork21));
        ff4_pathB21.set(ATTR_OUTPUTS, singleton(ff4_data21));
        // From FF42
        ff4_pathA22.set(ATTR_INPUTS, singleton(ff4_data2));
        ff4_pathA22.set(ATTR_OUTPUTS, singleton(ff4_fork22));
        ff4_pathB22.set(ATTR_INPUTS, singleton(ff4_fork22));
        ff4_pathB22.set(ATTR_OUTPUTS, singleton(ff4_data22));


        final List<HookNotification.HookNotificationMessage> messages = asList(
                new HookNotification.EntityCreateRequest(NIFI_USER, ff1_data1),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff1_data11),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff1_data12),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff1_pathA11),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff1_pathA12),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff1_fork11),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff1_fork12),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff1_pathB11),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff1_pathB12),

                new HookNotification.EntityCreateRequest(NIFI_USER, ff2_data2),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff2_data21),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff2_data22),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff2_pathA21),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff2_pathA22),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff2_fork21),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff2_fork22),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff2_pathB21),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff2_pathB22),

                new HookNotification.EntityCreateRequest(NIFI_USER, ff3_data1),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff3_data11),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff3_data12),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff3_pathA11),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff3_pathA12),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff3_fork11),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff3_fork12),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff3_pathB11),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff3_pathB12),

                new HookNotification.EntityCreateRequest(NIFI_USER, ff4_data2),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff4_data21),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff4_data22),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff4_pathA21),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff4_pathA22),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff4_fork21),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff4_fork22),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff4_pathB21),
                new HookNotification.EntityCreateRequest(NIFI_USER, ff4_pathB22)
        );

        final Notifier notifier = new Notifier();
        sender.send(messages, notifier);

        // EntityCreateRequest, same entities get de-duplicated. nifi_flow_path is created after other types.
        final Referenceable r_data1 = createRef(TYPE_DATASET, "data1@test");
        final Referenceable r_data11 = createRef(TYPE_DATASET, "data11@test");
        final Referenceable r_data12 = createRef(TYPE_DATASET, "data12@test");
        final Referenceable r_pathA1 = createRef(TYPE_NIFI_FLOW_PATH, "A::0001@test");
        final Referenceable r_fork11 = createRef(TYPE_NIFI_QUEUE, "B::0011@test");
        final Referenceable r_fork12 = createRef(TYPE_NIFI_QUEUE, "B::0012@test");
        final Referenceable r_pathB11 = createRef(TYPE_NIFI_FLOW_PATH, "B::0011@test");
        final Referenceable r_pathB12 = createRef(TYPE_NIFI_FLOW_PATH, "B::0012@test");
        r_pathA1.set(ATTR_INPUTS, singleton(r_data1));
        r_pathA1.set(ATTR_OUTPUTS, asList(r_fork11, r_fork12));
        r_pathB11.set(ATTR_INPUTS, singleton(r_fork11));
        r_pathB11.set(ATTR_OUTPUTS, singleton(r_data11));
        r_pathB12.set(ATTR_INPUTS, singleton(r_fork12));
        r_pathB12.set(ATTR_OUTPUTS, singleton(r_data12));

        final Referenceable r_data2 = createRef(TYPE_DATASET, "data2@test");
        final Referenceable r_data21 = createRef(TYPE_DATASET, "data21@test");
        final Referenceable r_data22 = createRef(TYPE_DATASET, "data22@test");
        final Referenceable r_pathA2 = createRef(TYPE_NIFI_FLOW_PATH, "A::0002@test");
        final Referenceable r_fork21 = createRef(TYPE_NIFI_QUEUE, "B::0021@test");
        final Referenceable r_fork22 = createRef(TYPE_NIFI_QUEUE, "B::0022@test");
        final Referenceable r_pathB21 = createRef(TYPE_NIFI_FLOW_PATH, "B::0021@test");
        final Referenceable r_pathB22 = createRef(TYPE_NIFI_FLOW_PATH, "B::0022@test");
        r_pathA2.set(ATTR_INPUTS, singleton(r_data2));
        r_pathA2.set(ATTR_OUTPUTS, asList(r_fork21, r_fork22));
        r_pathB21.set(ATTR_INPUTS, singleton(r_fork21));
        r_pathB21.set(ATTR_OUTPUTS, singleton(r_data21));
        r_pathB22.set(ATTR_INPUTS, singleton(r_fork22));
        r_pathB22.set(ATTR_OUTPUTS, singleton(r_data22));

        assertCreateMessage(notifier, 0,
                r_data1, r_data11, r_data12, r_fork11, r_fork12,
                r_data2, r_data21, r_data22, r_fork21, r_fork22,
                r_pathA1, r_pathB11, r_pathB12,
                r_pathA2, r_pathB21, r_pathB22);
    }

    private Map<String, String> createGuidReference(String type, String guid) {
        Map<String, String> map = new HashMap<>();
        map.put(ATTR_TYPENAME, type);
        map.put(ATTR_GUID, guid);
        return map;
    }

    @Test
    public void testUpdateFlowPath() throws AtlasServiceException {
        final NotificationSender sender = new NotificationSender();
        final Referenceable fileC = createRef("fs_path", "/tmp/file-c.txt@test");
        final Referenceable fileD = createRef("fs_path", "/tmp/file-d.txt@test");

        // New in/out fileC and fileD are found for path1.
        final Referenceable newPath1Lineage = createRef(TYPE_NIFI_FLOW_PATH, "path1@test");
        newPath1Lineage.set(ATTR_INPUTS, singleton(fileC));
        newPath1Lineage.set(ATTR_OUTPUTS, singleton(fileD));

        final List<HookNotification.HookNotificationMessage> messages = asList(
                new HookNotification.EntityCreateRequest(NIFI_USER, fileC),
                new HookNotification.EntityCreateRequest(NIFI_USER, fileD),
                new HookNotification.EntityPartialUpdateRequest(NIFI_USER, TYPE_NIFI_FLOW_PATH, ATTR_QUALIFIED_NAME, "path1@test", newPath1Lineage)
        );

        final NiFiAtlasClient atlasClient = mock(NiFiAtlasClient.class);
        sender.setAtlasClient(atlasClient);

        // Existing nifi_flow_path
        final AtlasEntity path1Entity = new AtlasEntity(TYPE_NIFI_FLOW_PATH, ATTR_QUALIFIED_NAME, "path1@test");
        path1Entity.setGuid("path1-guid");
        path1Entity.setAttribute(ATTR_INPUTS, singleton(createGuidReference("fs_path", "fileA-guid")));
        path1Entity.setAttribute(ATTR_OUTPUTS, singleton(createGuidReference("fs_path", "fileB-guid")));

        final AtlasEntity fileAEntity = new AtlasEntity("fs_path", ATTR_QUALIFIED_NAME, "file-a.txt@test");
        fileAEntity.setGuid("fileA-guid");

        final AtlasEntity fileBEntity = new AtlasEntity("fs_path", ATTR_QUALIFIED_NAME, "file-b.txt@test");
        fileBEntity.setGuid("fileA-guid");

        final AtlasEntity.AtlasEntityWithExtInfo path1Ext = new AtlasEntity.AtlasEntityWithExtInfo(path1Entity);
        final AtlasEntity.AtlasEntityWithExtInfo fileAExt = new AtlasEntity.AtlasEntityWithExtInfo(fileAEntity);
        final AtlasEntity.AtlasEntityWithExtInfo fileBExt = new AtlasEntity.AtlasEntityWithExtInfo(fileBEntity);
        when(atlasClient.searchEntityDef(eq(new AtlasObjectId(TYPE_NIFI_FLOW_PATH, ATTR_QUALIFIED_NAME,"path1@test")))).thenReturn(path1Ext);
        when(atlasClient.searchEntityDef(eq(new AtlasObjectId("fileA-guid")))).thenReturn(fileAExt);
        when(atlasClient.searchEntityDef(eq(new AtlasObjectId("fileB-guid")))).thenReturn(fileBExt);

        final Notifier notifier = new Notifier();
        sender.send(messages, notifier);

        assertCreateMessage(notifier, 0, fileC, fileD);
        final Referenceable updatedPath1 = createRef(TYPE_NIFI_FLOW_PATH, "path1@test");
        updatedPath1.set(ATTR_INPUTS, asList(new Referenceable("fileA-guid", "fs_path", Collections.emptyMap()), fileC));
        updatedPath1.set(ATTR_OUTPUTS, asList(new Referenceable("fileB-guid", "fs_path", Collections.emptyMap()), fileD));
        assertUpdateFlowPathMessage(notifier, 1, updatedPath1);
    }

}
