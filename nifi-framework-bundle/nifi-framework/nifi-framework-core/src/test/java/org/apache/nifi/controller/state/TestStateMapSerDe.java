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

package org.apache.nifi.controller.state;

import org.apache.nifi.components.state.StateMap;
import org.junit.jupiter.api.Test;
import org.wali.UpdateType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStateMapSerDe {

    @Test
    public void testCreateRoundTrip() throws IOException {
        final String componentId = "1234";

        final StateMapSerDe serde = new StateMapSerDe();
        final Map<String, String> stateValues = new HashMap<>();
        stateValues.put("abc", "xyz");
        stateValues.put("cba", "zyx");

        String version = "3";
        final StateMap stateMap = new StandardStateMap(stateValues, Optional.of(version));
        final StateMapUpdate record = new StateMapUpdate(stateMap, componentId, UpdateType.CREATE);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final DataOutputStream out = new DataOutputStream(baos)) {
            serde.serializeRecord(record, out);
        }

        final StateMapUpdate update;
        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (final DataInputStream in = new DataInputStream(bais)) {
            update = serde.deserializeRecord(in, serde.getVersion());
        }

        assertNotNull(update);
        assertEquals(componentId, update.getComponentId());
        assertEquals(UpdateType.CREATE, update.getUpdateType());
        final StateMap recoveredStateMap = update.getStateMap();

        final Optional<String> stateVersion = recoveredStateMap.getStateVersion();
        assertTrue(stateVersion.isPresent());
        assertEquals(version, stateVersion.get());
        assertEquals(stateValues, recoveredStateMap.toMap());

        // Journals written with version 0 hold edits in the same layout as full records
        final StateMapUpdate versionZeroEdit = deserializeEdit(serde, baos.toByteArray(), Map.of(), 0);
        assertEquals(componentId, versionZeroEdit.getComponentId());
        assertEquals(stateValues, versionZeroEdit.getStateMap().toMap());
        assertEquals(Optional.of(version), versionZeroEdit.getStateMap().getStateVersion());
    }

    @Test
    public void testPatchEditRoundTrip() throws IOException {
        final String componentId = "1234";
        final StateMapSerDe serde = new StateMapSerDe();

        final Map<String, String> largeState = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            largeState.put("key" + i, "value" + i);
        }
        final StateMapUpdate previousRecord = new StateMapUpdate(new StandardStateMap(largeState, Optional.of("4")), componentId, UpdateType.UPDATE);

        final Map<String, String> patches = new HashMap<>();
        patches.put("key0", "changed");
        patches.put("key1", null);
        final Map<String, String> patchedState = new HashMap<>(largeState);
        patchedState.put("key0", "changed");
        patchedState.remove("key1");
        final StandardStateMap patchedStateMap = new StandardStateMap(patchedState, Optional.of("5"));

        final byte[] patchEdit = serializeEdit(serde, previousRecord, new StateMapUpdate(patchedStateMap, componentId, UpdateType.UPDATE, patches));
        final byte[] wholeStateEdit = serializeEdit(serde, previousRecord, new StateMapUpdate(patchedStateMap, componentId, UpdateType.UPDATE));
        assertTrue(patchEdit.length * 100 < wholeStateEdit.length);

        // Both kinds of edit recover to the same state when the previous record is available
        final Map<Object, StateMapUpdate> recoveredRecords = Map.of(componentId, previousRecord);
        for (final byte[] edit : new byte[][] {patchEdit, wholeStateEdit}) {
            final StateMapUpdate recoveredEdit = deserializeEdit(serde, edit, recoveredRecords, serde.getVersion());
            assertEquals(patchedState, recoveredEdit.getStateMap().toMap());
            assertEquals(Optional.of("5"), recoveredEdit.getStateMap().getStateVersion());
        }

        final IOException noPreviousRecord = assertThrows(IOException.class, () -> deserializeEdit(serde, patchEdit, Map.of(), serde.getVersion()));
        assertTrue(noPreviousRecord.getMessage().contains(componentId));

        // Without a previous record to apply patches to, the whole state is written
        final byte[] firstEdit = serializeEdit(serde, null, new StateMapUpdate(patchedStateMap, componentId, UpdateType.UPDATE, patches));
        assertEquals(patchedState, deserializeEdit(serde, firstEdit, Map.of(), serde.getVersion()).getStateMap().toMap());
    }

    private byte[] serializeEdit(final StateMapSerDe serde, final StateMapUpdate previousRecord, final StateMapUpdate record) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final DataOutputStream out = new DataOutputStream(baos)) {
            serde.serializeEdit(previousRecord, record, out);
        }
        return baos.toByteArray();
    }

    private StateMapUpdate deserializeEdit(final StateMapSerDe serde, final byte[] edit, final Map<Object, StateMapUpdate> recoveredRecords, final int version) throws IOException {
        try (final DataInputStream in = new DataInputStream(new ByteArrayInputStream(edit))) {
            return serde.deserializeEdit(in, recoveredRecords, version);
        }
    }
}
