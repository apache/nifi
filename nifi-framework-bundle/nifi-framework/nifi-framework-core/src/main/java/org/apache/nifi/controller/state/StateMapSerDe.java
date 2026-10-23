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
import org.wali.SerDe;
import org.wali.UpdateType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Serializes component state for the write-ahead log. Full records, which are used for snapshots, always hold the whole state map.
 * Starting with version 1, an edit in the journal holds either the whole state map or only the patches. An edit that holds
 * only the patches is applied on top of the component's previous record when the journal is recovered. Version 0 edits always
 * hold the whole state map, in the same layout as full records.
 */
public class StateMapSerDe implements SerDe<StateMapUpdate> {
    private static final long EMPTY_VERSION = -1;

    private static final int PATCHES_VERSION = 1;
    private static final int VERSION = PATCHES_VERSION;

    @Override
    public void serializeEdit(final StateMapUpdate previousRecordState, final StateMapUpdate newRecordState, final DataOutputStream out) throws IOException {
        out.writeUTF(newRecordState.getComponentId());
        out.writeUTF(newRecordState.getUpdateType().name());
        if (newRecordState.getUpdateType() == UpdateType.DELETE) {
            return;
        }

        // Only the patches can be written if the journal is able to recover the previous record to apply them to
        final boolean writePatchesOnly = newRecordState.getPatches() != null && previousRecordState != null && previousRecordState.getStateMap() != null;
        out.writeBoolean(writePatchesOnly);

        final StateMap stateMap = newRecordState.getStateMap();
        out.writeLong(stateMap.getStateVersion().map(Long::parseLong).orElse(EMPTY_VERSION));
        writeEntries(writePatchesOnly ? newRecordState.getPatches() : stateMap.toMap(), out);
    }

    @Override
    public void serializeRecord(final StateMapUpdate record, final DataOutputStream out) throws IOException {
        out.writeUTF(record.getComponentId());
        out.writeUTF(record.getUpdateType().name());
        if (record.getUpdateType() == UpdateType.DELETE) {
            return;
        }

        final StateMap stateMap = record.getStateMap();
        final long recordVersion = stateMap.getStateVersion().map(Long::parseLong).orElse(EMPTY_VERSION);
        out.writeLong(recordVersion);
        writeEntries(stateMap.toMap(), out);
    }

    private void writeEntries(final Map<String, String> map, final DataOutputStream out) throws IOException {
        out.writeInt(map.size());
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            final boolean hasKey = entry.getKey() != null;
            final boolean hasValue = entry.getValue() != null;
            out.writeBoolean(hasKey);
            if (hasKey) {
                out.writeUTF(entry.getKey());
            }

            out.writeBoolean(hasValue);
            if (hasValue) {
                out.writeUTF(entry.getValue());
            }
        }
    }

    @Override
    public StateMapUpdate deserializeEdit(final DataInputStream in, final Map<Object, StateMapUpdate> currentRecordStates, final int version) throws IOException {
        if (version < PATCHES_VERSION) {
            return deserializeRecord(in, version);
        }

        final String componentId = in.readUTF();
        final UpdateType updateType = UpdateType.valueOf(in.readUTF());
        if (updateType == UpdateType.DELETE) {
            return new StateMapUpdate(null, componentId, updateType);
        }

        final boolean patchesOnly = in.readBoolean();
        final String stateVersion = String.valueOf(in.readLong());
        final Map<String, String> entries = readEntries(in);
        if (!patchesOnly) {
            return new StateMapUpdate(new StandardStateMap(entries, Optional.of(stateVersion)), componentId, updateType);
        }

        final StateMapUpdate previousRecord = currentRecordStates.get(componentId);
        if (previousRecord == null || previousRecord.getStateMap() == null) {
            throw new IOException("Journal holds state patches for component " + componentId + " but no previous state was recovered for that component");
        }

        final Map<String, String> patchedState = previousRecord.getStateMap().toMap(entries);
        return new StateMapUpdate(new StandardStateMap(patchedState, Optional.of(stateVersion)), componentId, updateType);
    }

    @Override
    public StateMapUpdate deserializeRecord(final DataInputStream in, final int version) throws IOException {
        final String componentId = in.readUTF();
        final String updateTypeName = in.readUTF();
        final UpdateType updateType = UpdateType.valueOf(updateTypeName);
        if (updateType == UpdateType.DELETE) {
            return new StateMapUpdate(null, componentId, updateType);
        }

        final long recordVersion = in.readLong();
        final Map<String, String> stateValues = readEntries(in);
        final String stateVersion = String.valueOf(recordVersion);
        return new StateMapUpdate(new StandardStateMap(stateValues, Optional.of(stateVersion)), componentId, updateType);
    }

    private Map<String, String> readEntries(final DataInputStream in) throws IOException {
        final int numEntries = in.readInt();
        final Map<String, String> entries = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            final boolean hasKey = in.readBoolean();
            final String key = hasKey ? in.readUTF() : null;
            final boolean hasValue = in.readBoolean();
            final String value = hasValue ? in.readUTF() : null;
            entries.put(key, value);
        }

        return entries;
    }

    @Override
    public Object getRecordIdentifier(final StateMapUpdate record) {
        return record.getComponentId();
    }

    @Override
    public UpdateType getUpdateType(final StateMapUpdate record) {
        return record.getUpdateType();
    }

    @Override
    public String getLocation(final StateMapUpdate record) {
        return null;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }
}
