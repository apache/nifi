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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.components.state.StateMap;
import org.wali.SerDe;
import org.wali.UpdateType;

public class StateMapSerDe implements SerDe<StateMapUpdate> {
    private static final int VERSION = 0;

    @Override
    public void serializeEdit(final StateMapUpdate previousRecordState, final StateMapUpdate newRecordState, final DataOutputStream out) throws IOException {
        serializeRecord(newRecordState, out);
    }

    @Override
    public void serializeRecord(final StateMapUpdate record, final DataOutputStream out) throws IOException {
        out.writeUTF(record.getComponentId());
        out.writeUTF(record.getUpdateType().name());
        if (record.getUpdateType() == UpdateType.DELETE) {
            return;
        }

        final StateMap stateMap = record.getStateMap();
        final long recordVersion = stateMap.getVersion();
        out.writeLong(recordVersion);

        final Map<String, String> map = stateMap.toMap();
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
        return deserializeRecord(in, version);
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
        final int numEntries = in.readInt();
        final Map<String, String> stateValues = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            final boolean hasKey = in.readBoolean();
            final String key = hasKey ? in.readUTF() : null;
            final boolean hasValue = in.readBoolean();
            final String value = hasValue ? in.readUTF() : null;
            stateValues.put(key, value);
        }

        return new StateMapUpdate(new StandardStateMap(stateValues, recordVersion), componentId, updateType);
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
