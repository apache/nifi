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
package org.wali;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class DummyRecordSerde implements SerDe<DummyRecord> {

    private int throwIOEAfterNserializeEdits = -1;
    private int throwOOMEAfterNserializeEdits = -1;
    private int serializeEditCount = 0;

    @Override
    public void serializeEdit(final DummyRecord previousState, final DummyRecord record, final DataOutputStream out) throws IOException {
        if (throwIOEAfterNserializeEdits >= 0 && (serializeEditCount++ >= throwIOEAfterNserializeEdits)) {
            throw new IOException("Serialized " + (serializeEditCount - 1) + " records successfully, so now it's time to throw IOE");
        }
        if (throwOOMEAfterNserializeEdits >= 0 && (serializeEditCount++ >= throwOOMEAfterNserializeEdits)) {
            throw new OutOfMemoryError("Serialized " + (serializeEditCount - 1) + " records successfully, so now it's time to throw OOME");
        }

        out.writeUTF(record.getUpdateType().name());
        out.writeUTF(record.getId());

        if (record.getUpdateType() != UpdateType.DELETE) {
            final Map<String, String> props = record.getProperties();
            out.writeInt(props.size());
            for (final Map.Entry<String, String> entry : props.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }
    }

    @Override
    public void serializeRecord(final DummyRecord record, final DataOutputStream out) throws IOException {
        serializeEdit(null, record, out);
    }

    @Override
    public DummyRecord deserializeRecord(final DataInputStream in, final int version) throws IOException {
        final String updateTypeName = in.readUTF();
        final UpdateType updateType = UpdateType.valueOf(updateTypeName);
        final String id = in.readUTF();
        final DummyRecord record = new DummyRecord(id, updateType);

        if (record.getUpdateType() != UpdateType.DELETE) {
            final int numProps = in.readInt();
            for (int i = 0; i < numProps; i++) {
                final String key = in.readUTF();
                final String value = in.readUTF();
                record.setProperty(key, value);
            }
        }
        return record;
    }

    @Override
    public Object getRecordIdentifier(final DummyRecord record) {
        return record.getId();
    }

    @Override
    public UpdateType getUpdateType(final DummyRecord record) {
        return record.getUpdateType();
    }

    @Override
    public DummyRecord deserializeEdit(final DataInputStream in, final Map<Object, DummyRecord> currentVersion, final int version) throws IOException {
        return deserializeRecord(in, version);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    public void setThrowIOEAfterNSerializeEdits(final int n) {
        this.throwIOEAfterNserializeEdits = n;
    }

    public void setThrowOOMEAfterNSerializeEdits(final int n) {
        this.throwOOMEAfterNserializeEdits = n;
    }

    @Override
    public String getLocation(final DummyRecord record) {
        return null;
    }
}
