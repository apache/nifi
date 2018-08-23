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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class DummyRecordSerde implements SerDe<DummyRecord> {
    private static final int INLINE_RECORD_INDICATOR = 1;
    private static final int EXTERNAL_FILE_INDICATOR = 8;

    private int throwIOEAfterNserializeEdits = -1;
    private int throwOOMEAfterNserializeEdits = -1;
    private int serializeEditCount = 0;

    private final Set<File> externalFilesWritten = new HashSet<>();
    private Queue<DummyRecord> externalRecords;

    @SuppressWarnings("fallthrough")
    @Override
    public void serializeEdit(final DummyRecord previousState, final DummyRecord record, final DataOutputStream out) throws IOException {
        if (throwIOEAfterNserializeEdits >= 0 && (serializeEditCount++ >= throwIOEAfterNserializeEdits)) {
            throw new IOException("Serialized " + (serializeEditCount - 1) + " records successfully, so now it's time to throw IOE");
        }
        if (throwOOMEAfterNserializeEdits >= 0 && (serializeEditCount++ >= throwOOMEAfterNserializeEdits)) {
            throw new OutOfMemoryError("Serialized " + (serializeEditCount - 1) + " records successfully, so now it's time to throw OOME");
        }

        out.write(INLINE_RECORD_INDICATOR);
        out.writeUTF(record.getUpdateType().name());
        out.writeUTF(record.getId());

        switch (record.getUpdateType()) {
            case DELETE:
                break;
            case SWAP_IN: {
                out.writeUTF(record.getSwapLocation());
                // intentionally fall through to CREATE/UPDATE block
            }
            case CREATE:
            case UPDATE: {
                    final Map<String, String> props = record.getProperties();
                    out.writeInt(props.size());
                    for (final Map.Entry<String, String> entry : props.entrySet()) {
                        out.writeUTF(entry.getKey());
                        out.writeUTF(entry.getValue());
                    }
                }
                break;
            case SWAP_OUT:
                out.writeUTF(record.getSwapLocation());
                break;
        }

    }

    @Override
    public void serializeRecord(final DummyRecord record, final DataOutputStream out) throws IOException {
        serializeEdit(null, record, out);
    }

    @Override
    @SuppressWarnings("fallthrough")
    public DummyRecord deserializeRecord(final DataInputStream in, final int version) throws IOException {
        if (externalRecords != null) {
            final DummyRecord record = externalRecords.poll();
            if (record != null) {
                return record;
            }

            externalRecords = null;
        }

        final int recordLocationIndicator = in.read();
        if (recordLocationIndicator == EXTERNAL_FILE_INDICATOR) {
            final String externalFilename = in.readUTF();
            final File externalFile = new File(externalFilename);

            try (final InputStream fis = new FileInputStream(externalFile);
                 final InputStream bufferedIn = new BufferedInputStream(fis);
                 final DataInputStream dis = new DataInputStream(bufferedIn)) {

                externalRecords = new LinkedBlockingQueue<>();

                DummyRecord record;
                while ((record = deserializeRecordInline(dis, version, true)) != null) {
                    externalRecords.offer(record);
                }

                return externalRecords.poll();
            }
        } else if (recordLocationIndicator == INLINE_RECORD_INDICATOR) {
            return deserializeRecordInline(in, version, false);
        } else {
            throw new IOException("Encountered invalid record location indicator: " + recordLocationIndicator);
        }
    }

    @Override
    public boolean isMoreInExternalFile() {
        return externalRecords != null && !externalRecords.isEmpty();
    }

    private DummyRecord deserializeRecordInline(final DataInputStream in, final int version, final boolean expectInlineRecordIndicator) throws IOException {
        if (expectInlineRecordIndicator) {
            final int locationIndicator = in.read();
            if (locationIndicator < 0) {
                return null;
            }

            if (locationIndicator != INLINE_RECORD_INDICATOR) {
                throw new IOException("Expected inline record indicator but encountered " + locationIndicator);
            }
        }

        final String updateTypeName = in.readUTF();
        final UpdateType updateType = UpdateType.valueOf(updateTypeName);
        final String id = in.readUTF();
        final DummyRecord record = new DummyRecord(id, updateType);

        switch (record.getUpdateType()) {
            case DELETE:
                break;
            case SWAP_IN: {
                final String swapLocation = in.readUTF();
                record.setSwapLocation(swapLocation);
                // intentionally fall through to the CREATE/UPDATE block
            }
            case CREATE:
            case UPDATE:
                final int numProps = in.readInt();
                for (int i = 0; i < numProps; i++) {
                    final String key = in.readUTF();
                    final String value = in.readUTF();
                    record.setProperty(key, value);
                }
                break;
            case SWAP_OUT:
                final String swapLocation = in.readUTF();
                record.setSwapLocation(swapLocation);
                break;
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
        return record.getSwapLocation();
    }

    @Override
    public boolean isWriteExternalFileReferenceSupported() {
        return true;
    }

    @Override
    public void writeExternalFileReference(final File externalFile, final DataOutputStream out) throws IOException {
        out.write(EXTERNAL_FILE_INDICATOR);
        out.writeUTF(externalFile.getAbsolutePath());

        externalFilesWritten.add(externalFile);
    }

    public Set<File> getExternalFileReferences() {
        return Collections.unmodifiableSet(externalFilesWritten);
    }
}
