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
package org.apache.nifi.distributed.cache.server.set;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.wali.MinimalLockingWriteAheadLog;
import org.wali.SerDe;
import org.wali.UpdateType;
import org.wali.WriteAheadRepository;

public class PersistentSetCache implements SetCache {

    private final SetCache wrapped;
    private final WriteAheadRepository<SetRecord> wali;

    private final AtomicLong modifications = new AtomicLong(0L);

    public PersistentSetCache(final String serviceIdentifier, final File persistencePath, final SetCache cacheToWrap) throws IOException {
        wali = new MinimalLockingWriteAheadLog<>(persistencePath.toPath(), 1, new Serde(), null);
        wrapped = cacheToWrap;
    }

    public synchronized void restore() throws IOException {
        final Collection<SetRecord> recovered = wali.recoverRecords();
        for (final SetRecord record : recovered) {
            if (record.getUpdateType() == UpdateType.CREATE) {
                addIfAbsent(record.getBuffer());
            }
        }
    }

    @Override
    public synchronized SetCacheResult remove(final ByteBuffer value) throws IOException {
        final SetCacheResult removeResult = wrapped.remove(value);
        if (removeResult.getResult()) {
            final SetRecord record = new SetRecord(UpdateType.DELETE, value);
            final List<SetRecord> records = new ArrayList<>();
            records.add(record);
            wali.update(records, false);

            final long modCount = modifications.getAndIncrement();
            if (modCount > 0 && modCount % 1000 == 0) {
                wali.checkpoint();
            }
        }

        return removeResult;
    }

    @Override
    public synchronized SetCacheResult addIfAbsent(final ByteBuffer value) throws IOException {
        final SetCacheResult addResult = wrapped.addIfAbsent(value);
        if (addResult.getResult()) {
            final SetRecord record = new SetRecord(UpdateType.CREATE, value);
            final List<SetRecord> records = new ArrayList<>();
            records.add(record);

            final SetCacheRecord evictedRecord = addResult.getEvictedRecord();
            if (evictedRecord != null) {
                records.add(new SetRecord(UpdateType.DELETE, evictedRecord.getValue()));
            }

            wali.update(records, false);

            final long modCount = modifications.getAndIncrement();
            if (modCount > 0 && modCount % 1000 == 0) {
                wali.checkpoint();
            }
        }

        return addResult;
    }

    @Override
    public synchronized SetCacheResult contains(final ByteBuffer value) throws IOException {
        return wrapped.contains(value);
    }

    @Override
    public void shutdown() throws IOException {
        wali.shutdown();
    }

    private static class SetRecord {

        private final UpdateType updateType;
        private final ByteBuffer value;

        public SetRecord(final UpdateType updateType, final ByteBuffer value) {
            this.updateType = updateType;
            this.value = value;
        }

        public UpdateType getUpdateType() {
            return updateType;
        }

        public ByteBuffer getBuffer() {
            return value;
        }

        public byte[] getData() {
            return value.array();
        }
    }

    private static class Serde implements SerDe<SetRecord> {

        @Override
        public void serializeEdit(final SetRecord previousRecordState, final SetRecord newRecordState, final DataOutputStream out) throws IOException {
            final UpdateType updateType = newRecordState.getUpdateType();
            if (updateType == UpdateType.DELETE) {
                out.write(0);
            } else {
                out.write(1);
            }

            final byte[] data = newRecordState.getData();
            out.writeInt(data.length);
            out.write(newRecordState.getData());
        }

        @Override
        public void serializeRecord(SetRecord record, DataOutputStream out) throws IOException {
            serializeEdit(null, record, out);
        }

        @Override
        public SetRecord deserializeEdit(final DataInputStream in, final Map<Object, SetRecord> currentRecordStates, final int version) throws IOException {
            final int value = in.read();
            if (value < 0) {
                throw new EOFException();
            }

            final UpdateType updateType = (value == 0 ? UpdateType.DELETE : UpdateType.CREATE);

            final int size = in.readInt();
            final byte[] data = new byte[size];
            in.readFully(data);

            return new SetRecord(updateType, ByteBuffer.wrap(data));
        }

        @Override
        public SetRecord deserializeRecord(DataInputStream in, int version) throws IOException {
            return deserializeEdit(in, new HashMap<Object, SetRecord>(), version);
        }

        @Override
        public Object getRecordIdentifier(final SetRecord record) {
            return record.getBuffer();
        }

        @Override
        public UpdateType getUpdateType(final SetRecord record) {
            return record.getUpdateType();
        }

        @Override
        public String getLocation(final SetRecord record) {
            return null;
        }

        @Override
        public int getVersion() {
            return 1;
        }
    }
}
