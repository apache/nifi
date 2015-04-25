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
package org.apache.nifi.distributed.cache.server.map;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.wali.MinimalLockingWriteAheadLog;
import org.wali.SerDe;
import org.wali.UpdateType;
import org.wali.WriteAheadRepository;

public class PersistentMapCache implements MapCache {

    private final MapCache wrapped;
    private final WriteAheadRepository<MapWaliRecord> wali;
    
    private final AtomicLong modifications = new AtomicLong(0L);
    
    public PersistentMapCache(final String serviceIdentifier, final File persistencePath, final MapCache cacheToWrap) throws IOException {
        wali = new MinimalLockingWriteAheadLog<>(persistencePath.toPath(), 1, new Serde(), null);
        wrapped = cacheToWrap;
    }

    synchronized void restore() throws IOException {
        final Collection<MapWaliRecord> recovered = wali.recoverRecords();
        for ( final MapWaliRecord record : recovered ) {
            if ( record.getUpdateType() == UpdateType.CREATE ) {
                wrapped.putIfAbsent(record.getKey(), record.getValue());
            }
        }
    }

    @Override
    public MapPutResult putIfAbsent(final ByteBuffer key, final ByteBuffer value) throws IOException {
        final MapPutResult putResult = wrapped.putIfAbsent(key, value);
        if ( putResult.isSuccessful() ) {
            // The put was successful.
            final MapWaliRecord record = new MapWaliRecord(UpdateType.CREATE, key, value);
            final List<MapWaliRecord> records = new ArrayList<>();
            records.add(record);

            if ( putResult.getEvictedKey() != null ) {
                records.add(new MapWaliRecord(UpdateType.DELETE, putResult.getEvictedKey(), putResult.getEvictedValue()));
            }
            
            wali.update(Collections.singletonList(record), false);
            
            final long modCount = modifications.getAndIncrement();
            if ( modCount > 0 && modCount % 100000 == 0 ) {
                wali.checkpoint();
            }
        }
        
        return putResult;
    }
    
    @Override
    public MapPutResult put(final ByteBuffer key, final ByteBuffer value) throws IOException {
    	final MapPutResult putResult = wrapped.put(key, value);
        if ( putResult.isSuccessful() ) {
            // The put was successful.
            final MapWaliRecord record = new MapWaliRecord(UpdateType.CREATE, key, value);
            final List<MapWaliRecord> records = new ArrayList<>();
            records.add(record);

            if ( putResult.getEvictedKey() != null ) {
                records.add(new MapWaliRecord(UpdateType.DELETE, putResult.getEvictedKey(), putResult.getEvictedValue()));
            }
            
            wali.update(Collections.singletonList(record), false);
            
            final long modCount = modifications.getAndIncrement();
            if ( modCount > 0 && modCount % 100000 == 0 ) {
                wali.checkpoint();
            }
        }
        
        return putResult;
    }

    @Override
    public boolean containsKey(final ByteBuffer key) throws IOException {
        return wrapped.containsKey(key);
    }

    @Override
    public ByteBuffer get(final ByteBuffer key) throws IOException {
        return wrapped.get(key);
    }

    @Override
    public ByteBuffer remove(ByteBuffer key) throws IOException {
        final ByteBuffer removeResult = wrapped.remove(key);
        if ( removeResult != null ) {
            final MapWaliRecord record = new MapWaliRecord(UpdateType.DELETE, key, removeResult);
            final List<MapWaliRecord> records = new ArrayList<>(1);
            records.add(record);
            wali.update(records, false);
            
            final long modCount = modifications.getAndIncrement();
            if ( modCount > 0 && modCount % 1000 == 0 ) {
                wali.checkpoint();
            }
        }
        return removeResult;
    }


    @Override
    public void shutdown() throws IOException {
        wali.shutdown();
    }


    private static class MapWaliRecord {
        private final UpdateType updateType;
        private final ByteBuffer key;
        private final ByteBuffer value;
        
        public MapWaliRecord(final UpdateType updateType, final ByteBuffer key, final ByteBuffer value) {
            this.updateType = updateType;
            this.key = key;
            this.value = value;
        }
        
        public UpdateType getUpdateType() {
            return updateType;
        }
        
        public ByteBuffer getKey() {
            return key;
        }
        
        public ByteBuffer getValue() {
            return value;
        }
    }
    
    private static class Serde implements SerDe<MapWaliRecord> {

        @Override
        public void serializeEdit(MapWaliRecord previousRecordState, MapWaliRecord newRecordState, java.io.DataOutputStream out) throws IOException {
            final UpdateType updateType = newRecordState.getUpdateType();
            if ( updateType == UpdateType.DELETE ) {
                out.write(0);
            } else {
                out.write(1);
            }
            
            final byte[] key = newRecordState.getKey().array();
            final byte[] value = newRecordState.getValue().array();
            
            out.writeInt(key.length);
            out.write(key);
            out.writeInt(value.length);
            out.write(value);
        }

        @Override
        public void serializeRecord(MapWaliRecord record, java.io.DataOutputStream out) throws IOException {
            serializeEdit(null, record, out);
        }

        @Override
        public MapWaliRecord deserializeEdit(final DataInputStream in, final Map<Object, MapWaliRecord> currentRecordStates, final int version) throws IOException {
            final int updateTypeValue = in.read();
            if ( updateTypeValue < 0 ) {
                throw new EOFException();
            }

            final UpdateType updateType = (updateTypeValue == 0 ? UpdateType.DELETE : UpdateType.CREATE);
            
            final int keySize = in.readInt();
            final byte[] key = new byte[keySize];
            in.readFully(key);

            final int valueSize = in.readInt();
            final byte[] value = new byte[valueSize];
            in.readFully(value);

            return new MapWaliRecord(updateType, ByteBuffer.wrap(key), ByteBuffer.wrap(value));
        }

        @Override
        public MapWaliRecord deserializeRecord(DataInputStream in, int version) throws IOException {
            return deserializeEdit(in, new HashMap<Object, MapWaliRecord>(), version);
        }

        @Override
        public Object getRecordIdentifier(final MapWaliRecord record) {
            return record.getKey();
        }

        @Override
        public UpdateType getUpdateType(final MapWaliRecord record) {
            return record.getUpdateType();
        }

        @Override
        public String getLocation(final MapWaliRecord record) {
            return null;
        }

        @Override
        public int getVersion() {
            return 1;
        }
    }
}