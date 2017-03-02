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
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.net.ssl.SSLContext;

import org.apache.nifi.distributed.cache.server.AbstractCacheServer;
import org.apache.nifi.distributed.cache.server.EvictionPolicy;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;

public class MapCacheServer extends AbstractCacheServer {

    private final MapCache cache;

    public MapCacheServer(final String identifier, final SSLContext sslContext, final int port, final int maxSize,
            final EvictionPolicy evictionPolicy, final File persistencePath) throws IOException {
        super(identifier, sslContext, port);

        final MapCache simpleCache = new SimpleMapCache(identifier, maxSize, evictionPolicy);

        if (persistencePath == null) {
            this.cache = simpleCache;
        } else {
            final PersistentMapCache persistentCache = new PersistentMapCache(identifier, persistencePath, simpleCache);
            persistentCache.restore();
            this.cache = persistentCache;
        }
    }

    /**
     * Refer {@link org.apache.nifi.distributed.cache.protocol.ProtocolHandshake#initiateHandshake(InputStream, OutputStream, VersionNegotiator)}
     * for details of each version enhancements.
     */
    protected StandardVersionNegotiator getVersionNegotiator() {
        return new StandardVersionNegotiator(2, 1);
    }

    @Override
    protected boolean listen(final InputStream in, final OutputStream out, final int version) throws IOException {
        final DataInputStream dis = new DataInputStream(in);
        final DataOutputStream dos = new DataOutputStream(out);
        final String action = dis.readUTF();
        try {
            switch (action) {
            case "close": {
                return false;
            }
            case "putIfAbsent": {
                final byte[] key = readValue(dis);
                final byte[] value = readValue(dis);
                final MapPutResult putResult = cache.putIfAbsent(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
                dos.writeBoolean(putResult.isSuccessful());
                break;
            }
            case "put": {
                final byte[] key = readValue(dis);
                final byte[] value = readValue(dis);
                cache.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
                dos.writeBoolean(true);
                break;
            }
            case "containsKey": {
                final byte[] key = readValue(dis);
                final boolean contains = cache.containsKey(ByteBuffer.wrap(key));
                dos.writeBoolean(contains);
                break;
            }
            case "getAndPutIfAbsent": {
                final byte[] key = readValue(dis);
                final byte[] value = readValue(dis);

                final MapPutResult putResult = cache.putIfAbsent(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
                if (putResult.isSuccessful()) {
                    // Put was successful. There was no old value to get.
                    dos.writeInt(0);
                } else {
                    // we didn't put. Write back the previous value
                    final byte[] byteArray = putResult.getExisting().getValue().array();
                    dos.writeInt(byteArray.length);
                    dos.write(byteArray);
                }

                break;
            }
            case "get": {
                final byte[] key = readValue(dis);
                final ByteBuffer existingValue = cache.get(ByteBuffer.wrap(key));
                if (existingValue == null) {
                    // there was no existing value.
                    dos.writeInt(0);
                } else {
                    // a value already existed.
                    final byte[] byteArray = existingValue.array();
                    dos.writeInt(byteArray.length);
                    dos.write(byteArray);
                }

                break;
            }
            case "remove": {
                final byte[] key = readValue(dis);
                final boolean removed = cache.remove(ByteBuffer.wrap(key)) != null;
                dos.writeBoolean(removed);
                break;
            }
            case "fetch": {
                final byte[] key = readValue(dis);
                final MapCacheRecord existing = cache.fetch(ByteBuffer.wrap(key));
                if (existing == null) {
                    // there was no existing value.
                    dos.writeLong(-1);
                    dos.writeInt(0);
                } else {
                    // a value already existed.
                    dos.writeLong(existing.getRevision());
                    final byte[] byteArray = existing.getValue().array();
                    dos.writeInt(byteArray.length);
                    dos.write(byteArray);
                }

                break;
            }
            case "replace": {
                final byte[] key = readValue(dis);
                final long revision = dis.readLong();
                final byte[] value = readValue(dis);
                final MapPutResult result = cache.replace(new MapCacheRecord(ByteBuffer.wrap(key), ByteBuffer.wrap(value), revision));
                dos.writeBoolean(result.isSuccessful());
                break;
            }
            default: {
                throw new IOException("Illegal Request");
            }
            }
        } finally {
            dos.flush();
        }

        return true;
    }

    @Override
    public void stop() throws IOException {
        try {
            super.stop();
        } finally {
            cache.shutdown();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (!stopped) {
            stop();
        }
    }

    private byte[] readValue(final DataInputStream dis) throws IOException {
        final int numBytes = dis.readInt();
        final byte[] buffer = new byte[numBytes];
        dis.readFully(buffer);
        return buffer;
    }

}
