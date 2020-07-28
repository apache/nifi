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
package org.apache.nifi.distributed.cache.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.net.ssl.SSLContext;

import org.apache.nifi.distributed.cache.server.set.PersistentSetCache;
import org.apache.nifi.distributed.cache.server.set.SetCache;
import org.apache.nifi.distributed.cache.server.set.SetCacheResult;
import org.apache.nifi.distributed.cache.server.set.SimpleSetCache;

public class SetCacheServer extends AbstractCacheServer {

    private final SetCache cache;

    public SetCacheServer(final String identifier, final SSLContext sslContext, final int port, final int maxSize,
            final EvictionPolicy evictionPolicy, final File persistencePath) throws IOException {
        super(identifier, sslContext, port);

        final SetCache simpleCache = new SimpleSetCache(identifier, maxSize, evictionPolicy);

        if (persistencePath == null) {
            this.cache = simpleCache;
        } else {
            final PersistentSetCache persistentCache = new PersistentSetCache(identifier, persistencePath, simpleCache);
            persistentCache.restore();
            this.cache = persistentCache;
        }
    }

    @Override
    protected boolean listen(final InputStream in, final OutputStream out, final int version) throws IOException {
        final DataInputStream dis = new DataInputStream(in);
        final DataOutputStream dos = new DataOutputStream(out);

        final String action = dis.readUTF();
        if (action.equals("close")) {
            return false;
        }

        final int valueLength = dis.readInt();
        final byte[] value = new byte[valueLength];
        dis.readFully(value);
        final ByteBuffer valueBuffer = ByteBuffer.wrap(value);

        final SetCacheResult response;
        switch (action) {
            case "addIfAbsent":
                response = cache.addIfAbsent(valueBuffer);
                break;
            case "contains":
                response = cache.contains(valueBuffer);
                break;
            case "remove":
                response = cache.remove(valueBuffer);
                break;
            default:
                throw new IOException("IllegalRequest");
        }

        dos.writeBoolean(response.getResult());
        dos.flush();

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

}
