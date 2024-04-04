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
package org.apache.nifi.distributed.cache.client.adapter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.Deserializer;

import java.io.IOException;

/**
 * Implementation of {@link InboundAdapter} where the service response payload is expected to be
 * an {@link AtomicCacheEntry}, allowing for synchronized access to a given service cache entry.
 *
 * @param <K> the expected type of the service response key
 * @param <V> the expected type of the service response value
 */

public class AtomicCacheEntryInboundAdapter<K, V> implements InboundAdapter {

    /**
     * The key of the requested service map entry.
     */
    private final K key;

    /**
     * The deserializer to be used to construct the value from the service response object.
     */
    private final Deserializer<V> deserializer;

    /**
     * Container for bytes queued from the service response {@link io.netty.channel.Channel}.
     */
    private final ByteBuf byteBuf;

    /**
     * The state of receipt of an individual service response token.
     */
    private final InboundToken<V> inboundToken;

    /**
     * The cache revision of the requested object.
     */
    private Long revision;

    /**
     * The cache entry reassembled from the service response.
     */
    private AtomicCacheEntry<K, V, Long> result;

    /**
     * Constructor.
     *
     * @param key          the key of the requested service cache map entry
     * @param deserializer the deserializer to be used to construct the service response map value
     */
    public AtomicCacheEntryInboundAdapter(final K key, final Deserializer<V> deserializer) {
        this.key = key;
        this.deserializer = deserializer;
        this.byteBuf = Unpooled.buffer();
        this.inboundToken = new InboundToken<>();
        this.revision = null;
        this.result = null;
    }

    /**
     * @return the service method response value
     */
    public AtomicCacheEntry<K, V, Long> getResult() {
        return result;
    }

    @Override
    public boolean isComplete() {
        return inboundToken.isComplete();
    }

    @Override
    public void queue(final byte[] bytes) {
        byteBuf.writeBytes(bytes);
    }

    @Override
    public void dequeue() throws IOException {
        if ((revision == null) && (byteBuf.readableBytes() >= Long.BYTES)) {
            revision = byteBuf.readLong();
        }
        if (revision != null) {
            inboundToken.update(byteBuf, deserializer);
            if (inboundToken.isComplete()) {
                result = (revision < 0) ? null : new AtomicCacheEntry<>(key, inboundToken.getValue(), revision);
            }
        }
    }
}
