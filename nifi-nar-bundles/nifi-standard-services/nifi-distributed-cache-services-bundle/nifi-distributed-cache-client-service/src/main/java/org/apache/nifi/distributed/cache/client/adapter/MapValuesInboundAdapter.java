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
import org.apache.nifi.distributed.cache.client.Deserializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link InboundAdapter} where the service response payload is expected to be
 * a collection of values of type V.  These are to be reassembled into a map using the keys
 * provided by the caller.
 *
 * @param <K> the expected type of the service keys
 * @param <V> the expected type of the service response values
 */
public class MapValuesInboundAdapter<K, V> implements InboundAdapter {

    /**
     * An iterator used to traverse the caller-supplied keys, in order to reassemble the service map.
     */
    private final Iterator<K> iteratorKeys;

    /**
     * The deserializer to be used to construct the values in the service response object.
     */
    private final Deserializer<V> deserializerV;

    /**
     * Container for bytes queued from the service response {@link io.netty.channel.Channel}.
     */
    private final ByteBuf byteBuf;

    /**
     * The state of receipt of an individual service response token.
     */
    private final InboundToken<V> inboundToken;

    /**
     * The reassembled map resulting from the service call.
     */
    private final Map<K, V> result;

    /**
     * Constructor.
     *
     * @param keys          the map keys requested by the caller
     * @param deserializerV the deserializer to be used to instantiate the service response map values
     * @param result        container for the map entries reconstituted from the service response
     */
    public MapValuesInboundAdapter(final Set<K> keys, final Deserializer<V> deserializerV, final Map<K, V> result) {
        this.iteratorKeys = keys.iterator();
        this.deserializerV = deserializerV;
        this.byteBuf = Unpooled.buffer();
        this.inboundToken = new InboundToken<>();
        this.result = result;
    }

    /**
     * @return the service method response map
     */
    public Map<K, V> getResult() {
        return result;
    }

    @Override
    public boolean isComplete() {
        return (!iteratorKeys.hasNext());
    }

    @Override
    public void queue(final byte[] bytes) {
        byteBuf.writeBytes(bytes);
    }

    @Override
    public void dequeue() throws IOException {
        while (iteratorKeys.hasNext()) {
            inboundToken.update(byteBuf, deserializerV);
            if (inboundToken.isComplete()) {
                result.put(iteratorKeys.next(), inboundToken.getValue());
                inboundToken.reset();
            } else {
                break;
            }
        }
    }
}
