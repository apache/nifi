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
import java.util.Map;

/**
 * Implementation of {@link InboundAdapter} where the service response payload is expected to be
 * a map of keys of type K to values of type V.
 *
 * @param <K> the expected type of the service response keys
 * @param <V> the expected type of the service response values
 */
public class MapInboundAdapter<K, V> implements InboundAdapter {

    /**
     * The deserializer to be used to construct the keys in the service response map.
     */
    private final Deserializer<K> deserializerK;

    /**
     * The deserializer to be used to construct the values from the service response object.
     */
    private final Deserializer<V> deserializerV;

    /**
     * Container for bytes queued from the service response {@link io.netty.channel.Channel}.
     */
    private final ByteBuf byteBuf;

    /**
     * The state of receipt of an individual service response token.
     */
    private final InboundToken<K> inboundTokenK;

    /**
     * The state of receipt of an individual service response token.
     */
    private final InboundToken<V> inboundTokenV;

    /**
     * The count of map entries expected in the service response.
     */
    private Integer size;

    /**
     * The entries received in the service response.
     */
    private final Map<K, V> result;

    /**
     * Constructor.
     *
     * @param deserializerK the deserializer to be used to construct the service response map keys
     * @param deserializerV the deserializer to be used to construct the service response map values
     * @param result        container for the map entries received in the service response
     */
    public MapInboundAdapter(final Deserializer<K> deserializerK,
                             final Deserializer<V> deserializerV, final Map<K, V> result) {
        this.deserializerK = deserializerK;
        this.deserializerV = deserializerV;
        this.byteBuf = Unpooled.buffer();
        this.inboundTokenK = new InboundToken<>();
        this.inboundTokenV = new InboundToken<>();
        this.size = null;
        this.result = result;
    }

    /**
     * @return the service method response value
     */
    public Map<K, V> getResult() {
        return result;
    }

    @Override
    public boolean isComplete() {
        return ((size != null) && (this.result.size() >= size));
    }

    @Override
    public void queue(final byte[] bytes) {
        byteBuf.writeBytes(bytes);
    }

    @Override
    public void dequeue() throws IOException {
        if ((size == null) && (byteBuf.readableBytes() >= Integer.BYTES)) {
            size = byteBuf.readInt();
        }
        while ((size != null) && (result.size() < size)) {
            if (!inboundTokenK.isComplete()) {
                inboundTokenK.update(byteBuf, deserializerK);
            }
            if (inboundTokenK.isComplete() && !inboundTokenV.isComplete()) {
                inboundTokenV.update(byteBuf, deserializerV);
            }
            if (inboundTokenK.isComplete() && inboundTokenV.isComplete()) {
                result.put(inboundTokenK.getValue(), inboundTokenV.getValue());
                inboundTokenK.reset();
                inboundTokenV.reset();
            } else {
                break;
            }
        }
    }
}
