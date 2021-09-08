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
import java.util.Set;

/**
 * Implementation of {@link InboundAdapter} where the service response payload is expected to be
 * a set of objects of type T.
 *
 * @param <T> the expected type of the service response
 */
public class SetInboundAdapter<T> implements InboundAdapter {

    /**
     * The deserializer to be used to construct the service response object.
     */
    private final Deserializer<T> deserializer;

    /**
     * Container for bytes queued from the service response {@link io.netty.channel.Channel}.
     */
    private final ByteBuf byteBuf;

    /**
     * The state of receipt of an individual service response token.
     */
    private final InboundToken<T> inboundToken;

    /**
     * The count of objects expected in the service response.
     */
    private Integer size;

    /**
     * The objects received in the service response.
     */
    private final Set<T> result;

    /**
     * Constructor.
     *
     * @param deserializer the deserializer to be used to construct the service response object
     * @param result       container for the objects received in the service response
     */
    public SetInboundAdapter(final Deserializer<T> deserializer, final Set<T> result) {
        this.deserializer = deserializer;
        this.byteBuf = Unpooled.buffer();
        this.inboundToken = new InboundToken<>();
        this.size = null;
        this.result = result;
    }

    /**
     * @return the service method response value
     */
    public Set<T> getResult() {
        return result;
    }

    @Override
    public boolean isComplete() {
        return ((size != null) && (result.size() >= size));
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
            inboundToken.update(byteBuf, deserializer);
            if (inboundToken.isComplete()) {
                result.add(inboundToken.getValue());
                inboundToken.reset();
            } else {
                break;
            }
        }
    }
}
