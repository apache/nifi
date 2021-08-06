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

/**
 * Implementation of {@link InboundAdapter} where the service response payload is expected to be
 * an object of type T.
 *
 * @param <T> the expected type of the service response
 */
public class ValueInboundAdapter<T> implements InboundAdapter {

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
     * Constructor.
     *
     * @param deserializer the deserializer to be used to construct the service response object
     */
    public ValueInboundAdapter(final Deserializer<T> deserializer) {
        this.deserializer = deserializer;
        this.byteBuf = Unpooled.buffer();
        this.inboundToken = new InboundToken<>();
    }

    /**
     * @return the service method response value
     */
    public T getResult() {
        return inboundToken.getValue();
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
        inboundToken.update(byteBuf, deserializer);
    }
}
