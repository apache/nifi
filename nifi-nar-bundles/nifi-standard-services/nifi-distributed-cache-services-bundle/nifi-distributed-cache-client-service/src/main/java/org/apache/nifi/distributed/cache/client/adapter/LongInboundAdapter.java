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

import java.io.IOException;

/**
 * Implementation of {@link InboundAdapter} where the service response payload is expected to be a {@link Long}.
 */
public class LongInboundAdapter implements InboundAdapter {

    /**
     * Container for bytes queued from the service response {@link io.netty.channel.Channel}.
     */
    private final ByteBuf byteBuf;

    /**
     * The received service method response value.  This is set to a non-null value upon receipt.
     */
    private Long result;

    /**
     * Constructor.
     */
    public LongInboundAdapter() {
        this.byteBuf = Unpooled.buffer();
        this.result = null;
    }

    /**
     * @return the service method response value
     */
    public Long getResult() {
        return result;
    }

    @Override
    public boolean isComplete() {
        return (result != null);
    }

    @Override
    public void queue(final byte[] bytes) {
        byteBuf.writeBytes(bytes);
    }

    @Override
    public void dequeue() throws IOException {
        if (byteBuf.readableBytes() >= Long.BYTES) {
            result = byteBuf.readLong();
        }
    }
}
