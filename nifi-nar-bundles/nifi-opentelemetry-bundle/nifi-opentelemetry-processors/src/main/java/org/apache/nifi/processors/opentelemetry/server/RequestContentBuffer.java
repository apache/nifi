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
package org.apache.nifi.processors.opentelemetry.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Buffer wrapper for HTTP Request Content received from streams or frames
 */
class RequestContentBuffer {
    private AtomicReference<ByteBuf> contentBufferRef = new AtomicReference<>();

    /**
     * Clear Buffer
     *
     */
    void clearBuffer() {
        contentBufferRef.getAndSet(null);
    }

    /**
     * Get Buffer contents
     *
     * @return Buffer contents
     */
    ByteBuf getBuffer() {
        return contentBufferRef.get();
    }

    /**
     * Update Content Buffer with incremental Frame Buffer and allocate new Content Buffer when not defined
     *
     * @param channelHandlerContext Channel Handler Context
     * @param frameBuffer Frame Buffer with incremental content updates
     */
    void updateBuffer(final ChannelHandlerContext channelHandlerContext, final ByteBuf frameBuffer) {
        final int readableBytes = frameBuffer.readableBytes();

        final ByteBuf requestContentBuffer;
        final ByteBuf requestContent = contentBufferRef.get();
        if (requestContent == null) {
            requestContentBuffer = channelHandlerContext.alloc().buffer(readableBytes);
            requestContentBuffer.writeBytes(frameBuffer);
        } else {
            requestContentBuffer = requestContent;
            requestContentBuffer.ensureWritable(readableBytes);
            requestContentBuffer.writeBytes(frameBuffer);
        }

        contentBufferRef.getAndSet(requestContentBuffer);
    }
}
