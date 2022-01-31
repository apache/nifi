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
package org.apache.nifi.processor.util.listen.dispatcher;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ByteBufferPool implements ByteBufferSource {

    private final BlockingQueue<ByteBuffer> pool;

    public ByteBufferPool(final int poolSize, final int bufferSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("A pool of available ByteBuffers is required");
        }

        this.pool = new LinkedBlockingQueue<>(poolSize);

        for (int i = 0; i < poolSize; i++) {
            pool.offer(ByteBuffer.allocate(bufferSize));
        }
    }

    @Override
    public ByteBuffer acquire() {
        final ByteBuffer buffer = pool.poll();
        buffer.clear();
        buffer.mark();
        return buffer;
    }

    @Override
    public void release(final ByteBuffer byteBuffer) {
        try {
            pool.put(byteBuffer);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
