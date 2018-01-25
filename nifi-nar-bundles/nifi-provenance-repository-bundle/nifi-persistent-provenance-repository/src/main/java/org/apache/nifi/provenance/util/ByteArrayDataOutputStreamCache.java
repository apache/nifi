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

package org.apache.nifi.provenance.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ByteArrayDataOutputStreamCache {
    private final BlockingQueue<ByteArrayDataOutputStream> queue;
    private final int initialBufferSize;
    private final int maxBufferSize;

    public ByteArrayDataOutputStreamCache(final int maxCapacity, final int initialBufferSize, final int maxBufferSize) {
        this.queue = new LinkedBlockingQueue<>(maxCapacity);
        this.initialBufferSize = initialBufferSize;
        this.maxBufferSize = maxBufferSize;
    }

    public ByteArrayDataOutputStream checkOut() {
        final ByteArrayDataOutputStream stream = queue.poll();
        if (stream != null) {
            return stream;
        }

        return new ByteArrayDataOutputStream(initialBufferSize);
    }

    public void checkIn(final ByteArrayDataOutputStream bados) {
        final int size = bados.getByteArrayOutputStream().size();
        if (size > maxBufferSize) {
            return;
        }

        bados.getByteArrayOutputStream().reset();
        queue.offer(bados);
    }
}
