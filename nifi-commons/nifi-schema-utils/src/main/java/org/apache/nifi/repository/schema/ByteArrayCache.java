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

package org.apache.nifi.repository.schema;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ByteArrayCache {
    private final BlockingQueue<byte[]> queue;
    private final int bufferSize;

    public ByteArrayCache(final int maxCapacity, final int bufferSize) {
        this.queue = new LinkedBlockingQueue<>(maxCapacity);
        this.bufferSize = bufferSize;
    }

    public byte[] checkOut() {
        final byte[] array = queue.poll();
        if (array != null) {
            return array;
        }

        return new byte[bufferSize];
    }

    public void checkIn(final byte[] array) {
        if (array.length != bufferSize) {
            return;
        }

        queue.offer(array);
    }
}
