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
package org.apache.nifi.controller.repository.io;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MemoryManager {

    private final int blockSize;

    private final BlockingQueue<byte[]> queue;

    public MemoryManager(final long totalSize, final int blockSize) {
        this.blockSize = blockSize;

        final int numBlocks = (int) (totalSize / blockSize);
        queue = new LinkedBlockingQueue<>(numBlocks);

        for (int i = 0; i < numBlocks; i++) {
            queue.offer(new byte[blockSize]);
        }
    }

    byte[] checkOut() {
        return queue.poll();
    }

    void checkIn(final byte[] buffer) {
        queue.offer(buffer);
    }

    void checkIn(final Collection<byte[]> buffers) {
        queue.addAll(buffers);
    }

    int getBlockSize() {
        return blockSize;
    }
}
