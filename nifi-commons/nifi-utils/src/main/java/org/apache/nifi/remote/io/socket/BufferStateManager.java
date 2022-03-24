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
package org.apache.nifi.remote.io.socket;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferStateManager {

    private static final Logger logger = LoggerFactory.getLogger(BufferStateManager.class);

    private ByteBuffer buffer;
    private Direction direction = Direction.WRITE;

    public BufferStateManager(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public BufferStateManager(final ByteBuffer buffer, final Direction direction) {
        this.buffer = buffer;
        this.direction = direction;
    }

    /**
     * Ensures that the buffer is at least as big as the size specified, resizing the buffer if necessary. This operation MAY change the direction of the buffer.
     *
     * @param requiredSize the desired size of the buffer
     */
    public void ensureSize(final int requiredSize) {
        if (buffer.capacity() < requiredSize) {
            final ByteBuffer newBuffer = ByteBuffer.allocate(requiredSize);

            // we have to read buffer so make sure the direction is correct.
            if (direction == Direction.WRITE) {
                buffer.flip();
            }

            // Copy from buffer to newBuffer
            newBuffer.put(buffer);

            // Swap the buffers
            buffer = newBuffer;

            // the new buffer is ready to be written to
            direction = Direction.WRITE;
        }
    }

    public ByteBuffer prepareForWrite(final int requiredSize) {
        ensureSize(requiredSize);

        if (direction == Direction.READ) {
            direction = Direction.WRITE;
            buffer.position(buffer.limit());
        }

        buffer.limit(buffer.capacity());
        return buffer;
    }

    public ByteBuffer prepareForRead(final int requiredSize) {
        ensureSize(requiredSize);

        if (direction == Direction.WRITE) {
            direction = Direction.READ;
            buffer.flip();
        }

        return buffer;
    }

    /**
     * Clears the contents of the buffer and sets direction to WRITE
     */
    public void clear() {
        logger.debug("Clearing {}", buffer);
        buffer.clear();
        direction = Direction.WRITE;
    }

    public void compact() {
        final String before = buffer.toString();
        buffer.compact();
        logger.debug("Before compact: {}, after: {}", before, buffer);
        direction = Direction.WRITE;
    }

    public static enum Direction {

        READ, WRITE;
    }

}
