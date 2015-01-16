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
package org.apache.nifi.io.nio.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.nifi.io.nio.BufferPool;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 *
 * @author none
 */
public abstract class AbstractStreamConsumer implements StreamConsumer {

    private final String uniqueId;
    private BufferPool bufferPool = null;
    private final BlockingQueue<ByteBuffer> filledBuffers = new LinkedBlockingQueue<>();
    private final AtomicBoolean streamEnded = new AtomicBoolean(false);
    private final AtomicBoolean consumerEnded = new AtomicBoolean(false);

    public AbstractStreamConsumer(final String id) {
        uniqueId = id;
    }

    @Override
    public final void setReturnBufferQueue(final BufferPool returnQueue) {
        bufferPool = returnQueue;
    }

    @Override
    public final void addFilledBuffer(final ByteBuffer buffer) {
        if (isConsumerFinished()) {
            buffer.clear();
            bufferPool.returnBuffer(buffer, buffer.remaining());
        } else {
            filledBuffers.add(buffer);
        }
    }

    @Override
    public final void process() throws IOException {
        if (isConsumerFinished()) {
            return;
        }
        if (streamEnded.get() && filledBuffers.isEmpty()) {
            consumerEnded.set(true);
            onConsumerDone();
            return;
        }
        final ByteBuffer buffer = filledBuffers.poll();
        if (buffer != null) {
            final int bytesToProcess = buffer.remaining();
            try {
                processBuffer(buffer);
            } finally {
                buffer.clear();
                bufferPool.returnBuffer(buffer, bytesToProcess);
            }
        }
    }

    protected abstract void processBuffer(ByteBuffer buffer) throws IOException;

    @Override
    public final void signalEndOfStream() {
        streamEnded.set(true);
    }

    /**
     * Convenience method that is called when the consumer is done processing
     * based on being told the signal is end of stream and has processed all
     * given buffers.
     */
    protected void onConsumerDone() {
    }

    @Override
    public final boolean isConsumerFinished() {
        return consumerEnded.get();
    }

    @Override
    public final String getId() {
        return uniqueId;
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        AbstractStreamConsumer rhs = (AbstractStreamConsumer) obj;
        return new EqualsBuilder().appendSuper(super.equals(obj)).append(uniqueId, rhs.uniqueId).isEquals();
    }

    @Override
    public final int hashCode() {
        return new HashCodeBuilder(19, 23).append(uniqueId).toHashCode();
    }

    @Override
    public final String toString() {
        return new ToStringBuilder(this, ToStringStyle.NO_FIELD_NAMES_STYLE).append(uniqueId).toString();
    }
}
