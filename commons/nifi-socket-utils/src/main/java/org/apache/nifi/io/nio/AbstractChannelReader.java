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
package org.apache.nifi.io.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.io.nio.consumer.StreamConsumer;
import org.apache.nifi.io.nio.consumer.StreamConsumerFactory;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author none
 */
public abstract class AbstractChannelReader implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractChannelReader.class);
    private final String uniqueId;
    private final SelectionKey key;
    private final BufferPool bufferPool;
    private final StreamConsumer consumer;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicReference<ScheduledFuture<?>> future = new AtomicReference<>(null);//the future on which this reader runs...

    public AbstractChannelReader(final String id, final SelectionKey key, final BufferPool empties, final StreamConsumerFactory consumerFactory) {
        this.uniqueId = id;
        this.key = key;
        this.bufferPool = empties;
        this.consumer = consumerFactory.newInstance(id);
        consumer.setReturnBufferQueue(bufferPool);
    }

    protected void setScheduledFuture(final ScheduledFuture<?> future) {
        this.future.set(future);
    }

    protected ScheduledFuture<?> getScheduledFuture() {
        return future.get();
    }

    protected SelectionKey getSelectionKey() {
        return key;
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    private void closeStream() {
        if (isClosed.get()) {
            return;
        }
        try {
            isClosed.set(true);
            future.get().cancel(false);
            key.cancel();
            key.channel().close();
        } catch (final IOException ioe) {
            LOGGER.warn("Unable to cleanly close stream due to " + ioe);
        } finally {
            consumer.signalEndOfStream();
        }
    }

    /**
     * Allows a subclass to specifically handle how it reads from the given
     * key's channel into the given buffer.
     *
     * @param key
     * @param buffer
     * @return the number of bytes read in the final read cycle. A value of zero
     * or more indicates the channel is still open but a value of -1 indicates
     * end of stream.
     * @throws IOException
     */
    protected abstract int fillBuffer(SelectionKey key, ByteBuffer buffer) throws IOException;

    @Override
    public final void run() {
        if (!key.isValid() || consumer.isConsumerFinished()) {
            closeStream();
            return;
        }
        if (!key.isReadable()) {
            return;//there is nothing available to read...or we aren't allow to read due to throttling
        }
        ByteBuffer buffer = null;
        try {
            buffer = bufferPool.poll();
            if (buffer == null) {
                return; // no buffers available - come back later
            }
            final int bytesRead = fillBuffer(key, buffer);
            buffer.flip();
            if (buffer.remaining() > 0) {
                consumer.addFilledBuffer(buffer);
                buffer = null; //clear the reference - is now the consumer's responsiblity
            } else {
                buffer.clear();
                bufferPool.returnBuffer(buffer, 0);
                buffer = null; //clear the reference - is now back to the queue
            }
            if (bytesRead < 0) { //we've reached the end
                closeStream();
            }
        } catch (final Exception ioe) {
            closeStream();
            LOGGER.error("Closed channel reader " + this + " due to " + ioe);
        } finally {
            if (buffer != null) {
                buffer.clear();
                bufferPool.returnBuffer(buffer, 0);
            }
        }
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
        AbstractChannelReader rhs = (AbstractChannelReader) obj;
        return new EqualsBuilder().appendSuper(super.equals(obj)).append(uniqueId, rhs.uniqueId).isEquals();
    }

    @Override
    public final int hashCode() {
        return new HashCodeBuilder(17, 37).append(uniqueId).toHashCode();
    }

    @Override
    public final String toString() {
        return new ToStringBuilder(this, ToStringStyle.NO_FIELD_NAMES_STYLE).append(uniqueId).toString();
    }
}
