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
package org.apache.nifi.processors.standard.util;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.io.nio.BufferPool;
import org.apache.nifi.io.nio.consumer.StreamConsumer;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.OutputStreamCallback;

/**
 *
 */
public class UDPStreamConsumer implements StreamConsumer {

    private final ComponentLog logger;
    final List<FlowFile> newFlowFileQueue;
    private final String uniqueId;
    private BufferPool bufferPool = null;
    private final BlockingQueue<ByteBuffer> filledBuffers = new LinkedBlockingQueue<>();
    private final AtomicBoolean streamEnded = new AtomicBoolean(false);
    private final AtomicBoolean consumerDone = new AtomicBoolean(false);
    private ProcessSession session;
    private final UDPConsumerCallback udpCallback;

    public UDPStreamConsumer(final String streamId, final List<FlowFile> newFlowFiles, final long fileSizeTrigger, final ComponentLog logger,
            final boolean flowFilePerDatagram) {
        this.uniqueId = streamId;
        this.newFlowFileQueue = newFlowFiles;
        this.logger = logger;
        this.udpCallback = new UDPConsumerCallback(filledBuffers, fileSizeTrigger, flowFilePerDatagram);
    }

    @Override
    public void setReturnBufferQueue(final BufferPool pool) {
        this.bufferPool = pool;
        this.udpCallback.setBufferPool(pool);
    }

    @Override
    public void addFilledBuffer(final ByteBuffer buffer) {
        if (isConsumerFinished()) {
            bufferPool.returnBuffer(buffer, 0);
        } else {
            filledBuffers.add(buffer);
        }
    }

    private void close() {
        if (isConsumerFinished()) {
            return;
        }
        consumerDone.set(true);
        ByteBuffer buf = null;
        while ((buf = filledBuffers.poll()) != null) {
            bufferPool.returnBuffer(buf, 0);
        }
    }

    public void setSession(ProcessSession session) {
        this.session = session;
    }

    @Override
    public void process() throws IOException {
        if (isConsumerFinished()) {
            return;
        }

        FlowFile newFlowFile = null;
        try {
            if (streamEnded.get() && filledBuffers.isEmpty()) {
                close();
                return;
            }
            if (filledBuffers.isEmpty()) {
                return;
            }
            // time to make a new flow file
            newFlowFile = session.create();
            newFlowFile = session.putAttribute(newFlowFile, "source.stream.identifier", uniqueId);
            newFlowFile = session.write(newFlowFile, udpCallback);
            if (newFlowFile.getSize() == 0) {
                session.remove(newFlowFile);
                return;
            }
            newFlowFileQueue.add(newFlowFile);
        } catch (final Exception ex) {
            close();
            if (newFlowFile != null) {
                try {
                    session.remove(newFlowFile);
                } catch (final Exception ex2) {
                    logger.warn("Unable to delete partial flow file due to: ", ex2);
                }
            }
            throw new IOException("Problem while processing data stream", ex);
        }
    }

    @Override
    public void signalEndOfStream() {
        streamEnded.set(true);
    }

    @Override
    public boolean isConsumerFinished() {
        return consumerDone.get();
    }

    @Override
    public String getId() {
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
        UDPStreamConsumer rhs = (UDPStreamConsumer) obj;
        return new EqualsBuilder().appendSuper(super.equals(obj)).append(uniqueId, rhs.uniqueId).isEquals();
    }

    @Override
    public final int hashCode() {
        return new HashCodeBuilder(17, 37).append(uniqueId).toHashCode();
    }

    @Override
    public final String toString() {
        return new ToStringBuilder(this).append(uniqueId).toString();
    }

    public static final class UDPConsumerCallback implements OutputStreamCallback {

        BufferPool bufferPool;
        final BlockingQueue<ByteBuffer> filledBuffers;
        final long fileSizeTrigger;
        final boolean flowFilePerDatagram;

        public UDPConsumerCallback(final BlockingQueue<ByteBuffer> filledBuffers, final long fileSizeTrigger, final boolean flowFilePerDatagram) {
            this.filledBuffers = filledBuffers;
            this.fileSizeTrigger = fileSizeTrigger;
            this.flowFilePerDatagram = flowFilePerDatagram;
        }

        public void setBufferPool(BufferPool pool) {
            this.bufferPool = pool;
        }

        @Override
        public void process(final OutputStream out) throws IOException {
            try {
                long totalBytes = 0L;
                try (WritableByteChannel wbc = Channels.newChannel(new BufferedOutputStream(out))) {
                    ByteBuffer buffer = null;
                    while ((buffer = filledBuffers.poll(50, TimeUnit.MILLISECONDS)) != null) {
                        int bytesWrittenThisPass = 0;
                        try {
                            while (buffer.hasRemaining()) {
                                bytesWrittenThisPass += wbc.write(buffer);
                            }
                            totalBytes += bytesWrittenThisPass;
                            if (totalBytes > fileSizeTrigger || flowFilePerDatagram) {
                                break;// this is enough data
                            }
                        } finally {
                            bufferPool.returnBuffer(buffer, bytesWrittenThisPass);
                        }
                    }
                }
            } catch (final InterruptedException ie) {
                // irrelevant
            }
        }

    }
}
