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

package org.apache.nifi.controller.repository.claim;

import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.io.ContentClaimOutputStream;
import org.apache.nifi.controller.repository.metrics.PerformanceTracker;
import org.apache.nifi.controller.repository.metrics.PerformanceTrackingOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class StandardContentClaimWriteCache implements ContentClaimWriteCache {
    private final ContentRepository contentRepo;
    private final Map<ResourceClaim, MappedOutputStream> streamMap = new ConcurrentHashMap<>();
    private final Queue<ContentClaim> queue = new LinkedList<>();
    private final PerformanceTracker performanceTracker;
    private final long maxAppendableClaimBytes;
    private final int bufferSize;

    public StandardContentClaimWriteCache(final ContentRepository contentRepo, final PerformanceTracker performanceTracker, final long maxAppendableClaimBytes, final int bufferSize) {
        this.contentRepo = contentRepo;
        this.performanceTracker = performanceTracker;
        this.maxAppendableClaimBytes = maxAppendableClaimBytes;
        this.bufferSize = bufferSize;
    }

    @Override
    public void reset() throws IOException {
        try {
            forEachStream(OutputStream::close);
        } finally {
            streamMap.clear();
            queue.clear();
        }
    }

    @Override
    public ContentClaim getContentClaim() throws IOException {
        final ContentClaim contentClaim = queue.poll();
        if (contentClaim != null) {
            flush(contentClaim);

            final MappedOutputStream mappedOutputStream = streamMap.get(contentClaim.getResourceClaim());
            if (mappedOutputStream != null) {
                final OutputStream contentRepoStream = mappedOutputStream.getContentRepoStream();
                if (contentRepoStream instanceof ContentClaimOutputStream) {
                    return ((ContentClaimOutputStream) contentRepoStream).newContentClaim();
                }
            }
        }

        final ContentClaim claim = contentRepo.create(false);
        registerStream(claim);
        return claim;
    }

    private OutputStream registerStream(final ContentClaim contentClaim) throws IOException {
        final OutputStream out = contentRepo.write(contentClaim);
        final OutputStream performanceTrackingOut = new PerformanceTrackingOutputStream(out, performanceTracker);
        final OutputStream buffered = new BufferedOutputStream(performanceTrackingOut, bufferSize);

        final MappedOutputStream mappedOutputStream = new MappedOutputStream(out, buffered);
        streamMap.put(contentClaim.getResourceClaim(), mappedOutputStream);
        return buffered;
    }

    private OutputStream getWritableStream(final ResourceClaim claim) {
        final MappedOutputStream mappedOutputStream = streamMap.get(claim);
        if (mappedOutputStream == null) {
            return null;
        }

        return mappedOutputStream.getBufferedStream();
    }

    @Override
    public OutputStream write(final ContentClaim claim) throws IOException {
        OutputStream out = getWritableStream(claim.getResourceClaim());
        if (out == null) {
            out = registerStream(claim);
        }

        if (!(claim instanceof StandardContentClaim)) {
            // we know that we will only create Content Claims that are of type StandardContentClaim, so if we get anything
            // else, just throw an Exception because it is not valid for this Repository
            throw new IllegalArgumentException("Cannot write to " + claim + " because that Content Claim does belong to this Claim Cache");
        }

        final StandardContentClaim scc = (StandardContentClaim) claim;
        final long initialLength = Math.max(0L, scc.getLength());

        final OutputStream bcos = out;
        return new OutputStream() {
            private boolean closed = false;

            private long bytesWritten = 0L;

            @Override
            public void write(final int b) throws IOException {
                bcos.write(b);
                bytesWritten++;
                scc.setLength(initialLength + bytesWritten);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                bcos.write(b, off, len);
                bytesWritten += len;
                scc.setLength(initialLength + bytesWritten);
            }

            @Override
            public void write(final byte[] b) throws IOException {
                write(b, 0, b.length);
            }

            @Override
            public void flush() throws IOException {
                // do nothing - do not flush underlying stream.
            }

            @Override
            public void close() {
                if (closed) {
                    return;
                }
                closed = true;

                if (scc.getLength() < 0) {
                    // If claim was not written to, set length to 0
                    scc.setLength(0L);
                }

                // Add the claim back to the queue if it is still writable
                if ((scc.getOffset() + scc.getLength()) < maxAppendableClaimBytes) {
                    queue.offer(claim);
                }
            }
        };
    }

    @Override
    public void flush(final ContentClaim contentClaim) throws IOException {
        if (contentClaim == null) {
            return;
        }

        flush(contentClaim.getResourceClaim());
    }

    @Override
    public void flush(final ResourceClaim claim) throws IOException {
        final MappedOutputStream mapped = streamMap.get(claim);
        if (mapped != null) {
            mapped.getBufferedStream().flush();
        }
    }

    @Override
    public void flush() throws IOException {
        forEachStream(OutputStream::flush);
    }

    private void forEachStream(final StreamProcessor proc) throws IOException {
        IOException exception = null;

        for (final MappedOutputStream mapped : streamMap.values()) {
            try {
                proc.process(mapped.getBufferedStream());
            } catch (final IOException ioe) {
                if (exception == null) {
                    exception = ioe;
                } else {
                    ioe.addSuppressed(exception);
                    exception = ioe;
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    private interface StreamProcessor {
        void process(final OutputStream out) throws IOException;
    }

    private static class MappedOutputStream {
        private final OutputStream contentRepoStream;
        private final OutputStream bufferedStream;

        public MappedOutputStream(final OutputStream contentRepoStream, final OutputStream bufferedStream) {
            this.contentRepoStream = contentRepoStream;
            this.bufferedStream = bufferedStream;
        }

        public OutputStream getContentRepoStream() {
            return contentRepoStream;
        }

        public OutputStream getBufferedStream() {
            return bufferedStream;
        }
    }
}
