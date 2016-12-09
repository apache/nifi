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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.stream.io.ByteCountingOutputStream;

public class ContentClaimWriteCache {
    private final ContentRepository contentRepo;
    private final Map<ResourceClaim, ByteCountingOutputStream> streamMap = new HashMap<>();
    private final Queue<ContentClaim> queue = new LinkedList<>();

    public ContentClaimWriteCache(final ContentRepository contentRepo) {
        this.contentRepo = contentRepo;
    }

    public void reset() throws IOException {
        try {
            forEachStream(OutputStream::close);
        } finally {
            streamMap.clear();
            queue.clear();
        }
    }

    public ContentClaim getContentClaim() throws IOException {
        final ContentClaim contentClaim = queue.poll();
        if (contentClaim != null) {
            contentRepo.incrementClaimaintCount(contentClaim);
            return contentClaim;
        }

        final ContentClaim claim = contentRepo.create(false);
        registerStream(claim);
        return claim;
    }

    private ByteCountingOutputStream registerStream(final ContentClaim contentClaim) throws IOException {
        final OutputStream out = contentRepo.write(contentClaim);
        final OutputStream buffered = new BufferedOutputStream(out);
        final ByteCountingOutputStream bcos = new ByteCountingOutputStream(buffered);
        streamMap.put(contentClaim.getResourceClaim(), bcos);
        return bcos;
    }

    public OutputStream write(final ContentClaim claim) throws IOException {
        OutputStream out = streamMap.get(claim.getResourceClaim());
        if (out == null) {
            out = registerStream(claim);
        }

        if (!(claim instanceof StandardContentClaim)) {
            // we know that we will only create Content Claims that are of type StandardContentClaim, so if we get anything
            // else, just throw an Exception because it is not valid for this Repository
            throw new IllegalArgumentException("Cannot write to " + claim + " because that Content Claim does belong to this Claim Cache");
        }

        final StandardContentClaim scc = (StandardContentClaim) claim;

        final OutputStream bcos = out;
        return new OutputStream() {
            @Override
            public void write(final int b) throws IOException {
                bcos.write(b);
                scc.setLength(Math.max(0L, scc.getLength()) + 1L);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                bcos.write(b, off, len);
                scc.setLength(Math.max(0L, scc.getLength()) + len);
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
            public void close() throws IOException {
                queue.offer(claim);
            }
        };
    }

    public void flush(final ContentClaim contentClaim) throws IOException {
        if (contentClaim == null) {
            return;
        }

        flush(contentClaim.getResourceClaim());
    }

    public void flush(final ResourceClaim claim) throws IOException {
        final OutputStream out = streamMap.get(claim);
        if (out != null) {
            out.flush();
        }
    }

    public void flush() throws IOException {
        forEachStream(OutputStream::flush);
    }

    private void forEachStream(final StreamProcessor proc) throws IOException {
        IOException exception = null;

        for (final OutputStream out : streamMap.values()) {
            try {
                proc.process(out);
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
}
