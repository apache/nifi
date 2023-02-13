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

import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.metrics.PerformanceTracker;
import org.apache.nifi.controller.repository.metrics.PerformanceTrackingInputStream;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An InputStream that is provided a Content Repository, Content Claim, and offset into the Content Claim where a FlowFile's
 * content begins, and is capable of reading the content from the Content Repository, as well as providing the ability to use
 * {@link #mark(int)}/{@link #reset()} in order to re-read content without buffering it.
 */
public class ContentClaimInputStream extends InputStream {
    private final ContentRepository contentRepository;
    private final ContentClaim contentClaim;
    private final long claimOffset;
    private final PerformanceTracker performanceTracker;

    private InputStream delegate;
    private long bytesConsumed;
    private long currentOffset; // offset into the Content Claim; will differ from bytesRead if reset() is called after reading at least one byte or if claimOffset > 0
    private long markOffset;
    private InputStream bufferedIn;
    private int markReadLimit;

    public ContentClaimInputStream(final ContentRepository contentRepository, final ContentClaim contentClaim, final long claimOffset, final PerformanceTracker performanceTracker) {
        this(contentRepository, contentClaim, claimOffset, null, performanceTracker);
    }

    public ContentClaimInputStream(final ContentRepository contentRepository, final ContentClaim contentClaim, final long claimOffset, final InputStream initialDelegate,
                                   final PerformanceTracker performanceTracker) {
        this.contentRepository = contentRepository;
        this.contentClaim = contentClaim;
        this.claimOffset = claimOffset;
        this.performanceTracker = performanceTracker;

        this.currentOffset = claimOffset;
        this.delegate = initialDelegate;
        if (delegate != null) {
            this.bufferedIn = new BufferedInputStream(delegate);
        }
    }

    private InputStream getDelegate() throws IOException {
        bufferedIn = null;
        if (delegate == null) {
            formDelegate();
        }

        return delegate;
    }

    public long getBytesConsumed() {
        return bytesConsumed;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public int read() throws IOException {
        int value = -1;
        if (bufferedIn != null) {
            value = bufferedIn.read();
        }

        if (value < 0) {
            value = getDelegate().read();
        }
        if (value != -1) {
            bytesConsumed++;
            currentOffset++;
        }

        return value;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        int count = -1;
        if (bufferedIn != null) {
            count = bufferedIn.read(b);
        }
        if (count < 0) {
            count = getDelegate().read(b);
        }

        if (count != -1) {
            bytesConsumed += count;
            currentOffset += count;
        }

        return count;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        int count = -1;
        if (bufferedIn != null) {
            count = bufferedIn.read(b, off, len);
        }
        if (count < 0) {
            count = getDelegate().read(b, off, len);
        }

        if (count != -1) {
            bytesConsumed += count;
            currentOffset += count;
        }

        return count;
    }

    @Override
    public long skip(final long n) throws IOException {
        final long count = getDelegate().skip(n);
        if (count > 0) {
            bytesConsumed += count;
            currentOffset += count;
        }

        return count;
    }

    @Override
    public int available() throws IOException {
        if (delegate == null) {
            return 0;
        }

        return delegate.available();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(final int readlimit) {
        markOffset = currentOffset;
        markReadLimit = readlimit;
        if (bufferedIn != null) {
            bufferedIn.mark(readlimit);
        }
    }

    @Override
    public void reset() throws IOException {
        if (markOffset < 0) {
            throw new IOException("Stream has not been marked");
        }

        if (bufferedIn != null && bytesConsumed <= markReadLimit) {
            bufferedIn.reset();
            currentOffset = markOffset;

            return;
        }

        if (currentOffset != markOffset) {
            if (delegate != null) {
                delegate.close();
            }

            formDelegate();

            performanceTracker.beginContentRead();
            try {
                StreamUtils.skip(delegate, markOffset - claimOffset);
            } finally {
                performanceTracker.endContentRead();
            }

            currentOffset = markOffset;
        }
    }

    @Override
    public void close() throws IOException {
        if (delegate != null) {
            delegate.close();
        }
    }

    private void formDelegate() throws IOException {
        if (delegate != null) {
            delegate.close();
        }

        performanceTracker.beginContentRead();
        try {
            delegate = new PerformanceTrackingInputStream(contentRepository.read(contentClaim), performanceTracker);
            StreamUtils.skip(delegate, claimOffset);
            currentOffset = claimOffset;

            if (markReadLimit > 0) {
                final int limitLeft = (int) (markReadLimit - currentOffset);
                if (limitLeft > 0) {
                    final InputStream limitedIn = new LimitedInputStream(delegate, limitLeft);
                    bufferedIn = new BufferedInputStream(limitedIn);
                    bufferedIn.mark(limitLeft);
                }
            }
        } finally {
            performanceTracker.endContentRead();
        }
    }
}
