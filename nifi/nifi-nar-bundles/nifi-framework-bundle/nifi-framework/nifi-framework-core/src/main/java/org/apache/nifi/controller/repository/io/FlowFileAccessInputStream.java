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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.FlowFileAccessException;

/**
 * <p>
 * Wraps an InputStream so that if any IOException is thrown, it will be wrapped in a FlowFileAccessException. We do this to isolate IOExceptions thrown by the framework from those thrown by user
 * code. If thrown by the framework, it generally indicates a problem communicating with the Content Repository and session rollback is often appropriate so that the FlowFile can be processed again.
 * </p>
 */
public class FlowFileAccessInputStream extends FilterInputStream {

    private final FlowFile flowFile;
    private final ContentClaim claim;
    private long bytesConsumed;
    private ContentNotFoundException thrown;

    public FlowFileAccessInputStream(final InputStream in, final FlowFile flowFile, final ContentClaim claim) {
        super(in);
        this.flowFile = flowFile;
        this.claim = claim;
    }

    private void ensureAllContentRead() throws ContentNotFoundException {
        if (bytesConsumed < flowFile.getSize()) {
            thrown = new ContentNotFoundException(claim, "Stream contained only " + bytesConsumed + " bytes but should have contained " + flowFile.getSize());
            throw thrown;
        }
    }

    /**
     * @return the ContentNotFoundException that was thrown by this stream, or <code>null</code> if no such Exception was thrown
     */
    public ContentNotFoundException getContentNotFoundException() {
        return thrown;
    }

    @Override
    public int read() throws IOException {
        try {
            final int byteRead = super.read();
            if (byteRead < 0) {
                ensureAllContentRead();
            } else {
                bytesConsumed++;
            }

            return byteRead;
        } catch (final ContentNotFoundException cnfe) {
            throw cnfe;
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not read from " + flowFile, ioe);
        }
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        try {
            final int count = super.read(b, off, len);
            if (count < 0) {
                ensureAllContentRead();
            } else {
                bytesConsumed += count;
            }

            return count;
        } catch (final ContentNotFoundException cnfe) {
            throw cnfe;
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not read from " + flowFile, ioe);
        }
    }

    @Override
    public int read(final byte[] b) throws IOException {
        try {
            final int count = super.read(b);
            if (count < 0) {
                ensureAllContentRead();
            } else {
                bytesConsumed += count;
            }

            return count;
        } catch (final ContentNotFoundException cnfe) {
            throw cnfe;
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not read from " + flowFile, ioe);
        }
    }

    @Override
    public int available() throws IOException {
        try {
            return super.available();
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not determine amount of data available from " + flowFile, ioe);
        }
    }

    @Override
    public boolean markSupported() {
        return super.markSupported();
    }

    @Override
    public long skip(final long n) throws IOException {
        try {
            final long count = super.skip(n);
            bytesConsumed += count;
            return count;
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not skip data in " + flowFile, ioe);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } catch (final IOException ioe) {
        }
    }

    @Override
    public void reset() throws IOException {
        try {
            super.reset();
        } catch (final IOException ioe) {
            throw new FlowFileAccessException("Could not reset stream from " + flowFile, ioe);
        }
    }

    @Override
    public void mark(final int n) {
        super.mark(n);
    }
}
