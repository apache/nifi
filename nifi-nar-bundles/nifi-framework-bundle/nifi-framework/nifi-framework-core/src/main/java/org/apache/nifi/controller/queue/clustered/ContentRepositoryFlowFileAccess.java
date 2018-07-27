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

package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.io.LimitedInputStream;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ContentRepositoryFlowFileAccess implements FlowFileContentAccess {
    private final ContentRepository contentRepository;

    public ContentRepositoryFlowFileAccess(final ContentRepository contentRepository) {
        this.contentRepository = contentRepository;
    }

    @Override
    public InputStream read(final FlowFileRecord flowFile) throws IOException {
        final InputStream rawIn;
        try {
            rawIn = contentRepository.read(flowFile.getContentClaim());
        } catch (final ContentNotFoundException cnfe) {
            throw new ContentNotFoundException(flowFile, flowFile.getContentClaim(), cnfe.getMessage());
        }

        if (flowFile.getContentClaimOffset() > 0) {
            try {
                StreamUtils.skip(rawIn, flowFile.getContentClaimOffset());
            } catch (final EOFException eof) {
                throw new ContentNotFoundException(flowFile, flowFile.getContentClaim(), "FlowFile has a Content Claim Offset of "
                    + flowFile.getContentClaimOffset() + " bytes but the Content Claim does not have that many bytes");
            }
        }

        final InputStream limitedIn = new LimitedInputStream(rawIn, flowFile.getSize());
        // Wrap the Content Repository's InputStream with one that ensures that we are able to consume all of the FlowFile's content or else throws EOFException
        return new FilterInputStream(limitedIn) {
            private long bytesRead = 0;

            @Override
            public int read(final byte[] b, final int off, final int len) throws IOException {
                return ensureNotTruncated(limitedIn.read(b, off, len));
            }

            @Override
            public int read(final byte[] b) throws IOException {
                return ensureNotTruncated(limitedIn.read(b));
            }

            @Override
            public int read() throws IOException {
                return ensureNotTruncated(limitedIn.read());
            }

            private int ensureNotTruncated(final int length) throws EOFException {
                if (length > -1) {
                    bytesRead += length;
                    return length;
                }

                if (bytesRead < flowFile.getSize()) {
                    throw new EOFException("Expected " + flowFile + " to contain " + flowFile.getSize() + " bytes but the content repository only had " + bytesRead + " bytes for it");
                }

                return length;
            }
        };
    }

}
