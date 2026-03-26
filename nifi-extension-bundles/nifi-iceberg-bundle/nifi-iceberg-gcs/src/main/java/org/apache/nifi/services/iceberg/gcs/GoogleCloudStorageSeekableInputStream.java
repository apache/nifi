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
package org.apache.nifi.services.iceberg.gcs;

import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PARTIAL;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.CONTENT_RANGE;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.ENCRYPTION_ALGORITHM;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.ENCRYPTION_KEY;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.ENCRYPTION_KEY_SHA256;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.RANGE;

/**
 * Google Cloud Storage implementation of SeekableInputStream supporting chunked buffered reads
 */
class GoogleCloudStorageSeekableInputStream extends SeekableInputStream {

    private static final Pattern CONTENT_RANGE_TOTAL = Pattern.compile("/(\\d+)$");
    private static final int FIRST_GROUP = 1;
    private static final int END_OF_STREAM = -1;

    private static final String RANGE_FORMAT = "bytes=%d-%d";

    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;

    private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStorageSeekableInputStream.class);

    private final HttpClientProvider httpClientProvider;
    private final GoogleCloudStorageLocation location;
    private final int readChunkSize;
    private final String downloadUri;
    private final String decryptionKey;
    private final String encryptionAlgorithm;

    private long pos;
    private long contentLength;
    private boolean contentLengthProvided;
    private byte[] buffer;
    private long bufferStart;
    private int bufferLength;
    private boolean closed;

    /**
     * Google Cloud Storage SeekableInputStream constructor with optional Content Length
     *
     * @param httpClientProvider HTTP Client Provider
     * @param storageProperties Google Cloud Storage Properties
     * @param location Google Cloud Storage Location
     * @param contentLength Content Length or null when not known
     */
    GoogleCloudStorageSeekableInputStream(
            final HttpClientProvider httpClientProvider,
            final GoogleCloudStorageProperties storageProperties,
            final GoogleCloudStorageLocation location,
            final Long contentLength
    ) {
        this.httpClientProvider = httpClientProvider;
        this.location = location;
        this.readChunkSize = storageProperties.readChunkSize();
        this.downloadUri = storageProperties.downloadUri(location);
        this.decryptionKey = storageProperties.decryptionKey();
        this.encryptionAlgorithm = storageProperties.encryptionAlgorithm();
        if (contentLength != null) {
            this.contentLength = contentLength;
            this.contentLengthProvided = true;
        }
    }

    /**
     * Get current stream position
     *
     * @return Current position
     */
    @Override
    public long getPos() {
        return pos;
    }

    /**
     * Seek to new position
     *
     * @param newPos New position
     */
    @Override
    public void seek(final long newPos) {
        if (newPos < 0) {
            throw new IllegalArgumentException("Seek position must not be negative [%d]".formatted(newPos));
        }
        this.pos = newPos;
    }

    /**
     * Read data and buffer as needed
     *
     * @return Data byte read
     * @throws IOException Thrown on failure to buffer
     */
    @Override
    public int read() throws IOException {
        ensureOpen();
        if (contentLengthProvided && pos >= contentLength) {
            return END_OF_STREAM;
        }

        ensureBuffer();
        if (bufferLength == 0) {
            return END_OF_STREAM;
        }

        final int offset = (int) (pos - bufferStart);
        if (offset >= bufferLength) {
            return END_OF_STREAM;
        }

        pos++;
        return buffer[offset] & 0xFF;
    }

    /**
     * Read data into buffer with start offset and length requested
     *
     * @param data Data buffer for bytes read
     * @param offset Start offset for data buffer
     * @param length Maximum number of bytes to read
     * @return Number of bytes read
     * @throws IOException Thrown on failure to buffer
     */
    @Override
    public int read(final byte[] data, final int offset, final int length) throws IOException {
        ensureOpen();
        if (length == 0) {
            return 0;
        }
        if (contentLengthProvided && pos >= contentLength) {
            return END_OF_STREAM;
        }

        int totalRead = 0;
        int remaining = length;
        int dataOffset = offset;

        while (remaining > 0) {
            ensureBuffer();
            if (bufferLength == 0) {
                break;
            }
            final int bufOffset = (int) (pos - bufferStart);
            if (bufOffset >= bufferLength) {
                break;
            }
            final int available = bufferLength - bufOffset;
            final int copyLength = Math.min(remaining, available);
            System.arraycopy(buffer, bufOffset, data, dataOffset, copyLength);
            pos += copyLength;
            totalRead += copyLength;
            dataOffset += copyLength;
            remaining -= copyLength;
        }

        return totalRead == 0 ? END_OF_STREAM : totalRead;
    }

    /**
     * Get number of bytes available in buffer for reading
     *
     * @return Number of bytes available for reading
     */
    @Override
    public int available() {
        if (buffer == null) {
            return 0;
        }
        final int bufferOffset = (int) (pos - bufferStart);
        final int bufferRemaining = bufferLength - bufferOffset;
        return Math.max(0, bufferRemaining);
    }

    /**
     * Close stream and clear buffer
     */
    @Override
    public void close() {
        closed = true;
        buffer = null;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    private void ensureBuffer() throws IOException {
        if (buffer != null) {
            final int offset = (int) (pos - bufferStart);
            if (offset >= 0 && offset < bufferLength) {
                return;
            }
        }
        fillBuffer();
    }

    private void fillBuffer() throws IOException {
        final long rangeStart = pos;
        final long rangeEnd = rangeStart + readChunkSize - 1;
        final String range = RANGE_FORMAT.formatted(rangeStart, rangeEnd);

        final HttpRequest.Builder builder = httpClientProvider.newRequestBuilder(downloadUri)
                .GET()
                .header(RANGE.getHeader(), range);

        addDecryptionHeaders(builder);

        final HttpResponse<byte[]> response = httpClientProvider.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        final int statusCode = response.statusCode();
        logger.debug("Read object [{}] Range [{}] HTTP {}", downloadUri, range, statusCode);

        // Set empty buffer when Google Cloud Storage cannot provide range requested
        if (HTTP_RANGE_NOT_SATISFIABLE == statusCode) {
            bufferStart = pos;
            bufferLength = 0;
            if (buffer == null) {
                buffer = new byte[0];
            }
            return;
        }

        if (statusCode != HTTP_OK && statusCode != HTTP_PARTIAL) {
            throw new IOException("Read object failed [%s] Range [%s] HTTP %d".formatted(location, range, statusCode));
        }

        // Set Content-Length when not provided
        if (!contentLengthProvided) {
            parseContentLength(response);
        }

        buffer = response.body();
        bufferStart = rangeStart;
        bufferLength = buffer.length;
    }

    private void parseContentLength(final HttpResponse<?> response) {
        response.headers().firstValue(CONTENT_RANGE.getHeader()).ifPresent(header -> {
            final Matcher matcher = CONTENT_RANGE_TOTAL.matcher(header);
            if (matcher.find()) {
                final String contentRangeTotal = matcher.group(FIRST_GROUP);
                contentLength = Long.parseLong(contentRangeTotal);
                contentLengthProvided = true;
            }
        });
    }

    private void addDecryptionHeaders(final HttpRequest.Builder builder) {
        if (decryptionKey != null && !decryptionKey.isBlank()) {
            builder.header(ENCRYPTION_ALGORITHM.getHeader(), encryptionAlgorithm);
            builder.header(ENCRYPTION_KEY.getHeader(), decryptionKey);
            builder.header(ENCRYPTION_KEY_SHA256.getHeader(), KeyDigestProvider.getDigestEncoded(decryptionKey));
        }
    }
}
