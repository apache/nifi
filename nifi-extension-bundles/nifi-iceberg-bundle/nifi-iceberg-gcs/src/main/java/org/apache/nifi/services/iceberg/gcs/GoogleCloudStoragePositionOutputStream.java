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

import org.apache.iceberg.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.CONTENT_RANGE;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.ENCRYPTION_ALGORITHM;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.ENCRYPTION_KEY;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.ENCRYPTION_KEY_SHA256;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.LOCATION;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.UPLOAD_CONTENT_TYPE;

/**
 * Google Cloud Storage implementation of PositionOutputStream supporting resumable uploads
 */
class GoogleCloudStoragePositionOutputStream extends PositionOutputStream {

    private static final String CONTENT_TYPE_OCTET_STREAM = "application/octet-stream";
    private static final String CONTENT_RANGE_FORMAT = "bytes %d-%d/%s";
    private static final String RANGE_TOTAL_UNKNOWN = "*";

    private static final int HTTP_RESUME_INCOMPLETE = 308;

    private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStoragePositionOutputStream.class);

    private final HttpClientProvider httpClientProvider;
    private final GoogleCloudStorageProperties storageProperties;
    private final String sessionUri;
    private final byte[] buffer;
    private final int chunkSize;
    private int bufferPosition;
    private long position;
    private boolean closed;

    /**
     * Google Cloud Storage Position OutputStream constructor initiates a resumable upload on creation
     *
     * @param httpClientProvider HTTP Client Provider
     * @param storageProperties Google Cloud Storage Properties
     * @param location Google Cloud Storage Location
     * @throws IOException Thrown on failure to initiate upload
     */
    GoogleCloudStoragePositionOutputStream(
            final HttpClientProvider httpClientProvider,
            final GoogleCloudStorageProperties storageProperties,
            final GoogleCloudStorageLocation location
    ) throws IOException {
        this.httpClientProvider = httpClientProvider;
        this.storageProperties = storageProperties;
        this.chunkSize = storageProperties.writeChunkSize();
        this.buffer = new byte[chunkSize];
        this.sessionUri = initiateUpload(location);
    }

    /**
     * Get stream position
     *
     * @return Current stream position
     */
    @Override
    public long getPos() {
        return position;
    }

    /**
     * Write byte to stream and increment position
     *
     * @param data byte to be written
     * @throws IOException Thrown on failure to send chunk
     */
    @Override
    public void write(final int data) throws IOException {
        ensureOpen();
        if (bufferPosition >= chunkSize) {
            sendChunk(false);
        }
        buffer[bufferPosition++] = (byte) data;
        position++;
    }

    /**
     * Write bytes to stream and increment position
     *
     * @param data bytes to be written
     * @param offset initial offset in bytes to be written
     * @param length number of bytes to be written
     * @throws IOException Thrown on failure to send chunk
     */
    @Override
    public void write(final byte[] data, final int offset, final int length) throws IOException {
        ensureOpen();

        int remaining = length;
        int currentOffset = offset;
        while (remaining > 0) {
            if (bufferPosition >= chunkSize) {
                sendChunk(false);
            }
            final int space = chunkSize - bufferPosition;
            final int copyLength = Math.min(remaining, space);

            System.arraycopy(data, currentOffset, buffer, bufferPosition, copyLength);
            bufferPosition += copyLength;
            currentOffset += copyLength;
            remaining -= copyLength;
            position += copyLength;
        }
    }

    /**
     * Close stream and send final chunk
     *
     * @throws IOException Thrown on failure to send chunk
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            sendChunk(true);
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    private String initiateUpload(final GoogleCloudStorageLocation location) throws IOException {
        final String uri = storageProperties.uploadUri(location);
        final HttpRequest.Builder builder = httpClientProvider.newRequestBuilder(uri)
                .POST(HttpRequest.BodyPublishers.noBody())
                .header(UPLOAD_CONTENT_TYPE.getHeader(), CONTENT_TYPE_OCTET_STREAM);

        addEncryptionHeaders(builder);

        final HttpResponse<String> response = httpClientProvider.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        final int statusCode = response.statusCode();
        if (HTTP_OK == statusCode) {
            final HttpHeaders headers = response.headers();
            final Optional<String> locationHeaderFound = headers.firstValue(LOCATION.getHeader());
            if (locationHeaderFound.isPresent()) {
                logger.debug("Upload initiated [{}] HTTP {}", uri, statusCode);
                return locationHeaderFound.get();
            } else {
                throw new IOException("Initiate upload failed [%s] Location response header not found".formatted(location));
            }
        } else {
            final String responseBody = response.body();
            throw new IOException("Initiate upload failed [%s] HTTP %d [%s]".formatted(location, statusCode, responseBody));
        }
    }

    private void sendChunk(final boolean lastChunk) throws IOException {
        if (bufferPosition == 0 && !lastChunk) {
            return;
        }

        final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(sessionUri))
                .timeout(HttpClientProvider.REQUEST_TIMEOUT);

        if (bufferPosition == 0) {
            builder.PUT(HttpRequest.BodyPublishers.noBody());
        } else {
            final long rangeStart = position - bufferPosition;
            final long rangeEnd = position - 1;
            final String total = lastChunk ? String.valueOf(position) : RANGE_TOTAL_UNKNOWN;
            final String contentRange = CONTENT_RANGE_FORMAT.formatted(rangeStart, rangeEnd, total);

            builder.PUT(HttpRequest.BodyPublishers.ofByteArray(buffer, 0, bufferPosition))
                    .header(CONTENT_RANGE.getHeader(), contentRange);
        }

        final HttpResponse<String> response = httpClientProvider.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        final int statusCode = response.statusCode();
        logger.debug("Buffer uploaded [{}] HTTP {}", sessionUri, statusCode);

        if (lastChunk) {
            if (statusCode != HTTP_OK && statusCode != HTTP_CREATED) {
                throw new IOException("Upload finalization failed HTTP %d [%s]".formatted(statusCode, response.body()));
            }
        } else {
            if (statusCode != HTTP_RESUME_INCOMPLETE) {
                throw new IOException("Chunk upload failed HTTP %d [%s]".formatted(statusCode, response.body()));
            }
        }

        bufferPosition = 0;
    }

    private void addEncryptionHeaders(final HttpRequest.Builder builder) {
        final String key = storageProperties.encryptionKey();
        if (key != null && !key.isBlank()) {
            builder.header(ENCRYPTION_ALGORITHM.getHeader(), storageProperties.encryptionAlgorithm());
            builder.header(ENCRYPTION_KEY.getHeader(), key);
            builder.header(ENCRYPTION_KEY_SHA256.getHeader(), KeyDigestProvider.getDigestEncoded(key));
        }
    }
}
