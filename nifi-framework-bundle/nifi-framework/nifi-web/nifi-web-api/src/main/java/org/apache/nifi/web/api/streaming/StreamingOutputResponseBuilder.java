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
package org.apache.nifi.web.api.streaming;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * HTTP Response Builder for Streaming Output with optional Range header handling
 */
public class StreamingOutputResponseBuilder {
    static final String ACCEPT_RANGES_HEADER = "Accept-Ranges";

    static final String CONTENT_RANGE_HEADER = "Content-Range";

    private static final String BYTES_UNIT = "bytes";

    private static final String CONTENT_RANGE_BYTES = "bytes %d-%d/%d";

    private static final int LAST_POSITION_OFFSET = -1;

    private static final ByteRangeParser byteRangeParser = new StandardByteRangeParser();

    private final InputStream inputStream;

    private String range;

    private boolean acceptRanges;

    /**
     * Streaming Output Response Builder with required Input Stream
     *
     * @param inputStream Input Stream to be transferred
     */
    public StreamingOutputResponseBuilder(final InputStream inputStream) {
        this.inputStream = Objects.requireNonNull(inputStream, "Input Stream required");
    }

    /**
     * Set HTTP Range header
     *
     * @param range Range header can be null or empty
     * @return Builder
     */
    public StreamingOutputResponseBuilder range(final String range) {
        this.range = range;
        this.acceptRanges = true;
        return this;
    }

    /**
     * Process arguments and prepare HTTP Response Builder
     *
     * @return Response Builder
     */
    public Response.ResponseBuilder build() {
        final Response.ResponseBuilder responseBuilder;

        final Optional<ByteRange> byteRangeFound = byteRangeParser.readByteRange(range);
        if (byteRangeFound.isPresent()) {
            final int completeLength = getCompleteLength();
            final ByteRange byteRange = byteRangeFound.get();
            final StreamingOutput streamingOutput = new ByteRangeStreamingOutput(inputStream, byteRange);
            responseBuilder = Response.status(Response.Status.PARTIAL_CONTENT).entity(streamingOutput);

            final String contentRange = getContentRange(byteRange, completeLength);
            responseBuilder.header(CONTENT_RANGE_HEADER, contentRange);
        } else {
            final StreamingOutput streamingOutput = new InputStreamingOutput(inputStream);
            responseBuilder = Response.ok(streamingOutput);
        }

        if (acceptRanges) {
            responseBuilder.header(ACCEPT_RANGES_HEADER, BYTES_UNIT);
        }

        return responseBuilder;
    }

    private int getCompleteLength() {
        try {
            return inputStream.available();
        } catch (final IOException e) {
            throw new UncheckedIOException("Complete Length read failed", e);
        }
    }

    private String getContentRange(final ByteRange byteRange, final int completeLength) {
        final OptionalLong lastPositionFound = byteRange.getLastPosition();
        final OptionalLong firstPositionFound = byteRange.getFirstPosition();

        final long lastPositionCompleteLength = completeLength - LAST_POSITION_OFFSET;

        final long lastPosition;
        if (lastPositionFound.isEmpty()) {
            lastPosition = lastPositionCompleteLength;
        } else {
            final long lastPositionRequested = lastPositionFound.getAsLong();
            lastPosition = Math.min(lastPositionRequested, lastPositionCompleteLength);
        }

        final long firstPosition;
        if (firstPositionFound.isEmpty()) {
            firstPosition = 0;
        } else {
            firstPosition = firstPositionFound.getAsLong();
        }

        return CONTENT_RANGE_BYTES.formatted(firstPosition, lastPosition, completeLength);
    }
}
