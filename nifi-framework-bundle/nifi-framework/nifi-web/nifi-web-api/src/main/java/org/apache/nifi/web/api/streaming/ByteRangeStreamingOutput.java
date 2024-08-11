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

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.nifi.stream.io.LimitingInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Streaming Output implementation supporting HTTP Range Header with specified first and last byte positions
 */
public class ByteRangeStreamingOutput implements StreamingOutput {
    private final InputStream inputStream;

    private final ByteRange byteRange;

    /**
     * Byte Range Streaming Output with required arguments
     *
     * @param inputStream Input Stream to be transferred
     * @param byteRange Byte Range containing first and last positions
     */
    public ByteRangeStreamingOutput(final InputStream inputStream, final ByteRange byteRange) {
        Objects.requireNonNull(inputStream, "Input Stream required");
        this.byteRange = Objects.requireNonNull(byteRange, "Byte Range required");

        final OptionalLong lastPositionFound = byteRange.getLastPosition();
        if (lastPositionFound.isPresent()) {
            final long lastPosition = lastPositionFound.getAsLong();

            final OptionalLong firstPositionFound = byteRange.getFirstPosition();
            if (firstPositionFound.isPresent()) {
                // Handle int-range when last position indicates limited number of bytes
                this.inputStream = new LimitingInputStream(inputStream, lastPosition);
            } else {
                // Handle suffix-range when last position indicates the last number of bytes from the end
                this.inputStream = inputStream;
            }
        } else {
            this.inputStream = inputStream;
        }
    }

    @Override
    public void write(final OutputStream outputStream) throws IOException, WebApplicationException {
        try (inputStream) {
            final OptionalLong firstPositionFound = byteRange.getFirstPosition();
            final OptionalLong lastPositionFound = byteRange.getLastPosition();

            if (firstPositionFound.isPresent()) {
                // Handle int-range with first position specified
                final long firstPosition = firstPositionFound.getAsLong();
                try {
                    inputStream.skipNBytes(firstPosition);
                } catch (final EOFException e) {
                    throw new RangeNotSatisfiableException("First Range Position [%d] not valid".formatted(firstPosition), e);
                }
            } else if (lastPositionFound.isPresent()) {
                // Handle suffix-range for last number of bytes specified
                final long lastPosition = lastPositionFound.getAsLong();
                final long available = inputStream.available();
                final long skip = available - lastPosition;
                inputStream.skipNBytes(skip);
            }

            inputStream.transferTo(outputStream);
        }
    }
}
