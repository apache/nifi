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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Streaming Output implementation supporting direct transfer of Input Stream
 */
public class InputStreamingOutput implements StreamingOutput {
    private final InputStream inputStream;

    /**
     * Streaming Output with required arguments
     *
     * @param inputStream Input Stream to be transferred
     */
    public InputStreamingOutput(final InputStream inputStream) {
        this.inputStream = Objects.requireNonNull(inputStream, "Input Stream required");
    }

    @Override
    public void write(final OutputStream outputStream) throws IOException, WebApplicationException {
        try (inputStream) {
            inputStream.transferTo(outputStream);
        }
    }
}
