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
package org.apache.nifi.processors.opentelemetry.encoding;

import com.google.protobuf.Message;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Service Request Reader implementation based on JSON Parser supporting standard OTLP Request Types
 */
public class JsonServiceRequestReader implements ServiceRequestReader {
    private static final RequestMapper REQUEST_MAPPER = new StandardRequestMapper();

    /**
     * Read Service Request parsed from stream
     *
     * @param inputStream Input Stream to be parsed
     * @param requestType Request Message Type
     * @return Service Request read
     */
    @Override
    public <T extends Message> T read(final InputStream inputStream, final Class<T> requestType) {
        Objects.requireNonNull(inputStream, "Input Stream required");

        try {
            return REQUEST_MAPPER.readValue(inputStream, requestType);
        } catch (final IOException e) {
            final String message = String.format("JSON Request Type [%s] parsing failed", requestType.getName());
            throw new UncheckedIOException(message, e);
        }
    }
}
