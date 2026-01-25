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
package org.apache.nifi.processors.box;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * A class responsible for writing metadata objects into a JSON array.
 */
final class BoxMetadataJsonArrayWriter implements Closeable {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private final JsonGenerator generator;
    private boolean hasBegun;
    private boolean closed;

    private BoxMetadataJsonArrayWriter(final JsonGenerator generator) {
        this.generator = generator;
        this.hasBegun = false;
        this.closed = false;
    }

    static BoxMetadataJsonArrayWriter create(final OutputStream outputStream) throws IOException {
        final JsonGenerator generator = JSON_FACTORY.createGenerator(outputStream);
        return new BoxMetadataJsonArrayWriter(generator);
    }

    void write(final Map<String, Object> templateFields) throws IOException {
        if (closed) {
            throw new IOException("The Writer is closed");
        }

        if (!hasBegun) {
            generator.writeStartArray();
            hasBegun = true;
        }

        writeRecord(templateFields);
    }

    private void writeRecord(final Map<String, Object> templateFields) throws IOException {
        generator.writeStartObject();

        for (Map.Entry<String, Object> entry : templateFields.entrySet()) {
            Object value = entry.getValue();
            if (value == null) {
                generator.writeNullField(entry.getKey());
            } else {
                generator.writeStringField(entry.getKey(), value.toString());
            }
        }

        generator.writeEndObject();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;

        if (!hasBegun) {
            generator.writeStartArray();
        }
        generator.writeEndArray();

        generator.close();
    }
}

