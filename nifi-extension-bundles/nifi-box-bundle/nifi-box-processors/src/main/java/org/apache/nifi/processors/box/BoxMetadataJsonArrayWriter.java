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

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A class responsible for writing metadata objects into a JSON array.
 */
final class BoxMetadataJsonArrayWriter implements Closeable {

    private final Writer writer;
    private boolean hasBegun;
    private boolean hasEntries;
    private boolean closed;

    private BoxMetadataJsonArrayWriter(final Writer writer) {
        this.writer = writer;
        this.hasBegun = false;
        this.hasEntries = false;
        this.closed = false;
    }

    static BoxMetadataJsonArrayWriter create(final OutputStream outputStream) throws IOException {
        final Writer writer = new OutputStreamWriter(outputStream, UTF_8);
        return new BoxMetadataJsonArrayWriter(writer);
    }

    void write(final Map<String, Object> templateFields) throws IOException {
        if (closed) {
            throw new IOException("The Writer is closed");
        }

        if (!hasBegun) {
            beginArray();
            hasBegun = true;
        }

        if (hasEntries) {
            writer.write(',');
        }

        final JsonObject json = toRecord(templateFields);
        json.writeTo(writer);

        hasEntries = true;
    }

    private JsonObject toRecord(final Map<String, Object> templateFields) {
        final JsonObject json = Json.object();

        for (Map.Entry<String, Object> entry : templateFields.entrySet()) {
            Object value = entry.getValue();
            if (value == null) {
                json.add(entry.getKey(), Json.NULL);
            } else {
                json.add(entry.getKey(), Json.value(value.toString()));
            }
        }

        return json;
    }

    private void beginArray() throws IOException {
        writer.write('[');
    }

    private void endArray() throws IOException {
        writer.write(']');
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;

        if (!hasBegun) {
            beginArray();
        }
        endArray();

        writer.close();
    }
}
