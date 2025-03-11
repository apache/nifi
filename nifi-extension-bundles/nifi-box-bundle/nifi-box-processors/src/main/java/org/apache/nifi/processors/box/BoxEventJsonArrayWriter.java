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

import com.box.sdk.BoxEvent;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A class responsible for writing {@link BoxEvent} objects into a JSON array.
 * Not thread-safe.
 */
final class BoxEventJsonArrayWriter implements Closeable {

    private final Writer writer;
    private boolean hasBegun;
    private boolean hasEntries;
    private boolean closed;

    private BoxEventJsonArrayWriter(final Writer writer) {
        this.writer = writer;
        this.hasBegun = false;
        this.hasEntries = false;
        this.closed = false;
    }

    static BoxEventJsonArrayWriter create(final OutputStream outputStream) throws IOException {
        final Writer writer = new OutputStreamWriter(outputStream, UTF_8);
        return new BoxEventJsonArrayWriter(writer);
    }

    void write(final BoxEvent event) throws IOException {
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

        final JsonObject json = toRecord(event);
        json.writeTo(writer);

        hasEntries = true;
    }

    private JsonObject toRecord(final BoxEvent event) {
        final JsonObject json = Json.object();

        json.add("accessibleBy", event.getAccessibleBy() == null ? Json.NULL : Json.parse(event.getAccessibleBy().getJson()));
        json.add("actionBy", event.getActionBy() == null ? Json.NULL : Json.parse(event.getActionBy().getJson()));
        json.add("additionalDetails", Objects.requireNonNullElse(event.getAdditionalDetails(), Json.NULL));
        json.add("createdAt", event.getCreatedAt() == null ? Json.NULL : Json.value(event.getCreatedAt().toString()));
        json.add("createdBy", event.getCreatedBy() == null ? Json.NULL : Json.parse(event.getCreatedBy().getJson()));
        json.add("eventType", event.getEventType() == null ? Json.NULL : Json.value(event.getEventType().name()));
        json.add("id", Objects.requireNonNullElse(Json.value(event.getID()), Json.NULL));
        json.add("ipAddress", Objects.requireNonNullElse(Json.value(event.getIPAddress()), Json.NULL));
        json.add("sessionID", Objects.requireNonNullElse(Json.value(event.getSessionID()), Json.NULL));
        json.add("source", Objects.requireNonNullElse(event.getSourceJSON(), Json.NULL));
        json.add("typeName", Objects.requireNonNullElse(Json.value(event.getTypeName()), Json.NULL));

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
