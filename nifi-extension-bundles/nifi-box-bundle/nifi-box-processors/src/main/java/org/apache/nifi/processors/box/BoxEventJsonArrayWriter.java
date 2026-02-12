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

import com.box.sdkgen.schemas.event.Event;
import com.box.sdkgen.schemas.eventsource.EventSource;
import com.box.sdkgen.schemas.eventsourceresource.EventSourceResource;
import com.box.sdkgen.schemas.file.File;
import com.box.sdkgen.schemas.folder.Folder;
import com.box.sdkgen.schemas.foldermini.FolderMini;
import com.box.sdkgen.schemas.user.User;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * A class responsible for writing {@link Event} objects into a JSON array.
 * Not thread-safe.
 */
final class BoxEventJsonArrayWriter implements Closeable {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final JsonGenerator generator;
    private boolean hasBegun;
    private boolean closed;

    private BoxEventJsonArrayWriter(final JsonGenerator generator) {
        this.generator = generator;
        this.hasBegun = false;
        this.closed = false;
    }

    static BoxEventJsonArrayWriter create(final OutputStream outputStream) throws IOException {
        final JsonGenerator generator = JSON_FACTORY.createGenerator(outputStream);
        return new BoxEventJsonArrayWriter(generator);
    }

    void write(final Event event) throws IOException {
        if (closed) {
            throw new IOException("The Writer is closed");
        }

        if (!hasBegun) {
            generator.writeStartArray();
            hasBegun = true;
        }

        writeEvent(event);
    }

    private void writeEvent(final Event event) throws IOException {
        generator.writeStartObject();

        // Map Event fields to JSON using camelCase to match the original NiFi Box processor format
        writeStringField("createdAt", event.getCreatedAt() != null ? event.getCreatedAt().toString() : null);
        writeStringField("recordedAt", event.getRecordedAt() != null ? event.getRecordedAt().toString() : null);
        writeStringField("eventType", event.getEventType() != null ? event.getEventType().getValue() : null);
        writeStringField("id", event.getEventId());
        writeStringField("sessionID", event.getSessionId());
        writeStringField("type", event.getType());

        // Handle createdBy if present (camelCase for field name, but inner fields match Box API)
        if (event.getCreatedBy() != null) {
            generator.writeObjectFieldStart("createdBy");
            writeStringField("id", event.getCreatedBy().getId());
            writeStringField("type", event.getCreatedBy().getType() != null ? event.getCreatedBy().getType().getValue() : null);
            writeStringField("name", event.getCreatedBy().getName());
            writeStringField("login", event.getCreatedBy().getLogin());
            generator.writeEndObject();
        } else {
            generator.writeNullField("createdBy");
        }

        // Handle source if present - use snake_case for inner fields to match Box API format
        writeSource(event.getSource());

        // Handle additionalDetails if present - serialize as proper JSON object
        writeAdditionalDetails(event.getAdditionalDetails());

        generator.writeEndObject();
    }

    private void writeSource(final EventSourceResource source) throws IOException {
        if (source == null) {
            generator.writeNullField("source");
            return;
        }

        generator.writeObjectFieldStart("source");
        try {
            // EventSourceResource is a union type (OneOfSix) - check what kind of source we have
            if (source.isFile()) {
                // File source - contains file_id, file_name, and parent folder info
                File file = source.getFile();
                writeStringField("item_type", "file");
                writeStringField("item_id", file.getId());
                writeStringField("item_name", file.getName());
                // Add file-specific fields for collaboration events
                writeStringField("file_id", file.getId());
                writeStringField("file_name", file.getName());
                // Add parent folder info if available
                FolderMini parent = file.getParent();
                if (parent != null) {
                    writeStringField("folder_id", parent.getId());
                    writeStringField("folder_name", parent.getName());
                }
            } else if (source.isFolder()) {
                // Folder source - contains folder_id, folder_name
                Folder folder = source.getFolder();
                writeStringField("item_type", "folder");
                writeStringField("item_id", folder.getId());
                writeStringField("item_name", folder.getName());
                // Add folder-specific fields for collaboration events
                writeStringField("folder_id", folder.getId());
                writeStringField("folder_name", folder.getName());
            } else if (source.isEventSource()) {
                // Generic EventSource - has item_type, item_id, item_name
                EventSource eventSource = source.getEventSource();
                String itemType = eventSource.getItemType() != null ? eventSource.getItemType().getValue() : null;
                writeStringField("item_type", itemType);
                writeStringField("item_id", eventSource.getItemId());
                writeStringField("item_name", eventSource.getItemName());
                // For EventSource, also populate file/folder specific fields based on item_type
                if ("file".equals(itemType)) {
                    writeStringField("file_id", eventSource.getItemId());
                    writeStringField("file_name", eventSource.getItemName());
                } else if ("folder".equals(itemType)) {
                    writeStringField("folder_id", eventSource.getItemId());
                    writeStringField("folder_name", eventSource.getItemName());
                }
                // Add parent folder info if available
                FolderMini parent = eventSource.getParent();
                if (parent != null) {
                    writeStringField("parent_id", parent.getId());
                    writeStringField("parent_name", parent.getName());
                }
            } else if (source.isUser()) {
                // User source
                User user = source.getUser();
                writeStringField("item_type", "user");
                writeStringField("id", user.getId());
                writeStringField("name", user.getName());
                writeStringField("login", user.getLogin());
            } else if (source.isMap()) {
                // Generic map - write all entries
                Map<String, Object> map = source.getMap();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    Object value = entry.getValue();
                    if (value != null) {
                        generator.writeFieldName(entry.getKey());
                        generator.writeObject(value);
                    }
                }
            } else if (source.isAppItemEventSource()) {
                // AppItemEventSource
                writeStringField("item_type", "app_item");
            } else {
                writeStringField("item_type", "unknown");
            }
        } catch (Exception e) {
            writeStringField("error", "Could not serialize source: " + e.getMessage());
        }
        generator.writeEndObject();
    }

    private void writeAdditionalDetails(final Map<String, Object> additionalDetails) throws IOException {
        if (additionalDetails == null) {
            generator.writeNullField("additionalDetails");
            return;
        }

        try {
            // Write additionalDetails as a proper JSON object, not a string
            generator.writeFieldName("additionalDetails");
            generator.writeStartObject();
            for (Map.Entry<String, Object> entry : additionalDetails.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value == null) {
                    generator.writeNullField(key);
                } else if (value instanceof String strValue) {
                    generator.writeStringField(key, strValue);
                } else if (value instanceof Number numValue) {
                    generator.writeNumberField(key, numValue.doubleValue());
                } else if (value instanceof Boolean boolValue) {
                    generator.writeBooleanField(key, boolValue);
                } else if (value instanceof Map<?, ?> mapValue) {
                    // Nested map - use ObjectMapper to serialize
                    generator.writeFieldName(key);
                    generator.writeRawValue(OBJECT_MAPPER.writeValueAsString(mapValue));
                } else {
                    // For other types, convert to string
                    generator.writeStringField(key, value.toString());
                }
            }
            generator.writeEndObject();
        } catch (Exception e) {
            generator.writeNullField("additionalDetails");
        }
    }

    private void writeStringField(final String fieldName, final String value) throws IOException {
        if (value != null) {
            generator.writeStringField(fieldName, value);
        } else {
            generator.writeNullField(fieldName);
        }
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
