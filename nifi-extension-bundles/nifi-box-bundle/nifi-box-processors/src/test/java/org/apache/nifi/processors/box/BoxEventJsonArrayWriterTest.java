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
import com.box.sdkgen.schemas.event.EventEventTypeField;
import com.box.sdkgen.schemas.eventsource.EventSource;
import com.box.sdkgen.schemas.eventsource.EventSourceItemTypeField;
import com.box.sdkgen.schemas.eventsourceresource.EventSourceResource;
import com.box.sdkgen.schemas.file.File;
import com.box.sdkgen.schemas.folder.Folder;
import com.box.sdkgen.schemas.foldermini.FolderMini;
import com.box.sdkgen.schemas.user.User;
import com.box.sdkgen.schemas.usermini.UserMini;
import com.box.sdkgen.serialization.json.EnumWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BoxEventJsonArrayWriterTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private ByteArrayOutputStream out;
    private BoxEventJsonArrayWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        out = new ByteArrayOutputStream();
        writer = BoxEventJsonArrayWriter.create(out);
    }

    @Test
    void writeNoEvents() throws IOException {
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        assertTrue(actual.isArray());
        assertEquals(0, actual.size());
    }

    @Test
    void writeSingleEvent() throws IOException {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn("1");
        when(event.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.ITEM_CREATE));
        when(event.getCreatedAt()).thenReturn(null);
        when(event.getSessionId()).thenReturn(null);
        when(event.getCreatedBy()).thenReturn(null);
        when(event.getSource()).thenReturn(null);
        when(event.getAdditionalDetails()).thenReturn(null);

        writer.write(event);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        assertTrue(actual.isArray());
        assertEquals(1, actual.size());
        JsonNode eventNode = actual.get(0);
        assertEquals("1", eventNode.get("id").asText());
        assertEquals("ITEM_CREATE", eventNode.get("eventType").asText());
    }

    @Test
    void writeMultipleEvents() throws IOException {
        Event event1 = createMockEvent("1", EventEventTypeField.ITEM_CREATE);
        Event event2 = createMockEvent("2", EventEventTypeField.GROUP_ADD_USER);
        Event event3 = createMockEvent("3", EventEventTypeField.COLLABORATION_ACCEPT);

        writer.write(event1);
        writer.write(event2);
        writer.write(event3);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        assertTrue(actual.isArray());
        assertEquals(3, actual.size());

        assertEquals("1", actual.get(0).get("id").asText());
        assertEquals("ITEM_CREATE", actual.get(0).get("eventType").asText());

        assertEquals("2", actual.get(1).get("id").asText());
        assertEquals("GROUP_ADD_USER", actual.get(1).get("eventType").asText());

        assertEquals("3", actual.get(2).get("id").asText());
        assertEquals("COLLABORATION_ACCEPT", actual.get(2).get("eventType").asText());
    }

    @Test
    void writeEventWithCreatedBy() throws IOException {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn("1");
        when(event.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.ITEM_CREATE));

        UserMini createdBy = mock(UserMini.class);
        when(createdBy.getId()).thenReturn("user123");
        when(createdBy.getName()).thenReturn("Test User");
        when(createdBy.getLogin()).thenReturn("test@example.com");
        when(event.getCreatedBy()).thenReturn(createdBy);
        when(event.getSource()).thenReturn(null);
        when(event.getAdditionalDetails()).thenReturn(null);

        writer.write(event);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        JsonNode createdByNode = actual.get(0).get("createdBy");
        assertEquals("user123", createdByNode.get("id").asText());
        assertEquals("Test User", createdByNode.get("name").asText());
        assertEquals("test@example.com", createdByNode.get("login").asText());
    }

    @Test
    void writeEventWithTimestamps() throws IOException {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn("1");
        when(event.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.ITEM_CREATE));
        OffsetDateTime createdAt = OffsetDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC);
        when(event.getCreatedAt()).thenReturn(createdAt);
        when(event.getCreatedBy()).thenReturn(null);
        when(event.getSource()).thenReturn(null);
        when(event.getAdditionalDetails()).thenReturn(null);

        writer.write(event);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        assertEquals(createdAt.toString(), actual.get(0).get("createdAt").asText());
    }

    @Test
    void writeEventWithFileSource() throws IOException {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn("1");
        when(event.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.COLLABORATION_ACCEPT));
        when(event.getCreatedBy()).thenReturn(null);
        when(event.getAdditionalDetails()).thenReturn(null);

        // Create mock File source
        File mockFile = mock(File.class);
        when(mockFile.getId()).thenReturn("file123");
        when(mockFile.getName()).thenReturn("document.pdf");

        FolderMini mockParent = mock(FolderMini.class);
        when(mockParent.getId()).thenReturn("folder456");
        when(mockParent.getName()).thenReturn("My Folder");
        when(mockFile.getParent()).thenReturn(mockParent);

        EventSourceResource mockSourceResource = mock(EventSourceResource.class);
        when(mockSourceResource.isFile()).thenReturn(true);
        when(mockSourceResource.getFile()).thenReturn(mockFile);
        when(event.getSource()).thenReturn(mockSourceResource);

        writer.write(event);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        JsonNode sourceNode = actual.get(0).get("source");

        // Check generic fields
        assertEquals("file", sourceNode.get("item_type").asText());
        assertEquals("file123", sourceNode.get("item_id").asText());
        assertEquals("document.pdf", sourceNode.get("item_name").asText());

        // Check file-specific fields
        assertEquals("file123", sourceNode.get("file_id").asText());
        assertEquals("document.pdf", sourceNode.get("file_name").asText());

        // Check parent folder fields
        assertEquals("folder456", sourceNode.get("folder_id").asText());
        assertEquals("My Folder", sourceNode.get("folder_name").asText());
    }

    @Test
    void writeEventWithFolderSource() throws IOException {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn("1");
        when(event.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.COLLABORATION_ACCEPT));
        when(event.getCreatedBy()).thenReturn(null);
        when(event.getAdditionalDetails()).thenReturn(null);

        // Create mock Folder source
        Folder mockFolder = mock(Folder.class);
        when(mockFolder.getId()).thenReturn("folder789");
        when(mockFolder.getName()).thenReturn("Shared Folder");

        EventSourceResource mockSourceResource = mock(EventSourceResource.class);
        when(mockSourceResource.isFolder()).thenReturn(true);
        when(mockSourceResource.getFolder()).thenReturn(mockFolder);
        when(event.getSource()).thenReturn(mockSourceResource);

        writer.write(event);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        JsonNode sourceNode = actual.get(0).get("source");

        // Check generic fields
        assertEquals("folder", sourceNode.get("item_type").asText());
        assertEquals("folder789", sourceNode.get("item_id").asText());
        assertEquals("Shared Folder", sourceNode.get("item_name").asText());

        // Check folder-specific fields
        assertEquals("folder789", sourceNode.get("folder_id").asText());
        assertEquals("Shared Folder", sourceNode.get("folder_name").asText());
    }

    @Test
    void writeEventWithEventSource() throws IOException {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn("1");
        when(event.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.ITEM_CREATE));
        when(event.getCreatedBy()).thenReturn(null);
        when(event.getAdditionalDetails()).thenReturn(null);

        // Create mock EventSource
        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getItemId()).thenReturn("item123");
        when(mockEventSource.getItemName()).thenReturn("Test Item");
        when(mockEventSource.getItemType()).thenReturn(new EnumWrapper<>(EventSourceItemTypeField.FILE));
        when(mockEventSource.getParent()).thenReturn(null);

        EventSourceResource mockSourceResource = mock(EventSourceResource.class);
        when(mockSourceResource.isEventSource()).thenReturn(true);
        when(mockSourceResource.getEventSource()).thenReturn(mockEventSource);
        when(event.getSource()).thenReturn(mockSourceResource);

        writer.write(event);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        JsonNode sourceNode = actual.get(0).get("source");

        assertEquals("item123", sourceNode.get("item_id").asText());
        assertEquals("Test Item", sourceNode.get("item_name").asText());
        assertEquals("file", sourceNode.get("item_type").asText());
        assertEquals("item123", sourceNode.get("file_id").asText());
        assertEquals("Test Item", sourceNode.get("file_name").asText());
    }

    @Test
    void writeEventWithUserSource() throws IOException {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn("1");
        when(event.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.ITEM_CREATE));
        when(event.getCreatedBy()).thenReturn(null);
        when(event.getAdditionalDetails()).thenReturn(null);

        // Create mock User source
        User mockUser = mock(User.class);
        when(mockUser.getId()).thenReturn("user456");
        when(mockUser.getName()).thenReturn("Another User");
        when(mockUser.getLogin()).thenReturn("another@example.com");

        EventSourceResource mockSourceResource = mock(EventSourceResource.class);
        when(mockSourceResource.isUser()).thenReturn(true);
        when(mockSourceResource.getUser()).thenReturn(mockUser);
        when(event.getSource()).thenReturn(mockSourceResource);

        writer.write(event);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        JsonNode sourceNode = actual.get(0).get("source");

        assertEquals("user", sourceNode.get("item_type").asText());
        assertEquals("user456", sourceNode.get("id").asText());
        assertEquals("Another User", sourceNode.get("name").asText());
        assertEquals("another@example.com", sourceNode.get("login").asText());
    }

    @Test
    void writeEventWithAdditionalDetailsAsJsonObject() throws IOException {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn("1");
        when(event.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.GROUP_ADD_USER));
        when(event.getCreatedBy()).thenReturn(null);
        when(event.getSource()).thenReturn(null);

        // Create additionalDetails map with group_id
        Map<String, Object> additionalDetails = new HashMap<>();
        additionalDetails.put("group_id", "group123");
        additionalDetails.put("group_name", "Engineering Team");
        additionalDetails.put("member_count", 42);
        additionalDetails.put("is_active", true);
        when(event.getAdditionalDetails()).thenReturn(additionalDetails);

        writer.write(event);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        JsonNode additionalDetailsNode = actual.get(0).get("additionalDetails");

        // Verify additionalDetails is a proper JSON object, not a string
        assertTrue(additionalDetailsNode.isObject());
        assertEquals("group123", additionalDetailsNode.get("group_id").asText());
        assertEquals("Engineering Team", additionalDetailsNode.get("group_name").asText());
        assertEquals(42, additionalDetailsNode.get("member_count").asInt());
        assertTrue(additionalDetailsNode.get("is_active").asBoolean());
    }

    @Test
    void writeEventWithNestedAdditionalDetails() throws IOException {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn("1");
        when(event.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.COLLABORATION_ACCEPT));
        when(event.getCreatedBy()).thenReturn(null);
        when(event.getSource()).thenReturn(null);

        // Create additionalDetails map with nested structure
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("nested_key", "nested_value");

        Map<String, Object> additionalDetails = new HashMap<>();
        additionalDetails.put("group_id", "group456");
        additionalDetails.put("nested", nestedMap);
        when(event.getAdditionalDetails()).thenReturn(additionalDetails);

        writer.write(event);
        writer.close();

        JsonNode actual = OBJECT_MAPPER.readTree(out.toString());
        JsonNode additionalDetailsNode = actual.get(0).get("additionalDetails");

        assertTrue(additionalDetailsNode.isObject());
        assertEquals("group456", additionalDetailsNode.get("group_id").asText());
        assertTrue(additionalDetailsNode.get("nested").isObject());
        assertEquals("nested_value", additionalDetailsNode.get("nested").get("nested_key").asText());
    }

    private Event createMockEvent(String eventId, EventEventTypeField eventType) {
        Event event = mock(Event.class);
        lenient().when(event.getEventId()).thenReturn(eventId);
        lenient().when(event.getEventType()).thenReturn(new EnumWrapper<>(eventType));
        lenient().when(event.getCreatedAt()).thenReturn(null);
        lenient().when(event.getSessionId()).thenReturn(null);
        lenient().when(event.getCreatedBy()).thenReturn(null);
        lenient().when(event.getSource()).thenReturn(null);
        lenient().when(event.getAdditionalDetails()).thenReturn(null);
        return event;
    }
}
