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
import com.eclipsesource.json.JsonValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BoxEventJsonArrayWriterTest {

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

        assertEquals(Json.array(), actualJson());
    }

    @Test
    void writeSingleEvent() throws IOException {
        final BoxEvent event = new BoxEvent(null, """
                {
                    "event_id": "1",
                    "event_type": "ITEM_CREATE"
                }
                """);

        writer.write(event);
        writer.close();

        final JsonValue expected = Json.array().add(
                createEventJson()
                        .set("id", event.getID())
                        .set("eventType", event.getEventType().name())
                        .set("typeName", event.getTypeName())
        );

        assertEquals(expected, actualJson());
    }

    @Test
    void writeMultipleEvents() throws IOException {
        final BoxEvent event1 = new BoxEvent(null, """
                {
                    "event_id": "1",
                    "event_type": "ITEM_CREATE",
                    "source": {
                        "item_type": "file",
                        "item_id": "123"
                    }
                }
                """);
        final BoxEvent event2 = new BoxEvent(null, """
                {
                    "event_id": "2",
                    "event_type": "GROUP_ADD_USER",
                    "source": {
                        "type": "group",
                        "id": "123"
                    }
                }
                """);
        final BoxEvent event3 = new BoxEvent(null, """
                {
                    "event_id": "3",
                    "event_type": "COLLABORATION_ACCEPT",
                    "source": {
                        "item_type": "file",
                        "item_id": "123"
                    }
                }
                """);

        writer.write(event1);
        writer.write(event2);
        writer.write(event3);
        writer.close();

        final JsonValue expected = Json.array()
                .add(createEventJson()
                        .set("id", event1.getID())
                        .set("eventType", event1.getEventType().name())
                        .set("typeName", event1.getTypeName())
                        .set("source", Json.object()
                                .add("item_type", event1.getSourceJSON().get("item_type"))
                                .add("item_id", event1.getSourceJSON().get("item_id"))
                        )
                )
                .add(createEventJson()
                        .set("id", event2.getID())
                        .set("eventType", event2.getEventType().name())
                        .set("typeName", event2.getTypeName())
                        .set("source", Json.object()
                                .add("type", event2.getSourceJSON().get("type"))
                                .add("id", event2.getSourceInfo().getID())
                        )
                )
                .add(createEventJson()
                        .set("id", event3.getID())
                        .set("eventType", event3.getEventType().name())
                        .set("typeName", event3.getTypeName())
                        .set("source", Json.object()
                                .add("item_type", event3.getSourceJSON().get("item_type"))
                                .add("item_id", event3.getSourceJSON().get("item_id"))
                        )
                );

        assertEquals(expected, actualJson());
    }

    private JsonValue actualJson() {
        return Json.parse(out.toString());
    }

    private JsonObject createEventJson() {
        // The Writer explicitly writes nulls as JSON null values.
        return Json.object()
                .add("accessibleBy", Json.NULL)
                .add("actionBy", Json.NULL)
                .add("additionalDetails", Json.NULL)
                .add("createdAt", Json.NULL)
                .add("createdBy", Json.NULL)
                .add("eventType", Json.NULL)
                .add("id", Json.NULL)
                .add("ipAddress", Json.NULL)
                .add("sessionID", Json.NULL)
                .add("source", Json.NULL)
                .add("typeName", Json.NULL);
    }
}
