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
package org.apache.nifi.cdc.event.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.nifi.cdc.event.EventInfo;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An abstract class that outputs common information (event type, timestamp, e.g.) about CDC events.
 */
public abstract class AbstractEventWriter<T extends EventInfo> implements EventWriter<T> {

    private final JsonFactory JSON_FACTORY = new JsonFactory();
    protected JsonGenerator jsonGenerator;

    // Common method to create a JSON generator and start the root object. Should be called by sub-classes unless they need their own generator and such.
    protected void startJson(OutputStream outputStream, T event) throws IOException {
        jsonGenerator = createJsonGenerator(outputStream);
        jsonGenerator.writeStartObject();
        String eventType = event.getEventType();
        if (eventType == null) {
            jsonGenerator.writeNullField("type");
        } else {
            jsonGenerator.writeStringField("type", eventType);
        }
        Long timestamp = event.getTimestamp();
        if (timestamp == null) {
            jsonGenerator.writeNullField("timestamp");
        } else {
            jsonGenerator.writeNumberField("timestamp", event.getTimestamp());
        }
    }

    protected void endJson() throws IOException {
        if (jsonGenerator == null) {
            throw new IOException("endJson called without a JsonGenerator");
        }
        jsonGenerator.writeEndObject();
        jsonGenerator.flush();
        jsonGenerator.close();
    }

    private JsonGenerator createJsonGenerator(OutputStream out) throws IOException {
        return JSON_FACTORY.createGenerator(out);
    }
}
