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
package org.apache.nifi.registry.event;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.hook.Event;
import org.apache.nifi.registry.hook.EventField;
import org.apache.nifi.registry.hook.EventFieldName;
import org.apache.nifi.registry.hook.EventType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Standard implementation of Event.
 */
public class StandardEvent implements Event {

    private final EventType eventType;

    private final List<EventField> eventFields;

    private StandardEvent(final Builder builder) {
        this.eventType = builder.eventType;
        this.eventFields = Collections.unmodifiableList(builder.eventFields == null
                ? Collections.emptyList() : new ArrayList<>(builder.eventFields));
        Validate.notNull(this.eventType);
    }

    @Override
    public EventType getEventType() {
        return eventType;
    }

    @Override
    public List<EventField> getFields() {
        return eventFields;
    }

    @Override
    public EventField getField(final EventFieldName fieldName) {
        if (fieldName == null) {
            return null;
        }

        return eventFields.stream().filter(e -> fieldName.equals(e.getName())).findFirst().orElse(null);
    }

    @Override
    public void validate() throws IllegalStateException {
        final int numProvidedFields = eventFields.size();
        final int numRequiredFields = eventType.getFieldNames().size();

        if (numProvidedFields != numRequiredFields) {
            throw new IllegalStateException(numRequiredFields + " fields were required, but only " + numProvidedFields + " were provided");
        }

        for (int i=0; i < numRequiredFields; i++) {
            final EventFieldName required = eventType.getFieldNames().get(i);
            final EventFieldName provided = eventFields.get(i).getName();
            if (!required.equals(provided)) {
                throw new IllegalStateException("Expected " + required.name() + ", but found " + provided.name());
            }
        }
    }

    /**
     * Builder for Events.
     */
    public static class Builder {

        private EventType eventType;
        private List<EventField> eventFields = new ArrayList<>();

        public Builder eventType(final EventType eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder addField(final EventFieldName name, final String value) {
            this.eventFields.add(new StandardEventField(name, value));
            return this;
        }

        public Builder addField(final EventField arg) {
            if (arg != null) {
                this.eventFields.add(arg);
            }
            return this;
        }

        public Builder addFields(final Collection<EventField> fields) {
            if (fields != null) {
                this.eventFields.addAll(fields);
            }
            return this;
        }

        public Builder clearFields() {
            this.eventFields.clear();
            return this;
        }

        public Event build() {
            return new StandardEvent(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StandardEvent that = (StandardEvent) o;
        return eventType == that.eventType && Objects.equals(eventFields, that.eventFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, eventFields);
    }

    @Override
    public String toString() {
        return "StandardEvent{eventType=" + eventType + ", eventFields=" + eventFields + '}';
    }
}
