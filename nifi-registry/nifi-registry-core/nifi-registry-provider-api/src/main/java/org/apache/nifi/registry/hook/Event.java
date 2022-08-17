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
package org.apache.nifi.registry.hook;

import java.util.List;

/**
 * An event that will be passed to EventHookProviders.
 */
public interface Event {

    /**
     * @return the type of the event
     */
    EventType getEventType();

    /**
     * @return the fields of the event in the order they were added to the event
     */
    List<EventField> getFields();

    /**
     * @param fieldName the name of the field to return
     * @return the EventField with the given name, or null if it does not exist
     */
    EventField getField(EventFieldName fieldName);

    /**
     * Will be called before publishing the event to ensure the event contains the required
     * fields for the given event type in the order specified by the type.
     *
     * @throws IllegalStateException if the event does not contain the required fields
     */
    void validate() throws IllegalStateException;

}
