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

import org.apache.nifi.registry.provider.Provider;

/**
 * An extension point that will be passed events produced by actions take in the registry.
 *
 * The list of event types can be found in {@link org.apache.nifi.registry.hook.EventType}.
 *
 * NOTE: Although this interface is intended to be an extension point, it is not yet considered stable and thus may
 * change across releases until the registry matures.
 */
public interface EventHookProvider extends Provider {

    /**
     * Handles the given event.
     *
     * @param event the event to handle
     * @throws EventHookException if an error occurs handling the event
     */
    void handle(Event event) throws EventHookException;

    /**
     * Examines the values from the 'Whitelisted Event Type ' properties in the hook provider definition to determine
     * if the Event should be invoked for this particular EventType
     *
     * @param eventType
     *  EventType that has been fired by the framework.
     *
     * @return
     *  True if the hook provider should be 'handled' and false otherwise.
     */
    default boolean shouldHandle(EventType eventType) {
        return true;
    }

}
