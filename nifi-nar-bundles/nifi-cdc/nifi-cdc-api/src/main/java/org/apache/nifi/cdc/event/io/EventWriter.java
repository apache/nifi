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

import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.cdc.event.EventInfo;
import org.apache.nifi.processor.Relationship;

/**
 * An interface to write an event to the process session. Note that a single event may produce multiple flow files.
 */
public interface EventWriter<T extends EventInfo> {

    String APPLICATION_JSON = "application/json";
    String SEQUENCE_ID_KEY = "cdc.sequence.id";
    String CDC_EVENT_TYPE_ATTRIBUTE = "cdc.event.type";

    /**
     * Writes the given event to the process session, possibly via transferring it to the specified relationship (usually used for success)
     *
     * @param session           The session to write the event to
     * @param transitUri        The URI indicating the source MySQL system from which the specified event is associated
     * @param eventInfo         The event data
     * @param currentSequenceId the current sequence ID
     * @param relationship      A relationship to transfer any flowfile(s) to
     * @return a sequence ID, usually incremented from the specified current sequence id by the number of flow files transferred and/or committed
     */
    long writeEvent(final ProcessSession session, String transitUri, final T eventInfo, final long currentSequenceId, Relationship relationship);
}
