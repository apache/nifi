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
 */package org.apache.nifi.processors.standard.db.event.io;

import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.standard.db.event.EventInfo;

/**
 * An interface to write an event to the process session. Note that a single event may produce multiple flow files.
 */
public interface EventWriter<T extends EventInfo> {

    String APPLICATION_JSON = "application/json";
    String SEQUENCE_ID_KEY = "cdc.sequence.id";
    String CDC_EVENT_TYPE_ATTRIBUTE = "cdc.event.type";

    long writeEvent(final ProcessSession session, final T eventInfo, final long currentSequenceId);
}
