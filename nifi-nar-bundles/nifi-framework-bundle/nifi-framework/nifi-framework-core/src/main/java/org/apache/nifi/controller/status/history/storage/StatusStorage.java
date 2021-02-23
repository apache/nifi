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
package org.apache.nifi.controller.status.history.storage;

import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Represents a writable status storage for a given entry type.
 *
 * @param <T> The entry type.
 */
public interface StatusStorage<T> {
    /**
     * Date format expected by the storage.
     */
    String CAPTURE_DATE_FORMAT = "yyyy-MM-dd:HH:mm:ss Z";

    /**
     * Date formatter for the database fields.
     */
    DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(CAPTURE_DATE_FORMAT).withZone(ZoneId.systemDefault());

    /**
     * Stores multiple entries.
     *
     * @param statusEntries A list of pair constructs. Every pair consists of the capture time (first) and the status entry (second).
     */
    void store(List<Pair<Instant, T>> statusEntries);
}
