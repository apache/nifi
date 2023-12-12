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
package org.apache.nifi.questdb;

import java.time.Instant;

/**
 * Used by {@code InsertRowDataSource} in order to populate the rows to insert. Calling {@code #initializeRow} before adding
 * fields is mandatory. Every call initializes a new row thus it is not possible to effect the row data prior to the call.
 */
public interface InsertRowContext {
    InsertRowContext initializeRow(Instant captured);

    InsertRowContext addLong(int position, long value);
    InsertRowContext addInt(int position, int value);
    InsertRowContext addShort(int position, short value);
    InsertRowContext addString(int position, String value);
    InsertRowContext addInstant(int position, Instant value);
    InsertRowContext addTimestamp(int position, long value);
}
