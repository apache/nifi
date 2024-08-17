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
package org.apache.nifi.questdb.embedded;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import org.apache.nifi.questdb.QueryRowContext;

final class CursorBasedQueryRowContext implements QueryRowContext {
    private final RecordCursor cursor;
    private Record actualRecord;

    CursorBasedQueryRowContext(final RecordCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public int getInt(final int position) {
        return actualRecord.getInt(position);
    }

    @Override
    public long getLong(final int position) {
        return actualRecord.getLong(position);
    }

    @Override
    public long getTimestamp(final int position) {
        return actualRecord.getTimestamp(position);
    }

    @Override
    public short getShort(final int position) {
        return actualRecord.getShort(position);
    }

    @Override
    public String getString(final int position) {
        return String.valueOf(actualRecord.getSymA(position));
    }

    boolean hasNext() {
        return cursor.hasNext();
    }

    void moveToNext() {
        actualRecord = cursor.getRecord();
    }
}
