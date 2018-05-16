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

package org.apache.nifi.serialization;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

public abstract class AbstractRecordSetWriter implements RecordSetWriter {
    private final OutputStream out;
    private int recordCount = 0;
    private boolean activeRecordSet = false;

    public AbstractRecordSetWriter(final OutputStream out) {
        this.out = out;
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public WriteResult write(final RecordSet recordSet) throws IOException {
        beginRecordSet();
        Record record;
        while ((record = recordSet.next()) != null) {
            write(record);
        }
        return finishRecordSet();
    }

    @Override
    public final WriteResult write(final Record record) throws IOException {
        final Map<String, String> attributes = writeRecord(record);
        return WriteResult.of(++recordCount, attributes);
    }

    protected OutputStream getOutputStream() {
        return out;
    }

    protected final int getRecordCount() {
        return recordCount;
    }

    protected final boolean isActiveRecordSet() {
        return activeRecordSet;
    }

    @Override
    public final void beginRecordSet() throws IOException {
        if (activeRecordSet) {
            throw new IllegalStateException("Cannot begin a RecordSet because a RecordSet has already begun");
        }

        activeRecordSet = true;
        onBeginRecordSet();
    }

    @Override
    public final WriteResult finishRecordSet() throws IOException {
        if (!isActiveRecordSet()) {
            throw new IllegalStateException("Cannot finish RecordSet because no RecordSet has begun");
        }

        final Map<String, String> attributes = onFinishRecordSet();
        return WriteResult.of(recordCount, attributes == null ? Collections.emptyMap() : attributes);
    }

    protected int incrementRecordCount() {
        return ++recordCount;
    }

    /**
     * Method that is called as a result of {@link #beginRecordSet()} being called. This gives subclasses
     * the chance to react to a new RecordSet beginning but prevents the subclass from changing how this
     * implementation maintains its internal state. By default, this method does nothing.
     *
     * @throws IOException if unable to write the necessary data for a new RecordSet
     */
    protected void onBeginRecordSet() throws IOException {
    }

    /**
     * Method that is called by {@link #finishRecordSet()} when a RecordSet is finished. This gives subclasses
     * the chance to react to a RecordSet being completed but prevents the subclass from changing how this
     * implementation maintains its internal state.
     *
     * @return a Map of key/value pairs that should be added to the FlowFile as attributes
     */
    protected Map<String, String> onFinishRecordSet() throws IOException {
        return Collections.emptyMap();
    }

    protected abstract Map<String, String> writeRecord(Record record) throws IOException;
}
