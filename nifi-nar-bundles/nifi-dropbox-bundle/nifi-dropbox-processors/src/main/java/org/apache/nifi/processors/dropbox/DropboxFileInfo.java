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
package org.apache.nifi.processors.dropbox;

import static org.apache.nifi.processors.dropbox.DropboxAttributes.FILENAME;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.ID;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.PATH;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.REVISION;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.SIZE;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.TIMESTAMP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class DropboxFileInfo implements ListableEntity {

    private static final RecordSchema SCHEMA;

    static {
        final List<RecordField> recordFields = new ArrayList<>();
        recordFields.add(new RecordField(ID, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(PATH, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(FILENAME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(TIMESTAMP, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(REVISION, RecordFieldType.STRING.getDataType(), false));

        SCHEMA = new SimpleRecordSchema(recordFields);
    }

    public static RecordSchema getRecordSchema() {
        return SCHEMA;
    }

    private final String id;
    private final String path;
    private final String filename;
    private final long size;
    private final long timestamp;
    private final String revision;

    private DropboxFileInfo(final Builder builder) {
        this.id = builder.id;
        this.path = builder.path;
        this.filename = builder.filename;
        this.size = builder.size;
        this.timestamp = builder.timestamp;
        this.revision = builder.revision;
    }

    public String getId() {
        return id;
    }

    public String getPath() {
        return path;
    }

    public String getRevision() {
        return revision;
    }

    public String getFileName() {
        return filename;
    }

    @Override
    public Record toRecord() {
        final Map<String, Object> values = new HashMap<>();

        values.put(ID, getId());
        values.put(PATH, getPath());
        values.put(FILENAME, getFileName());
        values.put(SIZE, getSize());
        values.put(TIMESTAMP, getTimestamp());
        values.put(REVISION, getRevision());

        return new MapRecord(SCHEMA, values);
    }

    @Override
    public String getName() {
        return getFileName();
    }

    @Override
    public String getIdentifier() {
        return getId();
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DropboxFileInfo that = (DropboxFileInfo) o;
        return id.equals(that.id) && size == that.size && timestamp == that.timestamp && path.equals(that.path) && filename.equals(that.filename)
                && revision.equals(that.revision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, path, filename, size, timestamp, revision);
    }

    public static final class Builder {

        private String id;
        private String path;
        private String filename;
        private long size;
        private long timestamp;
        private String revision;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder name(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder timestamp(long createdTime) {
            this.timestamp = createdTime;
            return this;
        }

        public Builder revision(String revision) {
            this.revision = revision;
            return this;
        }

        public DropboxFileInfo build() {
            return new DropboxFileInfo(this);
        }
    }
}
