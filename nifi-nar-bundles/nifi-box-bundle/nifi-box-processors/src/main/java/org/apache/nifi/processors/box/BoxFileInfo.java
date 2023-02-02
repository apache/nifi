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
package org.apache.nifi.processors.box;

import static org.apache.nifi.processors.box.BoxFileAttributes.ID;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BoxFileInfo implements ListableEntity {

    private  static final RecordSchema SCHEMA;

    static {
        final List<RecordField> recordFields = new ArrayList<>();

        recordFields.add(new RecordField(ID, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(CoreAttributes.FILENAME.key(), RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(CoreAttributes.PATH.key(), RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(TIMESTAMP, RecordFieldType.LONG.getDataType(), false));

        SCHEMA = new SimpleRecordSchema(recordFields);
    }

    private final String id;
    private final String fileName;
    private final String path;
    private final long size;
    private final long createdTime;
    private final long modifiedTime;

    public String getId() {
        return id;
    }

    public String getFileName() {
        return fileName;
    }

    public String getPath() {
        return path;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    @Override
    public Record toRecord() {
        final Map<String, Object> values = new HashMap<>();

        values.put(ID, getId());
        values.put(CoreAttributes.FILENAME.key(), getName());
        values.put(CoreAttributes.PATH.key(), getPath());
        values.put(SIZE, getSize());
        values.put(TIMESTAMP, getTimestamp());

        return new MapRecord(SCHEMA, values);
    }

    public static RecordSchema getRecordSchema() {
        return SCHEMA;
    }

    public static final class Builder {
        private String id;
        private String fileName;
        private String path;
        private long size;
        private long createdTime;
        private long modifiedTime;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder createdTime(long createdTime) {
            this.createdTime = createdTime;
            return this;
        }

        public Builder modifiedTime(long modifiedTime) {
            this.modifiedTime = modifiedTime;
            return this;
        }

        public BoxFileInfo build() {
            return new BoxFileInfo(this);
        }

    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BoxFileInfo other = (BoxFileInfo) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        return true;
    }

    private BoxFileInfo(final Builder builder) {
        this.id = builder.id;
        this.fileName = builder.fileName;
        this.path = builder.path;
        this.size = builder.size;
        this.createdTime = builder.createdTime;
        this.modifiedTime = builder.modifiedTime;
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
        long timestamp = Math.max(getCreatedTime(), getModifiedTime());

        return timestamp;
    }

    @Override
    public long getSize() {
        return size;
    }
}
