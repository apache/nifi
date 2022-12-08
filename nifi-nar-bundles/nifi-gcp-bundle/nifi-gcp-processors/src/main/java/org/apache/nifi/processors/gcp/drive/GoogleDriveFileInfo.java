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
package org.apache.nifi.processors.gcp.drive;

import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP;

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

public class GoogleDriveFileInfo implements ListableEntity {
    private  static final RecordSchema SCHEMA;

    static {
        final List<RecordField> recordFields = new ArrayList<>();

        recordFields.add(new RecordField(ID, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(FILENAME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(TIMESTAMP, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(MIME_TYPE, RecordFieldType.STRING.getDataType()));

        SCHEMA = new SimpleRecordSchema(recordFields);
    }

    private final String id;
    private final String fileName;
    private final long size;
    private final long createdTime;
    private final long modifiedTime;
    private final String mimeType;

    public String getId() {
        return id;
    }

    public String getFileName() {
        return fileName;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public String getMimeType() {
        return mimeType;
    }

    @Override
    public Record toRecord() {
        final Map<String, Object> values = new HashMap<>();

        values.put(ID, getId());
        values.put(FILENAME, getName());
        values.put(SIZE, getSize());
        values.put(TIMESTAMP, getTimestamp());
        values.put(MIME_TYPE, getMimeType());

        return new MapRecord(SCHEMA, values);
    }

    public static RecordSchema getRecordSchema() {
        return SCHEMA;
    }

    public static final class Builder {
        private String id;
        private String fileName;
        private long size;
        private long createdTime;
        private long modifiedTime;
        private String mimeType;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder fileName(String fileName) {
            this.fileName = fileName;
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

        public Builder mimeType(String mimeType) {
            this.mimeType = mimeType;
            return this;
        }

        public GoogleDriveFileInfo build() {
            return new GoogleDriveFileInfo(this);
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
        GoogleDriveFileInfo other = (GoogleDriveFileInfo) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        return true;
    }

    private GoogleDriveFileInfo(final Builder builder) {
        this.id = builder.id;
        this.fileName = builder.fileName;
        this.size = builder.size;
        this.createdTime = builder.createdTime;
        this.modifiedTime = builder.modifiedTime;
        this.mimeType = builder.mimeType;
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
