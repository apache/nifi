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
package org.apache.nifi.services.smb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

public class SmbListableEntity implements ListableEntity {

    private final String name;
    private final String shortName;
    private final String path;
    private final long timestamp;
    private final long creationTime;
    private final long lastAccessTime;
    private final long changeTime;
    private final boolean directory;
    private final long size;
    private final long allocationSize;

    private SmbListableEntity(String name, String shortName, String path, long timestamp, long creationTime,
            long lastAccessTime, long changeTime, boolean directory,
            long size, long allocationSize) {
        this.name = name;
        this.shortName = shortName;
        this.path = path;
        this.timestamp = timestamp;
        this.creationTime = creationTime;
        this.lastAccessTime = lastAccessTime;
        this.changeTime = changeTime;
        this.directory = directory;
        this.size = size;
        this.allocationSize = allocationSize;
    }

    public static SimpleRecordSchema getRecordSchema() {
        List<RecordField> fields = Arrays.asList(
                new RecordField("filename", RecordFieldType.STRING.getDataType(), false),
                new RecordField("shortName", RecordFieldType.STRING.getDataType(), false),
                new RecordField("path", RecordFieldType.STRING.getDataType(), false),
                new RecordField("identifier", RecordFieldType.STRING.getDataType(), false),
                new RecordField("timestamp", RecordFieldType.LONG.getDataType(), false),
                new RecordField("creationTime", RecordFieldType.LONG.getDataType(), false),
                new RecordField("lastAccessTime", RecordFieldType.LONG.getDataType(), false),
                new RecordField("changeTime", RecordFieldType.LONG.getDataType(), false),
                new RecordField("size", RecordFieldType.LONG.getDataType(), false),
                new RecordField("allocationSize", RecordFieldType.LONG.getDataType(), false)
        );
        return new SimpleRecordSchema(fields);
    }

    public static SmbListableEntityBuilder builder() {
        return new SmbListableEntityBuilder();
    }

    public String getShortName() {
        return shortName;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public long getChangeTime() {
        return changeTime;
    }

    public long getAllocationSize() {
        return allocationSize;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public String getPathWithName() {
        return path.isEmpty() ? name : path + "/" + name;
    }

    @Override
    public String getIdentifier() {
        return getPathWithName();
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public long getSize() {
        return size;
    }

    public boolean isDirectory() {
        return directory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SmbListableEntity that = (SmbListableEntity) o;
        return getPathWithName().equals(that.getPathWithName());
    }

    @Override
    public int hashCode() {
        return getPathWithName().hashCode();
    }

    @Override
    public String toString() {
        return getPathWithName() + " (last write: " + timestamp + " size: " + size + ")";
    }

    @Override
    public Record toRecord() {
        final Map<String, Object> record = new TreeMap<>();
        record.put("filename", getName());
        record.put("shortName", getShortName());
        record.put("path", path);
        record.put("identifier", getPathWithName());
        record.put("timestamp", getTimestamp());
        record.put("creationTime", getCreationTime());
        record.put("lastAccessTime", getLastAccessTime());
        record.put("size", getSize());
        record.put("allocationSize", getAllocationSize());
        return new MapRecord(getRecordSchema(), record);
    }

    public static class SmbListableEntityBuilder {

        private String name;
        private String shortName;
        private String path = "";
        private long timestamp;
        private long creationTime;
        private long lastAccessTime;
        private long changeTime;
        private boolean directory = false;
        private long size = 0;
        private long allocationSize = 0;

        public SmbListableEntityBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public SmbListableEntityBuilder setShortName(String shortName) {
            this.shortName = shortName;
            return this;
        }

        public SmbListableEntityBuilder setPath(String path) {
            this.path = path;
            return this;
        }

        public SmbListableEntityBuilder setTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public SmbListableEntityBuilder setCreationTime(long creationTime) {
            this.creationTime = creationTime;
            return this;
        }

        public SmbListableEntityBuilder setLastAccessTime(long lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
            return this;
        }

        public SmbListableEntityBuilder setChangeTime(long changeTime) {
            this.changeTime = changeTime;
            return this;
        }

        public SmbListableEntityBuilder setDirectory(boolean directory) {
            this.directory = directory;
            return this;
        }

        public SmbListableEntityBuilder setSize(long size) {
            this.size = size;
            return this;
        }

        public SmbListableEntityBuilder setAllocationSize(long allocationSize) {
            this.allocationSize = allocationSize;
            return this;
        }

        public SmbListableEntity build() {
            return new SmbListableEntity(name, shortName, path, timestamp, creationTime, lastAccessTime, changeTime,
                    directory, size, allocationSize);
        }
    }

}
