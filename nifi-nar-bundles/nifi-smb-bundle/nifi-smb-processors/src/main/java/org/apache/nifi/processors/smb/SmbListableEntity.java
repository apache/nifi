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
package org.apache.nifi.processors.smb;

import static org.apache.nifi.serialization.record.RecordFieldType.LONG;
import static org.apache.nifi.serialization.record.RecordFieldType.STRING;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;

public class SmbListableEntity implements ListableEntity {

    private final String name;
    private final String path;
    private final long timestamp;
    private final boolean directory;
    private final long size;

    public SmbListableEntity(String name, String path, long timestamp, boolean directory, long size) {
        this.name = name;
        this.path = path;
        this.timestamp = timestamp;
        this.directory = directory;
        this.size = size;
    }

    public SmbListableEntity(String fileName, long timeStamp, long size) {
        this.name = fileName;
        this.path = "";
        this.timestamp = timeStamp;
        this.directory = false;
        this.size = size;
    }

    public static SimpleRecordSchema getRecordSchema() {
        List<RecordField> fields = Arrays.asList(
                new RecordField("name", STRING.getDataType(), false),
                new RecordField("path", STRING.getDataType(), false),
                new RecordField("identifier", STRING.getDataType(), false),
                new RecordField("timeStamp", LONG.getDataType(), false),
                new RecordField("size", LONG.getDataType(), false)
        );
        return new SimpleRecordSchema(fields);
    }

    public static SmbListableEntityBuilder builder() {
        return new SmbListableEntityBuilder();
    }

    @Override
    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public String getPathWithName() {
        return path.isEmpty() ? name : path + "\\" + name;
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
        record.put("name", name);
        record.put("path", path);
        record.put("identifier", getPathWithName());
        record.put("timestamp", getTimestamp());
        record.put("size", size);
        return new MapRecord(getRecordSchema(), record);
    }

    public static class SmbListableEntityBuilder {

        private String name;
        private String path;
        private long timestamp;
        private boolean directory;
        private long size;

        public SmbListableEntityBuilder setName(String name) {
            this.name = name;
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

        public SmbListableEntityBuilder setDirectory(boolean directory) {
            this.directory = directory;
            return this;
        }

        public SmbListableEntityBuilder setSize(long size) {
            this.size = size;
            return this;
        }

        public SmbListableEntity build() {
            return new SmbListableEntity(name, path, timestamp, directory, size);
        }
    }

}
