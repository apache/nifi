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
package org.apache.nifi.processors.azure.storage.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ADLSFileInfo implements Comparable<ADLSFileInfo>, Serializable, ListableEntity {

    private static final RecordSchema SCHEMA;
    private static final String FILESYSTEM = "filesystem";
    private static final String FILE_PATH = "filePath";
    private static final String DIRECTORY = "directory";
    private static final String FILENAME = "filename";
    private static final String LENGTH = "length";
    private static final String LAST_MODIFIED = "lastModified";
    private static final String ETAG = "etag";

    static {
        List<RecordField> recordFields = new ArrayList<>();
        recordFields.add(new RecordField(FILESYSTEM, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(FILE_PATH, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(DIRECTORY, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(FILENAME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(LENGTH, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(LAST_MODIFIED, RecordFieldType.TIMESTAMP.getDataType(), false));
        recordFields.add(new RecordField(ETAG, RecordFieldType.STRING.getDataType()));
        SCHEMA = new SimpleRecordSchema(recordFields);
    }

    private static final Comparator<ADLSFileInfo> COMPARATOR = Comparator.comparing(ADLSFileInfo::getFileSystem).thenComparing(ADLSFileInfo::getFilePath);

    private final String fileSystem;
    private final String filePath;
    private final long length;
    private final long lastModified;
    private final String etag;

    private ADLSFileInfo(Builder builder) {
        this.fileSystem = builder.fileSystem;
        this.filePath = builder.filePath;
        this.length = builder.length;
        this.lastModified = builder.lastModified;
        this.etag = builder.etag;
    }

    public String getFileSystem() {
        return fileSystem;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getLength() {
        return length;
    }

    public long getLastModified() {
        return lastModified;
    }

    public String getEtag() {
        return etag;
    }

    public String getDirectory() {
        return filePath.contains("/") ? StringUtils.substringBeforeLast(filePath, "/") : "";
    }

    public String getFilename() {
        return filePath.contains("/") ? StringUtils.substringAfterLast(filePath, "/") : filePath;
    }

    @Override
    public String getName() {
        return getFilePath();
    }

    @Override
    public String getIdentifier() {
        return getFilePath();
    }

    @Override
    public long getTimestamp() {
        return getLastModified();
    }

    @Override
    public long getSize() {
        return getLength();
    }

    public static RecordSchema getRecordSchema() {
        return SCHEMA;
    }

    @Override
    public Record toRecord() {
        Map<String, Object> values = new HashMap<>();
        values.put(FILESYSTEM, getFileSystem());
        values.put(FILE_PATH, getFilePath());
        values.put(DIRECTORY, getDirectory());
        values.put(FILENAME, getFilename());
        values.put(LENGTH, getLength());
        values.put(LAST_MODIFIED, getLastModified());
        values.put(ETAG, getEtag());
        return new MapRecord(SCHEMA, values);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ADLSFileInfo otherFileInfo = (ADLSFileInfo) other;
        return Objects.equals(fileSystem, otherFileInfo.fileSystem)
                && Objects.equals(filePath, otherFileInfo.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileSystem, filePath);
    }

    @Override
    public int compareTo(ADLSFileInfo other) {
        return COMPARATOR.compare(this, other);
    }

    public static class Builder {
        private String fileSystem;
        private String filePath;
        private long length;
        private long lastModified;
        private String etag;

        public Builder fileSystem(String fileSystem) {
            this.fileSystem = fileSystem;
            return this;
        }

        public Builder filePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder lastModified(long lastModified) {
            this.lastModified = lastModified;
            return this;
        }

        public Builder etag(String etag) {
            this.etag = etag;
            return this;
        }

        public ADLSFileInfo build() {
            return new ADLSFileInfo(this);
        }
    }
}
