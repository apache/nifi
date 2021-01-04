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
package org.apache.nifi.processors.standard.util;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class FileInfo implements Comparable<FileInfo>, Serializable, ListableEntity {
    private static final long serialVersionUID = 1L;

    private static final RecordSchema SCHEMA;
    private static final String FILENAME = "filename";
    private static final String PATH = "path";
    private static final String DIRECTORY = "directory";
    private static final String SIZE = "size";
    private static final String LAST_MODIFIED = "lastModified";
    private static final String PERMISSIONS = "permissions";
    private static final String OWNER = "owner";
    private static final String GROUP = "group";

    private static final char[] PERMISSION_MODIFIER_CHARS = "xwrxwrxwr".toCharArray();

    static {
        final List<RecordField> recordFields = new ArrayList<>();
        recordFields.add(new RecordField(FILENAME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(PATH, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(DIRECTORY, RecordFieldType.BOOLEAN.getDataType(), false));
        recordFields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(LAST_MODIFIED, RecordFieldType.TIMESTAMP.getDataType(), false));
        recordFields.add(new RecordField(PERMISSIONS, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(OWNER, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(GROUP, RecordFieldType.STRING.getDataType()));
        SCHEMA = new SimpleRecordSchema(recordFields);
    }

    private final boolean directory;
    private final long size;
    private final long lastModifiedTime;
    private final String fileName;
    private final String fullPathFileName;
    private final String permissions;
    private final String owner;
    private final String group;

    public String getFileName() {
        return fileName;
    }

    public String getFullPathFileName() {
        return fullPathFileName;
    }

    public boolean isDirectory() {
        return directory;
    }

    public long getSize() {
        return size;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public String getPermissions() {
        return permissions;
    }

    public String getOwner() {
        return owner;
    }

    public String getGroup() {
        return group;
    }

    public Record toRecord() {
        final Map<String, Object> values = new HashMap<>(8);
        values.put(FILENAME, getFileName());
        values.put(PATH, new File(getFullPathFileName()).getParent());
        values.put(DIRECTORY, isDirectory());
        values.put(SIZE, getSize());
        values.put(LAST_MODIFIED, getLastModifiedTime());
        values.put(PERMISSIONS, getPermissions());
        values.put(OWNER, getOwner());
        values.put(GROUP, getGroup());
        return new MapRecord(SCHEMA, values);
    }

    public static RecordSchema getRecordSchema() {
        return SCHEMA;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fullPathFileName == null) ? 0 : fullPathFileName.hashCode());
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
        FileInfo other = (FileInfo) obj;
        if (fullPathFileName == null) {
            if (other.fullPathFileName != null) {
                return false;
            }
        } else if (!fullPathFileName.equals(other.fullPathFileName)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(FileInfo o) {
        return fullPathFileName.compareTo(o.fullPathFileName);
    }

    protected FileInfo(final Builder builder) {
        this.directory = builder.directory;
        this.size = builder.size;
        this.lastModifiedTime = builder.lastModifiedTime;
        this.fileName = builder.fileName;
        this.fullPathFileName = builder.fullPathFileName;
        this.permissions = builder.permissions;
        this.owner = builder.owner;
        this.group = builder.group;
    }

    public static final class Builder {

        private boolean directory;
        private long size;
        private long lastModifiedTime;
        private String fileName;
        private String fullPathFileName;
        private String permissions;
        private String owner;
        private String group;

        public FileInfo build() {
            return new FileInfo(this);
        }

        public Builder directory(boolean directory) {
            this.directory = directory;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder lastModifiedTime(long lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
            return this;
        }

        public Builder filename(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder fullPathFileName(String pathFileName) {
            this.fullPathFileName = pathFileName;
            return this;
        }

        public Builder permissions(String permissions) {
            this.permissions = permissions;
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }
    }

    public static String permissionToString(int fileModeOctal) {
        StringBuilder sb = new StringBuilder();
        for (char p : PERMISSION_MODIFIER_CHARS) {
            sb.append((fileModeOctal & 1) == 1 ? p : '-');
            fileModeOctal >>= 1;
        }
        return sb.reverse().toString();
    }

    @Override
    public String getName() {
        return getFileName();
    }

    @Override
    public String getIdentifier() {
        final String fullPathName = getFullPathFileName();
        return fullPathName == null ? getName() : fullPathName;
    }

    @Override
    public long getTimestamp() {
        return getLastModifiedTime();
    }
}
