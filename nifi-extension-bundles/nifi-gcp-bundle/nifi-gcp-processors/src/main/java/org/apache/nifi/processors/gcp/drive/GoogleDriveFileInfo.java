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


import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class GoogleDriveFileInfo implements ListableEntity {

    private final String id;
    private final String fileName;
    private final long size;
    private final boolean sizeAvailable;
    private final long createdTime;
    private final long modifiedTime;
    private final String mimeType;

    private final String path;
    private final String owner;
    private final String lastModifyingUser;
    private final String webViewLink;
    private final String webContentLink;

    private final RecordSchema recordSchema;

    public String getId() {
        return id;
    }

    public String getFileName() {
        return fileName;
    }

    public boolean isSizeAvailable() {
        return sizeAvailable;
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

    public String getPath() {
        return path;
    }

    public String getLastModifyingUser() {
        return lastModifyingUser;
    }

    public String getOwner() {
        return owner;
    }

    public String getWebViewLink() {
        return webViewLink;
    }

    public String getWebContentLink() {
        return webContentLink;
    }

    @Override
    public Record toRecord() {
        return new MapRecord(recordSchema, toObjectMap());
    }

    private Map<String, Object> toObjectMap() {
        final Map<String, Object> attributes = new HashMap<>();

        for (GoogleDriveFlowFileAttribute attribute : GoogleDriveFlowFileAttribute.values()) {
            Optional.ofNullable(attribute.getValue(this))
                    .ifPresent(value -> attributes.put(attribute.getName(), value));
        }

        return attributes;
    }

    public Map<String, String> toStringMap() {
        return toObjectMap().entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().toString()
                ));
    }

    public static final class Builder {
        private String id;
        private String fileName;
        private long size;
        private boolean sizeAvailable;
        private long createdTime;
        private long modifiedTime;
        private String mimeType;
        private String path;
        private String owner;
        private String lastModifyingUser;
        private String webViewLink;
        private String webContentLink;
        private RecordSchema recordSchema;

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

        public Builder sizeAvailable(boolean sizeAvailable) {
            this.sizeAvailable = sizeAvailable;
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

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder lastModifyingUser(String lastModifyingUser) {
            this.lastModifyingUser = lastModifyingUser;
            return this;
        }

        public Builder webViewLink(String webViewLink) {
            this.webViewLink = webViewLink;
            return this;
        }

        public Builder webContentLink(String webContentLink) {
            this.webContentLink = webContentLink;
            return this;
        }

        public Builder recordSchema(RecordSchema recordSchema) {
            this.recordSchema = recordSchema;
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
        this.sizeAvailable = builder.sizeAvailable;
        this.createdTime = builder.createdTime;
        this.modifiedTime = builder.modifiedTime;
        this.mimeType = builder.mimeType;
        this.path = builder.path;
        this.owner = builder.owner;
        this.lastModifyingUser = builder.lastModifyingUser;
        this.webViewLink = builder.webViewLink;
        this.webContentLink = builder.webContentLink;
        this.recordSchema = builder.recordSchema;
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
