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
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.CREATED_TIME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LAST_MODIFYING_USER;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LISTED_FOLDER_ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LISTED_FOLDER_NAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MODIFIED_TIME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.OWNER;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PARENT_FOLDER_ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PARENT_FOLDER_NAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PATH;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SHARED_DRIVE_ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SHARED_DRIVE_NAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_AVAILABLE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.WEB_CONTENT_LINK;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.WEB_VIEW_LINK;

public class GoogleDriveFileInfo implements ListableEntity {
    private  static final RecordSchema SCHEMA;

    static {
        final List<RecordField> recordFields = new ArrayList<>();

        recordFields.add(new RecordField(ID, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(FILENAME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(SIZE_AVAILABLE, RecordFieldType.BOOLEAN.getDataType(), false));
        recordFields.add(new RecordField(TIMESTAMP, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(CREATED_TIME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(MODIFIED_TIME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(MIME_TYPE, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(PATH, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(OWNER, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(LAST_MODIFYING_USER, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(WEB_VIEW_LINK, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(WEB_CONTENT_LINK, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(PARENT_FOLDER_ID, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(PARENT_FOLDER_NAME, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(LISTED_FOLDER_ID, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(LISTED_FOLDER_NAME, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(SHARED_DRIVE_ID, RecordFieldType.STRING.getDataType(), true));
        recordFields.add(new RecordField(SHARED_DRIVE_NAME, RecordFieldType.STRING.getDataType(), true));

        SCHEMA = new SimpleRecordSchema(recordFields);
    }

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

    private final String parentFolderId;
    private final String parentFolderName;
    private final String listedFolderId;
    private final String listedFolderName;
    private final String sharedDriveId;
    private final String sharedDriveName;

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

    public String getOwner() {
        return owner;
    }

    public String getLastModifyingUser() {
        return lastModifyingUser;
    }

    public String getWebViewLink() {
        return webViewLink;
    }

    public String getWebContentLink() {
        return webContentLink;
    }

    public String getParentFolderId() {
        return parentFolderId;
    }

    public String getParentFolderName() {
        return parentFolderName;
    }

    public String getListedFolderId() {
        return listedFolderId;
    }

    public String getListedFolderName() {
        return listedFolderName;
    }

    public String getSharedDriveId() {
        return sharedDriveId;
    }

    public String getSharedDriveName() {
        return sharedDriveName;
    }

    @Override
    public Record toRecord() {
        return new MapRecord(SCHEMA, toMap());
    }

    private Map<String, Object> toMap() {
        final Map<String, Object> values = new HashMap<>();

        values.put(ID, getId());
        values.put(FILENAME, getName());
        values.put(SIZE, getSize());
        values.put(SIZE_AVAILABLE, isSizeAvailable());
        values.put(CREATED_TIME, Instant.ofEpochMilli(getCreatedTime()).toString());
        values.put(MODIFIED_TIME, Instant.ofEpochMilli(getModifiedTime()).toString());
        values.put(TIMESTAMP, getTimestamp());
        values.put(MIME_TYPE, getMimeType());
        values.put(PATH, getPath());
        values.put(OWNER, getOwner());
        values.put(LAST_MODIFYING_USER, getLastModifyingUser());
        values.put(WEB_VIEW_LINK, getWebViewLink());
        values.put(WEB_CONTENT_LINK, getWebContentLink());
        values.put(PARENT_FOLDER_ID, getParentFolderId());
        values.put(PARENT_FOLDER_NAME, getParentFolderName());
        values.put(LISTED_FOLDER_ID, getListedFolderId());
        values.put(LISTED_FOLDER_NAME, getListedFolderName());
        values.put(SHARED_DRIVE_ID, getSharedDriveId());
        values.put(SHARED_DRIVE_NAME, getSharedDriveName());

        return values;
    }

    public Map<String, String> toAttributeMap() {
        return toMap().entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().toString()
                ));
    }

    public static RecordSchema getRecordSchema() {
        return SCHEMA;
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
        private String parentFolderId;
        private String parentFolderName;
        private String listedFolderId;
        private String listedFolderName;
        private String sharedDriveId;
        private String sharedDriveName;

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

        public Builder parentFolderId(String parentFolderId) {
            this.parentFolderId = parentFolderId;
            return this;
        }

        public Builder parentFolderName(String parentFolderName) {
            this.parentFolderName = parentFolderName;
            return this;
        }

        public Builder listedFolderId(String listedFolderId) {
            this.listedFolderId = listedFolderId;
            return this;
        }

        public Builder listedFolderName(String listedFolderName) {
            this.listedFolderName = listedFolderName;
            return this;
        }

        public Builder sharedDriveId(String sharedDriveId) {
            this.sharedDriveId = sharedDriveId;
            return this;
        }

        public Builder sharedDriveName(String sharedDriveName) {
            this.sharedDriveName = sharedDriveName;
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
        this.parentFolderId = builder.parentFolderId;
        this.parentFolderName = builder.parentFolderName;
        this.listedFolderId = builder.listedFolderId;
        this.listedFolderName = builder.listedFolderName;
        this.sharedDriveId = builder.sharedDriveId;
        this.sharedDriveName = builder.sharedDriveName;
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
