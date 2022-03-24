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

public class BlobInfo implements Comparable<BlobInfo>, Serializable, ListableEntity {
    private static final long serialVersionUID = 1L;

    private static final RecordSchema SCHEMA;
    private static final String BLOB_NAME = "blobName";
    private static final String BLOB_TYPE = "blobType";
    private static final String FILENAME = "filename";
    private static final String CONTAINER_NAME = "container";
    private static final String LENGTH = "length";
    private static final String LAST_MODIFIED = "lastModified";
    private static final String ETAG = "etag";
    private static final String CONTENT_LANGUAGE = "language";
    private static final String CONTENT_TYPE = "contentType";
    private static final String PRIMARY_URI = "primaryUri";
    private static final String SECONDARY_URI = "secondaryUri";

    static {
        final List<RecordField> recordFields = new ArrayList<>();
        recordFields.add(new RecordField(BLOB_NAME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(BLOB_TYPE, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(FILENAME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(CONTAINER_NAME, RecordFieldType.BOOLEAN.getDataType(), false));
        recordFields.add(new RecordField(LENGTH, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(LAST_MODIFIED, RecordFieldType.TIMESTAMP.getDataType(), false));
        recordFields.add(new RecordField(ETAG, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(CONTENT_LANGUAGE, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(CONTENT_TYPE, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(PRIMARY_URI, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(SECONDARY_URI, RecordFieldType.STRING.getDataType()));
        SCHEMA = new SimpleRecordSchema(recordFields);
    }


    private final String primaryUri;
    private final String secondaryUri;
    private final String contentType;
    private final String contentLanguage;
    private final String etag;
    private final long lastModifiedTime;
    private final long length;
    private final String blobType;
    private final String blobName;
    private final String containerName;

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String getPrimaryUri() {
        return primaryUri;
    }

    public String getSecondaryUri() {
        return secondaryUri;
    }

    public String getContentType() {
        return contentType;
    }

    public String getContentLanguage() {
        return contentLanguage;
    }

    public String getContainerName() {
        return containerName;
    }

    public String getBlobName() {
        return blobName;
    }

    public String getEtag() {
        return etag;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public long getLength() {
        return length;
    }

    public String getBlobType() {
        return blobType;
    }

    @Override
    public Record toRecord() {
        final Map<String, Object> values = new HashMap<>();
        values.put(PRIMARY_URI, getPrimaryUri());
        values.put(SECONDARY_URI, getSecondaryUri());
        values.put(CONTENT_TYPE, getContentType());
        values.put(CONTENT_LANGUAGE, getContentLanguage());
        values.put(CONTAINER_NAME, getContainerName());
        values.put(BLOB_NAME, getBlobName());
        values.put(FILENAME, getName());
        values.put(ETAG, getEtag());
        values.put(LAST_MODIFIED, getLastModifiedTime());
        values.put(LENGTH, getLength());
        values.put(BLOB_TYPE, getBlobType());
        return new MapRecord(SCHEMA, values);
    }

    public static RecordSchema getRecordSchema() {
        return SCHEMA;
    }

    public static final class Builder {
        private String primaryUri;
        private String secondaryUri;
        private String contentType;
        private String contentLanguage;
        private String etag;
        private long lastModifiedTime;
        private long length;
        private String blobType;
        private String containerName;
        private String blobName;

        public Builder primaryUri(String primaryUri) {
            this.primaryUri = primaryUri;
            return this;
        }

        public Builder secondaryUri(String secondaryUri) {
            this.secondaryUri = secondaryUri;
            return this;
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder contentLanguage(String contentLanguage) {
            this.contentLanguage = contentLanguage;
            return this;
        }

        public Builder containerName(String containerName) {
            this.containerName = containerName;
            return this;
        }

        public Builder etag(String etag) {
            this.etag = etag;
            return this;
        }

        public Builder lastModifiedTime(long lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
            return this;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder blobType(String blobType) {
            this.blobType = blobType;
            return this;
        }

        public Builder blobName(String blobName) {
            this.blobName = blobName;
            return this;
        }

        public BlobInfo build() {
            return new BlobInfo(this);
        }

    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((etag == null) ? 0 : etag.hashCode());
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
        BlobInfo other = (BlobInfo) obj;
        if (etag == null) {
            if (other.etag != null) {
                return false;
            }
        } else if (!etag.equals(other.etag)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(BlobInfo o) {
        return etag.compareTo(o.etag);
    }

    private BlobInfo(final Builder builder) {
        this.primaryUri = builder.primaryUri;
        this.secondaryUri = builder.secondaryUri;
        this.contentType = builder.contentType;
        this.contentLanguage = builder.contentLanguage;
        this.containerName = builder.containerName;
        this.etag = builder.etag;
        this.lastModifiedTime = builder.lastModifiedTime;
        this.length = builder.length;
        this.blobType = builder.blobType;
        this.blobName = builder.blobName;
    }

    @Override
    public String getName() {
        String primaryUri = getPrimaryUri();
        return primaryUri.substring(primaryUri.lastIndexOf('/') + 1);
    }

    @Override
    public String getIdentifier() {
        return getPrimaryUri();
    }

    @Override
    public long getTimestamp() {
        return getLastModifiedTime();
    }

    @Override
    public long getSize() {
        return length;
    }
}
