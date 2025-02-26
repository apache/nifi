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

import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.time.Instant;
import java.util.function.Function;

public enum GoogleDriveFlowFileAttribute {
    ID(GoogleDriveAttributes.ID, GoogleDriveFileInfo::getId, RecordFieldType.STRING, false),
    FILENAME(GoogleDriveAttributes.FILENAME, GoogleDriveFileInfo::getName, RecordFieldType.STRING, false),
    SIZE(GoogleDriveAttributes.SIZE, GoogleDriveFileInfo::getSize, RecordFieldType.LONG, false),
    SIZE_AVAILABLE(GoogleDriveAttributes.SIZE_AVAILABLE, GoogleDriveFileInfo::isSizeAvailable, RecordFieldType.BOOLEAN, false),
    TIMESTAMP(GoogleDriveAttributes.TIMESTAMP, GoogleDriveFileInfo::getTimestamp, RecordFieldType.LONG, false),
    CREATED_TIME(GoogleDriveAttributes.CREATED_TIME, fileInfo -> Instant.ofEpochMilli(fileInfo.getCreatedTime()).toString(), RecordFieldType.STRING, false),
    MODIFIED_TIME(GoogleDriveAttributes.MODIFIED_TIME, fileInfo -> Instant.ofEpochMilli(fileInfo.getModifiedTime()).toString(), RecordFieldType.STRING, false),
    MIME_TYPE(GoogleDriveAttributes.MIME_TYPE, GoogleDriveFileInfo::getMimeType, RecordFieldType.STRING, true),
    PATH(GoogleDriveAttributes.PATH, GoogleDriveFileInfo::getPath, RecordFieldType.STRING, true),
    OWNER(GoogleDriveAttributes.OWNER, GoogleDriveFileInfo::getOwner, RecordFieldType.STRING, true),
    LAST_MODIFYING_USER(GoogleDriveAttributes.LAST_MODIFYING_USER, GoogleDriveFileInfo::getLastModifyingUser, RecordFieldType.STRING, true),
    WEB_VIEW_LINK(GoogleDriveAttributes.WEB_VIEW_LINK, GoogleDriveFileInfo::getWebViewLink, RecordFieldType.STRING, true),
    WEB_CONTENT_LINK(GoogleDriveAttributes.WEB_CONTENT_LINK, GoogleDriveFileInfo::getWebContentLink, RecordFieldType.STRING, true);

    private final String name;
    private final Function<GoogleDriveFileInfo, Object> fromFileInfo;
    private final RecordField recordField;

    GoogleDriveFlowFileAttribute(String name, Function<GoogleDriveFileInfo, Object> fromFileInfo, RecordFieldType recordFieldType, boolean recordFieldNullable) {
        this.name = name;
        this.fromFileInfo = fromFileInfo;
        this.recordField = new RecordField(name, recordFieldType.getDataType(), recordFieldNullable);
    }

    public String getName() {
        return name;
    }

    public Object getValue(GoogleDriveFileInfo fileInfo) {
        return fromFileInfo.apply(fileInfo);
    }

    public RecordField getRecordField() {
        return recordField;
    }

    public static GoogleDriveFlowFileAttribute getByName(final String name) {
        for (GoogleDriveFlowFileAttribute item : values()) {
            if (item.getName().equals(name)) {
                return item;
            }
        }
        throw new IllegalArgumentException("GoogleDriveFlowFileAttribute with name [" + name + "] does not exist");
    }

    public static boolean isValidName(final String name) {
        try {
            getByName(name);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
