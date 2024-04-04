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

import org.apache.nifi.serialization.record.Record;

import java.util.Optional;
import java.util.function.Function;

public enum GoogleDriveFlowFileAttribute {
    ID(GoogleDriveAttributes.ID, GoogleDriveFileInfo::getId),
    FILENAME(GoogleDriveAttributes.FILENAME, GoogleDriveFileInfo::getName),
    SIZE(GoogleDriveAttributes.SIZE, fileInfo -> Optional.ofNullable(fileInfo.getSize())
            .map(String::valueOf)
            .orElse(null)
    ),
    TIMESTAMP(GoogleDriveAttributes.TIMESTAMP, fileInfo -> Optional.ofNullable(fileInfo.getTimestamp())
            .map(String::valueOf)
            .orElse(null)
    ),
    MIME_TYPE(GoogleDriveAttributes.MIME_TYPE, GoogleDriveFileInfo::getMimeType);

    private final String name;
    private final Function<GoogleDriveFileInfo, String> fromFileInfo;

    GoogleDriveFlowFileAttribute(String attributeName, Function<GoogleDriveFileInfo, String> fromFileInfo) {
        this.name = attributeName;
        this.fromFileInfo = fromFileInfo;
    }

    public String getName() {
        return name;
    }

    public String getValue(Record record) {
        return record.getAsString(name);
    }

    public String getValue(GoogleDriveFileInfo fileInfo) {
        return fromFileInfo.apply(fileInfo);
    }
}
