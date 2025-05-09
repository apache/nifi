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

import org.apache.nifi.flowfile.attributes.CoreAttributes;

public class GoogleDriveAttributes {

    public static final String ID = "drive.id";
    public static final String ID_DESC = "The id of the file";

    public static final String FILENAME = CoreAttributes.FILENAME.key();
    public static final String FILENAME_DESC = "The name of the file";

    public static final String SIZE = "drive.size";
    public static final String SIZE_DESC = "The size of the file. Set to 0 when the file size is not available (e.g. externally stored files).";

    public static final String SIZE_AVAILABLE = "drive.size.available";
    public static final String SIZE_AVAILABLE_DESC = "Indicates if the file size is known / available";

    public static final String TIMESTAMP = "drive.timestamp";
    public static final String TIMESTAMP_DESC =  "The last modified time or created time (whichever is greater) of the file." +
            " The reason for this is that the original modified date of a file is preserved when uploaded to Google Drive." +
            " 'Created time' takes the time when the upload occurs. However uploaded files can still be modified later.";

    public static final String CREATED_TIME = "drive.created.time";
    public static final String CREATED_TIME_DESC = "The file's creation time";

    public static final String MODIFIED_TIME = "drive.modified.time";
    public static final String MODIFIED_TIME_DESC = "The file's last modification time";

    public static final String MIME_TYPE = CoreAttributes.MIME_TYPE.key();
    public static final String MIME_TYPE_DESC =  "The MIME type of the file";

    public static final String PATH = "drive.path";
    public static final String PATH_DESC = "The path of the file's directory from the base directory. The path contains the folder names" +
            " in URL encoded form because Google Drive allows special characters in file names, including '/' (slash) and '\\' (backslash)." +
            " The URL encoded folder names are separated by '/' in the path.";

    public static final String OWNER = "drive.owner";
    public static final String OWNER_DESC = "The owner of the file";

    public static final String LAST_MODIFYING_USER = "drive.last.modifying.user";
    public static final String LAST_MODIFYING_USER_DESC = "The last modifying user of the file";

    public static final String WEB_VIEW_LINK = "drive.web.view.link";
    public static final String WEB_VIEW_LINK_DESC = "Web view link to the file";

    public static final String WEB_CONTENT_LINK = "drive.web.content.link";
    public static final String WEB_CONTENT_LINK_DESC = "Web content link to the file";

    public static final String PARENT_FOLDER_ID = "drive.parent.folder.id";
    public static final String PARENT_FOLDER_ID_DESC = "The id of the file's parent folder";

    public static final String PARENT_FOLDER_NAME = "drive.parent.folder.name";
    public static final String PARENT_FOLDER_NAME_DESC = "The name of the file's parent folder";

    public static final String LISTED_FOLDER_ID = "drive.listed.folder.id";
    public static final String LISTED_FOLDER_ID_DESC = "The id of the base folder that was listed";

    public static final String LISTED_FOLDER_NAME = "drive.listed.folder.name";
    public static final String LISTED_FOLDER_NAME_DESC = "The name of the base folder that was listed";

    public static final String SHARED_DRIVE_ID = "drive.shared.drive.id";
    public static final String SHARED_DRIVE_ID_DESC = "The id of the shared drive (if the file is located on a shared drive)";

    public static final String SHARED_DRIVE_NAME = "drive.shared.drive.name";
    public static final String SHARED_DRIVE_NAME_DESC = "The name of the shared drive (if the file is located on a shared drive)";

    public static final String ERROR_MESSAGE = "error.message";
    public static final String ERROR_MESSAGE_DESC = "The error message returned by Google Drive";

    public static final String ERROR_CODE = "error.code";
    public static final String ERROR_CODE_DESC = "The error code returned by Google Drive";

}
