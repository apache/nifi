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
    public static final String SIZE_DESC = "The size of the file";

    public static final String TIMESTAMP = "drive.timestamp";
    public static final String TIMESTAMP_DESC =  "The last modified time or created time (whichever is greater) of the file." +
            " The reason for this is that the original modified date of a file is preserved when uploaded to Google Drive." +
            " 'Created time' takes the time when the upload occurs. However uploaded files can still be modified later.";

    public static final String MIME_TYPE = CoreAttributes.MIME_TYPE.key();
    public static final String MIME_TYPE_DESC =  "The MIME type of the file";

    public static final String ERROR_MESSAGE = "error.message";
    public static final String ERROR_MESSAGE_DESC = "The error message returned by Google Drive";

    public static final String ERROR_CODE = "error.code";
    public static final String ERROR_CODE_DESC = "The error code returned by Google Drive";

}
