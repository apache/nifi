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

public final class ADLSAttributes {

    public static final String ATTR_NAME_FILESYSTEM = "azure.filesystem";
    public static final String ATTR_DESCRIPTION_FILESYSTEM = "The name of the Azure File System";

    public static final String ATTR_NAME_DIRECTORY = "azure.directory";
    public static final String ATTR_DESCRIPTION_DIRECTORY = "The name of the Azure Directory";

    public static final String ATTR_NAME_FILENAME = "azure.filename";
    public static final String ATTR_DESCRIPTION_FILENAME = "The name of the Azure File";

    public static final String ATTR_NAME_LENGTH = "azure.length";
    public static final String ATTR_DESCRIPTION_LENGTH = "The length of the Azure File";

    public static final String ATTR_NAME_LAST_MODIFIED = "azure.lastModified";
    public static final String ATTR_DESCRIPTION_LAST_MODIFIED = "The last modification time of the Azure File";

    public static final String ATTR_NAME_ETAG = "azure.etag";
    public static final String ATTR_DESCRIPTION_ETAG = "The ETag of the Azure File";

    public static final String ATTR_NAME_FILE_PATH = "azure.filePath";
    public static final String ATTR_DESCRIPTION_FILE_PATH = "The full path of the Azure File";

    public static final String ATTR_NAME_PRIMARY_URI = "azure.primaryUri";
    public static final String ATTR_DESCRIPTION_PRIMARY_URI = "Primary location for file content";

}
