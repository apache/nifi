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
package org.apache.nifi.processors.box;

public class BoxFileAttributes {

    public static final String ID = "box.id";
    public static final String ID_DESC = "The id of the file";

    public static final String FILENAME_DESC = "The name of the file";

    public static final String PATH_DESC = "The folder path where the file is located";

    public static final String SIZE = "box.size";
    public static final String SIZE_DESC = "The size of the file";

    public static final String TIMESTAMP = "box.timestamp";
    public static final String TIMESTAMP_DESC =   "The last modified time of the file";

    public static final String ERROR_MESSAGE = "error.message";
    public static final String ERROR_MESSAGE_DESC = "The error message returned by Box";

    public static final String ERROR_CODE = "error.code";
    public static final String ERROR_CODE_DESC = "The error code returned by Box";

}
