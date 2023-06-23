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
package org.apache.nifi.processors.dataupload;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import static org.apache.nifi.processors.dataupload.DataUploadSource.FLOWFILE_CONTENT;
import static org.apache.nifi.processors.dataupload.DataUploadSource.LOCAL_FILE;

public class DataUploadProperties {

    public static final PropertyDescriptor DATA_TO_UPLOAD = new PropertyDescriptor.Builder()
            .name("data-to-upload")
            .displayName("Data to Upload")
            .description("The source of the content to be uploaded.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .allowableValues(DataUploadSource.class)
            .defaultValue(FLOWFILE_CONTENT.getValue())
            .build();

    public static final PropertyDescriptor LOCAL_FILE_PATH = new PropertyDescriptor.Builder()
            .name("local-file-path")
            .displayName("Local File Path")
            .description("Path to a local file to be uploaded.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("${absolute.path}/${filename}")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(DATA_TO_UPLOAD, LOCAL_FILE)
            .build();
}
