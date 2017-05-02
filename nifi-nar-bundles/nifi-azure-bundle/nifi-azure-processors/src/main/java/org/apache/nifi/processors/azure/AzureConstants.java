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
package org.apache.nifi.processors.azure;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public final class AzureConstants {
    public static final String BLOCK = "Block";
    public static final String PAGE = "Page";

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder().name("Storage Account Key").description("The storage account key")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).sensitive(true).build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder().name("Storage Account Name").description("The storage account name")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).sensitive(true).build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder().name("Container name").description("Name of the azure storage container")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).build();

    // use HTTPS by default as per MSFT recommendation
    public static final String FORMAT_DEFAULT_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";

    private AzureConstants() {
        // do not instantiate
    }
}
