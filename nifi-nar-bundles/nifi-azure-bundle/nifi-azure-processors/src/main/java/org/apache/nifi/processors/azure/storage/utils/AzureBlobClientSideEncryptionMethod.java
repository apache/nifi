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

/**
 * Enumeration capturing essential information about the various client-side
 * encryption methods supported by Azure
 */
public enum AzureBlobClientSideEncryptionMethod {

    NONE("None", "The blobs sent to Azure are not encrypted."),
    SYMMETRIC("Symmetric", "The blobs sent to Azure are encrypted using a symmetric algorithm.");

    private final String cseName;
    private final String description;

    AzureBlobClientSideEncryptionMethod(String cseName, String description) {
        this.cseName = cseName;
        this.description = description;
    }

    public String getCseName() {
        return cseName;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return description;
    }
}
