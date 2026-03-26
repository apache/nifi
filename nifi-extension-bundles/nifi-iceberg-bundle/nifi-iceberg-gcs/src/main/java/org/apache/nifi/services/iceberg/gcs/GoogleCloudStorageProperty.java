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
package org.apache.nifi.services.iceberg.gcs;

/**
 * Google Cloud Storage configuration properties aligned with Apache Iceberg GCPProperties
 */
enum GoogleCloudStorageProperty {

    OAUTH2_TOKEN("gcs.oauth2.token"),

    SERVICE_HOST("gcs.service.host"),

    WRITE_CHUNK_SIZE_BYTES("gcs.channel.write.chunk-size-bytes"),

    READ_CHUNK_SIZE_BYTES("gcs.channel.read.chunk-size-bytes"),

    USER_PROJECT("gcs.user-project"),

    ENCRYPTION_KEY("gcs.encryption-key"),

    DECRYPTION_KEY("gcs.decryption-key");

    private final String property;

    GoogleCloudStorageProperty(final String property) {
        this.property = property;
    }

    String getProperty() {
        return property;
    }
}
